package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/api/drive/v3"
)

var (
	sqliteDatabase *SQLiteStorage
	driveService   *drive.Service
	zclient        *ZoomClient
)

func main() {
	var (
		configFileName string
		debug          bool
		err            error
	)
	flag.StringVar(&configFileName, "c", "config.yml", "Config file name")
	flag.BoolVar(&debug, "d", false, "sets log level to debug")

	flag.Parse()

	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	cfg := loadConfig(configFileName)

	log.Debug().Any("config", cfg).Msg("config loaded")

	sqliteDatabase, err = NewStorage(cfg.ClientCfg.DbLocation)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect sqlite")
	}

	driveService, err = NewDriveService(context.Background())
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect google drive service")
	}

	if cfg.ClientCfg.FetchAPI {
		zclient = NewZoomClient(Client{
			AccountId: cfg.ZoomCfg.AccountID,
			Id:        cfg.ZoomCfg.ClientID,
			Secret:    cfg.ZoomCfg.ClientSecret,
		})
		err = zclient.Authorize()
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to connect zoom service")
		}

		err := zclient.FetchAllMeetingRecordsSince(cfg.ClientCfg.UserIds, int(cfg.ClientCfg.Cutoff))
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to get meeting record data")
		}
	}

	previouslyUnsuccessfulCount, err := sqliteDatabase.CountUnsuccessSyncRecords(cfg.ClientCfg.FileType, cfg.ClientCfg.RecordType, unixToDateTimeString(int64(cfg.ClientCfg.Cutoff)))
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get count failed records")
	}

	log.Info().Msg(fmt.Sprintf("Total previously unsuccess sync %d", previouslyUnsuccessfulCount))

	err = sqliteDatabase.ResetFailedRecords()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to reset records")
	}

	meetings, err := sqliteDatabase.GetUniqueMeetingByFileExtensionAndRecordType(cfg.ClientCfg.FileType, cfg.ClientCfg.RecordType, unixToDateTimeString(int64(cfg.ClientCfg.Cutoff)))
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get meeting record data from db")
	}
	log.Info().Msg(fmt.Sprintf("Total unsynced meet count = %d", len(meetings)))

	if len(meetings) > 0 && !cfg.ClientCfg.DryRun {
		parentFolderId, err := CreateFolderIfNotExists(cfg.DriveCfg.FolderName, "")
		if err != nil {
			log.Fatal().Err(err).Msg("Failed create google drive base folder")
		}
		for _, fm := range meetings {
			err = syncMeetRecordToDrive(cfg, fm, cfg.ClientCfg.DownloadLocation, parentFolderId)
			if err != nil {
				log.Error().Err(err).Msg(fmt.Sprintf("Failed to process record with meet id = %d, topic = %s", fm.Id, fm.Topic))
			}
		}
	}
}

func downloadFileInChunks(filepath string, filename string, url string, chunkSize int) error {
	err := os.MkdirAll(filepath, os.ModePerm)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed create download folder")
	}

	log.Debug().Any("filepath", filepath).Msg("Topic download folder created")

	resp, err := http.Head(url)
	if err != nil {
		return err
	}

	fileSize, _ := strconv.Atoi(resp.Header.Get("Content-Length"))

	out, err := os.OpenFile(filepath+filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer out.Close()

	for i := 0; i < fileSize; i += chunkSize {
		end := i + chunkSize - 1
		if end > fileSize {
			end = fileSize
		}

		req, _ := http.NewRequest("GET", url, nil)
		req.Header.Add("Range", "bytes="+strconv.Itoa(i)+"-"+strconv.Itoa(end))
		resp, _ := http.DefaultClient.Do(req)

		if resp.StatusCode != http.StatusPartialContent {
			// If the status is not "Partial Content" - something went wrong
			return fmt.Errorf("expected HTTP status 206, got %s", resp.Status)
		}

		_, err = io.Copy(out, resp.Body)
		if err != nil {
			return err
		}

		resp.Body.Close()
	}

	log.Info().Any("download path", filepath+filename).Msg("Record downloaded")
	return nil
}

func syncMeetRecordToDrive(cfg config, meet Meeting, downloadLocation, parentFolderId string) error {
	var err error
	for _, fmr := range meet.Records {
		retryCount := 0
		for int(cfg.ClientCfg.Retry) >= retryCount {
			filepath := fmt.Sprintf("%s/%s - %s - %d/", downloadLocation, formatFolderName(meet.Topic), meet.DateTime, meet.Id)
			filename := fmt.Sprintf("%s.%s", string(fmr.Type), strings.ToLower(fmr.FileExtension))
			err := syncRecordToDrive(fmr, filepath, filename, parentFolderId)
			if err != nil {
				log.Error().Err(err).Msg(fmt.Sprintf("Failed to sync record from meeting = %s, retry count = %d", meet.Topic, retryCount))
				err = sqliteDatabase.UpdateRecord(fmr.Id, Failed)
				if err != nil {
					return err
				}
				retryCount++
			} else {
				log.Info().Str("topic", meet.Topic).Str("extension", fmr.FileExtension).Str("type", string(fmr.Type)).Msg("Record synced to google drive")
				break
			}
		}
	}
	return err
}

func syncRecordToDrive(record Record, filepath, filename, parentFolderId string) error {
	err := sqliteDatabase.UpdateRecord(record.Id, Downloading)
	if err != nil {
		return err
	}
	err = downloadFileInChunks(filepath, filename, record.DownloadURL, 1024000000)
	if err != nil {
		removeFolderIfExists(filepath)
		return err
	}
	defer os.RemoveAll(filepath)

	err = sqliteDatabase.UpdateRecord(record.Id, Downloaded)
	if err != nil {
		return err
	}
	err = Upload(driveService, parentFolderId, filepath, filename)
	if err != nil {
		return err
	}
	err = sqliteDatabase.UpdateRecord(record.Id, Synced)
	if err != nil {
		return err
	}
	return nil
}

func removeFolderIfExists(path string) error {
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		err := os.RemoveAll(path)
		if err != nil {
			return err
		}
	}
	return nil
}
