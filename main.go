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
	"z2gd/gdrive"
	"z2gd/storage"
	"z2gd/zoom"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/api/drive/v3"
)

func main() {
	var (
		configFileName string
		debug          bool
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

	sqliteDatabase, err := storage.NewStorage(cfg.ClientCfg.DbLocation)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect sqlite")
	}

	zclient := zoom.NewZoomClient(zoom.Client{
		AccountId: cfg.ZoomCfg.AccountID,
		Id:        cfg.ZoomCfg.ClientID,
		Secret:    cfg.ZoomCfg.ClientSecret,
	})

	srv, err := gdrive.NewService(context.Background())
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect google drive service")
	}

	err = zclient.Authorize()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect zoom service")
	}

	zoomMeets, err := zclient.GetAllMeetingRecordsSince(int(cfg.ClientCfg.Cutoff))
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get meeting record data")
	}
	log.Info().Msg(fmt.Sprintf("Total meet count = %d", len(zoomMeets)))

	zoomMeets = zoom.FilterRecordUniqueStartTimeAndId(zoomMeets)
	log.Info().Msg(fmt.Sprintf("Total unique meet count = %d", len(zoomMeets)))

	// zoomMeets = zoom.FilterRecordFiletype(zoomMeets, cfg.ClientCfg.FileType)
	// log.Info().Msg(fmt.Sprintf("Total filtered record file extension = %s, meet count = %d", cfg.ClientCfg.FileType, len(zoomMeets)))

	// zoomMeets = zoom.FilterRecordType(zoomMeets, zoom.RecordType(cfg.ClientCfg.RecordType))
	// log.Info().Msg(fmt.Sprintf("Total filtered record type = %s, meet count = %d", cfg.ClientCfg.RecordType, len(zoomMeets)))

	for _, fm := range zoomMeets {
		err = sqliteDatabase.SaveMeeting(fm)
		if err != nil {
			log.Error().Err(err).Msg(fmt.Sprintf("Failed to save meeting to db with meet id = %d, topic = %s", fm.Id, fm.Topic))
		}
	}

	previouslyUnsuccessfulCount, err := sqliteDatabase.CountUnsuccessSyncRecords(cfg.ClientCfg.FileType, zoom.RecordType(cfg.ClientCfg.RecordType), unixToDateTimeString(int64(cfg.ClientCfg.Cutoff)))
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
		parentFolderId, err := gdrive.CreateFolderIfNotExists(srv, cfg.DriveCfg.FolderName, "")
		if err != nil {
			log.Fatal().Err(err).Msg("Failed create google drive base folder")
		}
		for _, fm := range meetings {
			err = syncMeetRecordToDrive(cfg, srv, sqliteDatabase, fm, cfg.ClientCfg.DownloadLocation, parentFolderId)
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

func syncMeetRecordToDrive(cfg config, srv *drive.Service, sqlite *storage.SQLiteStorage, meet zoom.Meeting, downloadLocation, parentFolderId string) error {
	var err error
	for _, fmr := range meet.Records {
		retryCount := 0
		for int(cfg.ClientCfg.Retry) >= retryCount {
			filepath := fmt.Sprintf("%s/%s - %s - %d/", downloadLocation, formatFolderName(meet.Topic), meet.DateTime, meet.Id)
			filename := fmt.Sprintf("%s.%s", string(fmr.Type), strings.ToLower(fmr.FileExtension))
			err := syncRecordToDrive(srv, sqlite, fmr, filepath, filename, parentFolderId)
			if err != nil {
				log.Error().Err(err).Msg(fmt.Sprintf("Failed to sync record from meeting = %s, retry count = %d", meet.Topic, retryCount))
				err = sqlite.UpdateRecord(fmr.Id, zoom.Failed)
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

func syncRecordToDrive(srv *drive.Service, sqlite *storage.SQLiteStorage, record zoom.Record, filepath, filename, parentFolderId string) error {
	err := sqlite.UpdateRecord(record.Id, zoom.Downloading)
	if err != nil {
		return err
	}
	err = downloadFileInChunks(filepath, filename, record.DownloadURL, 1024000000)
	if err != nil {
		removeFolderIfExists(filepath)
		return err
	}
	defer os.RemoveAll(filepath)

	err = sqlite.UpdateRecord(record.Id, zoom.Downloaded)
	if err != nil {
		return err
	}
	err = gdrive.Upload(srv, parentFolderId, filepath, filename)
	if err != nil {
		return err
	}
	err = sqlite.UpdateRecord(record.Id, zoom.Synced)
	if err != nil {
		return err
	}
	// err = os.RemoveAll(filepath)
	// if err != nil {
	// 	return err
	// }
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
