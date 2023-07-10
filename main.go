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

	meets, err := zclient.GetAllMeetingRecordsSince(int(cfg.ClientCfg.Cutoff))
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get meeting record data")
	}
	log.Info().Msg(fmt.Sprintf("Total meet count = %d", len(meets)))

	meets = zoom.FilterRecordUniqueStartTimeAndId(meets)
	log.Info().Msg(fmt.Sprintf("Total unique meet count = %d", len(meets)))

	meets = zoom.FilterRecordFiletype(meets, cfg.ClientCfg.FileType)
	log.Info().Msg(fmt.Sprintf("Total filtered record file extension = %s, meet count = %d", cfg.ClientCfg.FileType, len(meets)))

	meets = zoom.FilterRecordType(meets, zoom.RecordType(cfg.ClientCfg.RecordType))
	log.Info().Msg(fmt.Sprintf("Total filtered record type = %s, meet count = %d", cfg.ClientCfg.RecordType, len(meets)))

	for _, fm := range meets {
		err = sqliteDatabase.SaveMeeting(fm)
		if err != nil {
			log.Error().Err(err).Msg(fmt.Sprintf("Failed to save meeting to db with meet id = %d, topic = %s", fm.Id, fm.Topic))
		}
		mt, err := sqliteDatabase.GetMeeting(fm.UUID)
		if err != nil {
			log.Error().Err(err).Msg(fmt.Sprintf("Failed to save meeting to db with meet id = %d, topic = %s", fm.Id, fm.Topic))
		}
		log.Info().Any("meet", mt)
	}

	if !cfg.ClientCfg.DryRun {
		parentFolderId, err := gdrive.CreateFolderIfNotExists(srv, cfg.DriveCfg.FolderName, "")
		if err != nil {
			log.Fatal().Err(err).Msg("Failed create google drive base folder")
		}
		for _, fm := range meets {
			err = syncMeetRecordToDrive(cfg, srv, fm, cfg.ClientCfg.DownloadLocation, parentFolderId)
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

func syncMeetRecordToDrive(cfg config, srv *drive.Service, meet zoom.Meeting, downloadLocation, parentFolderId string) error {
	var err error
	for _, fmr := range meet.Records {
		retryCount := 0
		for int(cfg.ClientCfg.Retry) >= retryCount {
			filepath := fmt.Sprintf("%s/%s - %s - %d/", downloadLocation, formatFolderName(meet.Topic), meet.StartTime.Format("02-01-2006"), meet.Id)
			filename := fmt.Sprintf("%s.%s", string(fmr.Type), strings.ToLower(fmr.FileExtension))
			err := syncRecordToDrive(srv, fmr, filepath, filename, parentFolderId)
			if err != nil {
				log.Error().Err(err).Msg(fmt.Sprintf("Failed to sync record from meeting = %s, retry count = %d", meet.Topic, retryCount))
				retryCount++
			} else {
				log.Info().Str("topic", meet.Topic).Str("extension", fmr.FileExtension).Str("type", string(fmr.Type)).Msg("Record synced to google drive")
				break
			}
		}
	}
	return err
}

func syncRecordToDrive(srv *drive.Service, record zoom.Record, filepath, filename, parentFolderId string) error {
	err := downloadFileInChunks(filepath, filename, record.DownloadURL, 1024000000)
	if err != nil {
		removeFolderIfExists(filepath)
		return err
	}
	err = gdrive.Upload(srv, parentFolderId, filepath, filename)
	if err != nil {
		return err
	}
	err = os.RemoveAll(filepath)
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
