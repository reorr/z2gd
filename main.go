package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"z2gd/gdrive"
	"z2gd/zoom"

	"google.golang.org/api/drive/v3"
)

func main() {
	var configFileName string
	flag.StringVar(&configFileName, "c", "config.yml", "Config file name")

	flag.Parse()

	cfg := loadConfig(configFileName)

	log.Println("config loaded")

	zclient := zoom.NewZoomClient(zoom.Client{
		AccountId: cfg.ZoomCfg.AccountID,
		Id:        cfg.ZoomCfg.ClientID,
		Secret:    cfg.ZoomCfg.ClientSecret,
	})

	// gdclient := ServiceAccount("credential.json")
	// srv, err := drive.New(gdclient)
	// if err != nil {
	// 	log.Fatalln(err)
	// }

	srv, err := gdrive.NewService(context.Background())
	if err != nil {
		log.Fatalln(err)
	}

	err = zclient.Authorize()
	if err != nil {
		log.Printf("Error: %+v\n\n", err)
	}
	meets, err := zclient.GetAllMeetingRecordsSince(int(cfg.ClientCfg.Cutoff))
	if err != nil {
		log.Printf("Error: %+v\n\n", err)
	}
	log.Printf("Meetings count: %+v\n\n", len(meets))

	meets = zoom.FilterRecordUniqueStartTimeAndId(meets)
	log.Printf("Filtered uniq meeting records count: %+v\n\n", len(meets))

	meets = zoom.FilterRecordFiletype(meets, cfg.ClientCfg.FileType)
	log.Printf("Filtered record file extension = %s,  meetings count: %+v\n\n", cfg.ClientCfg.FileType, len(meets))

	meets = zoom.FilterRecordType(meets, zoom.RecordType(cfg.ClientCfg.RecordType))
	log.Printf("Filtered record type = %s, meetings count: %+v\n\n", cfg.ClientCfg.RecordType, len(meets))

	if !cfg.ClientCfg.DryRun {
		parentFolderId, err := gdrive.CreateParentFolder(srv)
		if err != nil {
			log.Panic("[ERROR] err = ", err.Error())
		}
		for _, fm := range meets {
			err = syncRecordToDrive(srv, fm, cfg.ClientCfg.DownloadLocation, parentFolderId)
			if err != nil {
				log.Printf("[ERROR] processing record with meet id = %d, topic = %s", fm.Id, fm.Topic)
			}
		}
	}
}

func downloadFileInChunks(filepath string, filename string, url string, chunkSize int) error {
	err := os.MkdirAll(filepath, os.ModePerm)
	if err != nil {
		log.Println(err)
	}

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

	return nil
}

func syncRecordToDrive(srv *drive.Service, meet zoom.Meeting, downloadLocation, parentFolderId string) error {
	var err error
	for _, fmr := range meet.Records {
		downloadPath := fmt.Sprintf("%s/%s - %s - %d/", downloadLocation, formatFolderName(meet.Topic), meet.StartTime.Format("02-01-2006"), meet.Id)
		fmt.Println(downloadPath)
		downloadName := fmt.Sprintf("%s.%s", string(fmr.Type), strings.ToLower(fmr.FileExtension))
		err := downloadFileInChunks(downloadPath, downloadName, fmr.DownloadURL, 10240)
		if err != nil {
			log.Printf("[ERROR] err = %s", err.Error())
			break
		}
		err = gdrive.Upload(srv, parentFolderId, downloadPath, downloadName)
		if err != nil {
			log.Printf("[ERROR] err = %s", err.Error())
			break
		}
		err = os.RemoveAll(downloadPath)
		if err != nil {
			log.Printf("[ERROR] err = %s", err.Error())
			break
		}
	}
	return err
}