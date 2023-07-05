package gdrive

import (
	"fmt"
	"log"
	"os"
	"strings"

	"google.golang.org/api/drive/v3"
)

func Upload(srv *drive.Service, filepath, filename string) error {
	// baseMimeType := "text/plain"
	file, err := os.Open(filepath + filename)
	if err != nil {
		log.Fatalln(err)
	}
	paths := strings.Split(filepath, "/")
	foldername := paths[len(paths)-2]
	// fileInf, err := file.Stat()
	// if err != nil {
	// 	log.Fatalln(err)
	// }
	// create folder
	createFolder, err := srv.Files.Create(&drive.File{Name: foldername, MimeType: "application/vnd.google-apps.folder", Parents: []string{"1x56v5_6c0E3EQklgYTt4c_IiVgeHqFSD"}}).Do()
	if err != nil {
		log.Fatalf("Unable to create folder: %v", err)
	}
	var folderIDList []string
	folderIDList = append(folderIDList, createFolder.Id)
	defer file.Close()
	f := &drive.File{Name: filename, Parents: folderIDList}
	res, err := srv.Files.
		Create(f).
		Media(file).
		ProgressUpdater(func(now, size int64) { fmt.Printf("%d, %d\r", now, size) }).
		Do()
	if err != nil {
		return err
	}
	fmt.Printf("%v\n", res.DriveId)
	return nil
}
