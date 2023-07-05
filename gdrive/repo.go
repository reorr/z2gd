package gdrive

import (
	"fmt"
	"log"
	"os"
	"strings"

	"google.golang.org/api/drive/v3"
)

func Upload(srv *drive.Service, parentFolderId, filepath, filename string) error {
	// baseMimeType := "text/plain"
	file, err := os.Open(filepath + filename)
	if err != nil {
		log.Fatalln(err)
	}
	paths := strings.Split(filepath, "/")
	foldername := paths[len(paths)-2]

	// create folder
	createFolder, err := srv.Files.Create(&drive.File{Name: foldername, MimeType: "application/vnd.google-apps.folder", Parents: []string{parentFolderId}}).Do()
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

func getFolderID(srv *drive.Service, folderName string) (string, error) {
	query := fmt.Sprintf("mimeType='application/vnd.google-apps.folder' and name='%s'", folderName)
	resp, err := srv.Files.List().Q(query).Do()
	if err != nil {
		return "", err
	}

	if len(resp.Files) > 0 {
		return resp.Files[0].Id, nil
	}

	return "", nil
}

func CreateParentFolder(srv *drive.Service) (string, error) {
	parentFolderName := "Zoom Sync"
	parentFolderID, err := getFolderID(srv, parentFolderName)
	if err != nil {
		log.Fatalf("Failed to check if folder exists: %v", err)
		return "", err
	}
	if parentFolderID == "" {
		// Create the folder if it doesn't exist
		folder, err := srv.Files.Create(&drive.File{Name: parentFolderName, MimeType: "application/vnd.google-apps.folder"}).Do()
		if err != nil {
			log.Fatalf("Failed to create folder: %v", err)
			return "", err
		}
		parentFolderID = folder.Id
		fmt.Printf("Folder created with ID: %s\n", folder.Id)
	} else {
		fmt.Printf("Folder already exists with ID: %s\n", parentFolderID)
	}
	return parentFolderID, nil
}
