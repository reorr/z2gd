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
	topicFolderId, err := CreateFolderIfNotExists(srv, foldername, parentFolderId)
	if err != nil {
		log.Fatalf("Unable to create folder: %v", err)
	}
	var parentFolders []string
	parentFolders = append(parentFolders, topicFolderId)
	defer file.Close()
	f := &drive.File{Name: filename, Parents: parentFolders}
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

func getFolderID(srv *drive.Service, foldername string, parentFolderId string) (string, error) {
	query := fmt.Sprintf("mimeType='application/vnd.google-apps.folder' and name='%s'", foldername)
	if parentFolderId != "" {
		query = fmt.Sprintf("%s and '%s' in parents", query, parentFolderId)
	}
	resp, err := srv.Files.List().Q(query).Do()
	if err != nil {
		return "", err
	}

	if len(resp.Files) > 0 {
		if !resp.Files[0].Trashed {
			return resp.Files[0].Id, nil
		}
	}

	return "", nil
}

func CreateFolderIfNotExists(srv *drive.Service, foldername, parentFolderId string) (string, error) {
	folderId, err := getFolderID(srv, foldername, parentFolderId)
	if err != nil {
		log.Fatalf("Failed to check if folder exists: %v", err)
		return "", err
	}
	if folderId == "" {
		var parentFolders []string
		if parentFolderId != "" {
			parentFolders = append(parentFolders, parentFolderId)
		}
		// Create the folder if it doesn't exist
		folder, err := srv.Files.Create(&drive.File{Name: foldername, MimeType: "application/vnd.google-apps.folder", Parents: parentFolders}).Do()
		if err != nil {
			log.Fatalf("Failed to create folder: %v", err)
			return "", err
		}
		folderId = folder.Id
		fmt.Printf("Folder created with ID: %s\n", folder.Id)
	} else {
		fmt.Printf("Folder already exists with ID: %s\n", folderId)
	}
	return folderId, nil
}
