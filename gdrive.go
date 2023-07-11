package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/rs/zerolog/log"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
)

func ServiceAccount(credentialFile string) *http.Client {
	b, err := os.ReadFile(credentialFile)
	if err != nil {
		log.Fatal().Err(err)
	}
	var c = struct {
		Email      string `json:"client_email"`
		PrivateKey string `json:"private_key"`
	}{}
	json.Unmarshal(b, &c)
	config := &jwt.Config{
		Email:      c.Email,
		PrivateKey: []byte(c.PrivateKey),
		Scopes: []string{
			drive.DriveScope,
		},
		TokenURL: google.JWTTokenURL,
	}
	client := config.Client(context.Background())
	return client
}

// Retrieves a token, saves the token, then returns the generated client.
func GetClient(config *oauth2.Config) *http.Client {
	tokFile := "token.json"
	tok, err := tokenFromFile(tokFile)
	if err != nil {
		tok = getTokenFromWeb(config)
		saveToken(tokFile, tok)
	}
	return config.Client(context.Background(), tok)
}

// Requests a token from the web, then returns the retrieved token.
func getTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("Go to the following link in your browser then type the "+
		"authorization code: \n%v\n", authURL)

	var authCode string
	if _, err := fmt.Scan(&authCode); err != nil {
		log.Fatal().Err(err).Msg("Unable to read authorization code")
	}

	tok, err := config.Exchange(context.Background(), authCode)
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to retrieve token from web")
	}
	return tok
}

// Retrieves a token from a local file.
func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	tok := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(tok)
	if err != nil {
		return nil, err
	}
	return tok, nil
}

// Saves a token to a file path.
func saveToken(path string, token *oauth2.Token) {
	fmt.Printf("Saving credential file to: %s\n", path)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to cache OAuth token")
	}
	defer f.Close()

	err = json.NewEncoder(f).Encode(token)
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to encode token")
	}
}

func NewDriveService(ctx context.Context) (*drive.Service, error) {
	b, err := os.ReadFile("credentials.json")
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to read client secret file")
	}

	// If modifying these scopes, delete your previously saved token.json.
	config, err := google.ConfigFromJSON(b, drive.DriveScope)
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to parse client secret file to config")
	}
	client := GetClient(config)

	srv, err := drive.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to retrieve Drive client")
	}

	return srv, nil
}

func Upload(srv *drive.Service, parentFolderId, filepath, filename string) error {
	// baseMimeType := "text/plain"
	file, err := os.Open(filepath + filename)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to open download folder")
	}
	paths := strings.Split(filepath, "/")
	foldername := paths[len(paths)-2]

	// create folder
	topicFolderId, err := CreateFolderIfNotExists(foldername, parentFolderId)
	if err != nil {
		log.Error().Err(err).Msg("Failed create google drive base folder")
		return err
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
	log.Debug().Any("file", res).Msg("Uploaded")
	return nil
}

func getFolderID(foldername string, parentFolderId string) (string, error) {
	query := fmt.Sprintf("mimeType='application/vnd.google-apps.folder' and name='%s'", foldername)
	if parentFolderId != "" {
		query = fmt.Sprintf("%s and '%s' in parents", query, parentFolderId)
	}

	log.Debug().Any("search query", query).Msg("Search folder name")

	resp, err := driveService.Files.List().Q(query).Do()
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

func CreateFolderIfNotExists(foldername, parentFolderId string) (string, error) {
	folderId, err := getFolderID(foldername, parentFolderId)
	if err != nil {
		return "", err
	}
	if folderId == "" {
		var parentFolders []string
		if parentFolderId != "" {
			parentFolders = append(parentFolders, parentFolderId)
		}
		// Create the folder if it doesn't exist
		folder, err := driveService.Files.Create(&drive.File{Name: foldername, MimeType: "application/vnd.google-apps.folder", Parents: parentFolders}).Do()
		if err != nil {
			return "", err
		}
		folderId = folder.Id
		log.Debug().Any("folder id", folder.Id).Msg("Folder created")
	} else {
		log.Debug().Any("folder id", folderId).Msg("Folder found")
	}
	return folderId, nil
}
