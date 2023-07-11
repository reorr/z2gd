package main

import (
	b64 "encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	apiURI     = "api.zoom.us"
	apiVersion = "/v2"
)

type Client struct {
	AccountId        string `yaml:"account_id"`        // Zoom account id
	Id               string `yaml:"id"`                // Zoom client id
	Secret           string `yaml:"secret"`            // Zoom client secret
	DeleteDownloaded bool   `yaml:"delete_downloaded"` // Delete downloaded files from Zoom cloud
	TrashDownloaded  bool   `yaml:"trash_downloaded"`  // Move downloaded files to trash
	DeleteSkipped    bool   `yaml:"delete_skipped"`    // Delete skipped files from Zoom cloud (the ones that are shorter than MinDuration)
}

type AccessToken struct {
	AccessToken string    `json:"access_token"`
	ExpiresIn   int       `json:"expires_in"`
	Scope       string    `json:"scope"`
	TokenType   string    `json:"token_type"`
	ExpiresAt   time.Time `json:"-"`
}

type ZoomClient struct {
	cfg      *Client
	client   *http.Client
	token    *AccessToken
	mx       sync.Mutex
	endpoint string
}

func NewZoomClient(cfg Client) *ZoomClient {
	var uri = url.URL{
		Scheme: "https",
		Host:   apiURI,
		Path:   apiVersion,
	}
	client := &http.Client{}

	return &ZoomClient{
		cfg:      &cfg,
		client:   client,
		endpoint: uri.String(),
	}
}

func (z *ZoomClient) Authorize() error {
	bearer := b64.StdEncoding.EncodeToString([]byte(z.cfg.Id + ":" + z.cfg.Secret))

	params := url.Values{}
	params.Add(`grant_type`, `account_credentials`)
	params.Add(`account_id`, z.cfg.AccountId)

	req, err := http.NewRequest(http.MethodPost, "https://zoom.us/oauth/token", strings.NewReader(params.Encode()))
	if err != nil {
		return err
	}

	req.Header.Add(`Authorization`, fmt.Sprintf("Basic %s", bearer))
	req.Header.Add(`Host`, "zoom.us")
	req.Header.Add(`Content-Type`, "application/x-www-form-urlencoded")

	res, err := z.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("unable to authorize with account id: %s and client id: %s, status %d, message: %s", z.cfg.AccountId, z.cfg.Id, res.StatusCode, res.Body)
	}

	if err := json.NewDecoder(res.Body).Decode(&z.token); err != nil {
		return err
	}

	log.Debug().Any("zoom token", z.token.AccessToken)

	dur, err := time.ParseDuration(fmt.Sprintf("%ds", z.token.ExpiresIn))
	if err != nil {
		return err
	}
	z.token.ExpiresAt = time.Now().Add(dur).Add(-5 * time.Minute)

	return nil
}

func (z *ZoomClient) GetToken() (*AccessToken, error) {
	z.mx.Lock()
	defer z.mx.Unlock()

	if z.token == nil || z.token.ExpiresAt.Before(time.Now()) {
		if err := z.Authorize(); err != nil {
			return nil, err
		}
	}
	return z.token, nil
}

func (z *ZoomClient) FetchAllMeetingRecordsSince(userIds []string, cutoff int) error {
	_, err := z.GetToken()
	if err != nil {
		return errors.Join(fmt.Errorf("unable to get token"), err)
	}

	// meetings := []Meeting{}

	for _, userId := range userIds {
		from := time.Now().AddDate(0, 0, -30)
		to := time.Now()

		params := url.Values{}
		params.Add(`page_size`, "300")
		params.Add(`from`, from.Format("2006-01-02"))
		params.Add(`to`, to.Format("2006-01-02"))

		path := "/users/%s/recordings"
		pathWithUserId := fmt.Sprintf(path, userId)
		endpoint := z.endpoint + pathWithUserId
		log.Debug().Any("endpoint", endpoint).Msg("Zoom endpoint")

		req, err := http.NewRequest(http.MethodGet, endpoint+"?"+params.Encode(), nil)
		if err != nil {
			return err
		}

		req.Header.Add(`Authorization`, fmt.Sprintf("Bearer %s", z.token.AccessToken))
		req.Header.Add(`Host`, "zoom.us")
		req.Header.Add(`Content-Type`, "application/json")

		for int(to.Unix()) >= cutoff {
			log.Debug().Any("params", params.Encode()).Msg("Zoom params")

			req.URL.RawQuery = params.Encode()
			res, err := z.client.Do(req)
			if err != nil {
				return err
			}
			defer res.Body.Close()

			if res.StatusCode != http.StatusOK {
				return fmt.Errorf("unable to authorize with account id: %s and client id: %s, status %d, message: %s", z.cfg.AccountId, z.cfg.Id, res.StatusCode, res.Body)
			}

			recordings := &Recordings{}

			if err := json.NewDecoder(res.Body).Decode(recordings); err != nil {
				return err
			}

			for _, fm := range recordings.Meetings {
				err = sqliteDatabase.SaveMeeting(fm)
				if err != nil {
					log.Error().Err(err).Msg(fmt.Sprintf("Failed to save meeting to db with meet id = %d, topic = %s", fm.Id, fm.Topic))
				}
			}
			// meetings = append(meetings, recordings.Meetings...)

			from = from.AddDate(0, 0, -30)
			to = to.AddDate(0, 0, -30)
			params.Set(`from`, from.Format("2006-01-02"))
			params.Set(`to`, to.Format("2006-01-02"))
			time.Sleep(500 * time.Millisecond) // avoid rate limit
		}
	}

	return nil
}
