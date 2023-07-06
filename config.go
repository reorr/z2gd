package main

import (
	"io"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
)

/* Environment utility */

func loadEnvStr(key string, result *string) {
	s, ok := os.LookupEnv(key)
	if !ok {
		return
	}

	*result = s
}

func loadEnvUint(key string, result *uint) {
	s, ok := os.LookupEnv(key)
	if !ok {
		return
	}

	n, err := strconv.Atoi(s)

	if err != nil {
		return
	}

	*result = uint(n) // will clamp the negative value
}

func loadEnvBool(key string, result *bool) {
	s, ok := os.LookupEnv(key)
	if !ok {
		return
	}

	switch s {
	case "true", "1":
		*result = true
	case "false", "0":
		*result = false
	default:
		*result = false
	}
}

/* Configuration */
type zoomConfig struct {
	ClientID     string `yaml:"client_id" json:"client_id"`
	ClientSecret string `yaml:"client_secret" json:"client_secret"`
	AccountID    string `yaml:"account_id" json:"account_id"`
}

func defaultZoomConfig() zoomConfig {
	return zoomConfig{
		ClientID:     "thisisclientid",
		ClientSecret: "thisisclientsecret",
		AccountID:    "thisisaccountid",
	}
}

func (z *zoomConfig) loadFromEnv() {
	loadEnvStr("ZDG_ZOOM_CLIENT_ID", &z.ClientID)
	loadEnvStr("ZDG_ZOOM_CLIENT_SECRET", &z.ClientSecret)
	loadEnvStr("ZDG_ZOOM_ACCOUNT_ID", &z.AccountID)
}

type driveConfig struct {
	Credentials string `yaml:"credentials" json:"credentials"`
}

func defaultDriveConfig() driveConfig {
	return driveConfig{
		Credentials: "credentials.json",
	}
}

func (d *driveConfig) loadFromEnv() {
	loadEnvStr("ZDG_DRIVE_CREDENTIALS", &d.Credentials)
}

type clientConfig struct {
	DownloadLocation string `yaml:"download_location" json:"download_location"`
	FileType         string `yaml:"file_type" json:"file_type"`
	RecordType       string `yaml:"record_type" json:"record_type"`
	Cutoff           uint   `yaml:"cutoff" json:"cutoff"`
	DryRun           bool   `yaml:"dry_run" json:"dry_run"`
	Retry            uint   `yaml:"retry" json:"retry"`
}

func defaultClientConfig() clientConfig {
	return clientConfig{
		DownloadLocation: "/tmp",
		FileType:         "TXT",
		RecordType:       "chat_file",
		Cutoff:           1688169600,
		DryRun:           true,
		Retry:            0,
	}
}

func (d *clientConfig) loadFromEnv() {
	loadEnvStr("ZDG_CLIENT_DOWNLOAD_LOCATION", &d.DownloadLocation)
	loadEnvStr("ZDG_CLIENT_FILE_TYPE", &d.FileType)
	loadEnvStr("ZDG_CLIENT_RECORD_TYPE", &d.RecordType)
	loadEnvUint("ZDG_CLIENT_CUTOFF", &d.Cutoff)
	loadEnvBool("ZDG_CLIENT_DRY_RUN", &d.DryRun)
	loadEnvUint("ZDG_CLIENT_Retry", &d.Retry)
}

type config struct {
	ZoomCfg   zoomConfig   `yaml:"zoom" json:"zoom"`
	DriveCfg  driveConfig  `yaml:"drive" json:"drive"`
	ClientCfg clientConfig `yaml:"client" json:"client"`
}

func (c *config) loadFromEnv() {
	c.ZoomCfg.loadFromEnv()
	c.DriveCfg.loadFromEnv()
	c.ClientCfg.loadFromEnv()
}

func defaultConfig() config {
	return config{
		ZoomCfg:   defaultZoomConfig(),
		DriveCfg:  defaultDriveConfig(),
		ClientCfg: defaultClientConfig(),
	}
}

func loadConfigFromReader(r io.Reader, c *config) error {
	return yaml.NewDecoder(r).Decode(c)
}

func loadConfigFromFile(fn string, c *config) error {
	_, err := os.Stat(fn)

	if err != nil {
		return err
	}

	f, err := os.Open(fn)

	if err != nil {
		return err
	}

	defer f.Close()

	return loadConfigFromReader(f, c)
}

/* How to load the configuration, the highest priority loaded last
 * First: Initialise to default config
 * Second: Replace with environment variables
 * Third: Replace with configuration file
 */

func loadConfig(fn string) config {
	cfg := defaultConfig()
	cfg.loadFromEnv()

	loadConfigFromFile(fn, &cfg)

	return cfg
}
