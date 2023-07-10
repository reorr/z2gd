package storage

import (
	"context"
	"database/sql"
	"time"

	"z2gd/zoom"

	"github.com/rs/zerolog/log"
)

type SQLiteStorage struct {
	DB *sql.DB
}

// NewStorage creates new SQLite storage, creates tables if they don't exist
func NewStorage(path string) (*SQLiteStorage, error) {
	sqliteDatabase, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	// go func() {
	// 	<-ctx.Done()
	// 	sqliteDatabase.Close()
	// }()

	q := `CREATE TABLE IF NOT EXISTS meetings (
		uuid TEXT PRIMARY KEY,
		id INTEGER,
		topic TEXT,
		startTime TEXT
	);
	CREATE TABLE IF NOT EXISTS records (
		id TEXT PRIMARY KEY,
		meetingId TEXT,
		type TEXT,
		startTime TEXT,
		fileExtension TEXT,
		fileSize INTEGER,
		downUrl TEXT,
		playUrl TEXT,
		status TEXT,
		path TEXT
	);`
	_, err = sqliteDatabase.ExecContext(context.Background(), q)
	if err != nil {
		return nil, err
	}

	return &SQLiteStorage{DB: sqliteDatabase}, nil
}

// SaveMeeting saves a meeting to the database
func (s *SQLiteStorage) SaveMeeting(meeting zoom.Meeting) error {
	// convert time to local
	meeting.StartTime = meeting.StartTime.Local()

	q := "INSERT INTO `meetings`(uuid, id, topic, startTime) VALUES ($1, $2, $3, $4)"
	log.Debug().Msg("Saving meeting")

	_, err := s.DB.ExecContext(context.Background(), q,
		meeting.UUID,                            // uuid
		meeting.Id,                              // id
		meeting.Topic,                           // topic
		meeting.StartTime.Format(time.DateTime)) // startTime

	if err != nil {
		return err
	}

	for _, r := range meeting.Records {
		err := s.saveRecord(r)
		if err != nil {
			return err
		}
	}
	return nil
}

// SaveRecord saves a record to the database
func (s *SQLiteStorage) saveRecord(record zoom.Record) error {
	if record.Status == "" {
		record.Status = zoom.Queued
	}

	// convert time to local
	record.StartTime = record.StartTime.Local()

	q := "INSERT INTO `records` VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
	_, err := s.DB.ExecContext(context.Background(), q,
		record.Id,                              // id
		record.MeetingId,                       // meetingId
		record.Type,                            // type
		record.StartTime.Format(time.DateTime), // startTime
		record.FileExtension,                   // fileExtension
		record.FileSize,                        // fileSize
		record.DownloadURL,                     // downUrl
		record.PlayURL,                         // playUrl
		record.Status,                          // status
		record.FilePath)                        // path
	return err
}
