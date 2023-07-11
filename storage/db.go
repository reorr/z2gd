package storage

import (
	"context"
	"database/sql"
	"errors"
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

	q := "INSERT INTO `meetings`(uuid, id, topic, startTime) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING"
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

	q := "INSERT INTO `records` VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT DO NOTHING"
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

// GetMeeting returns a meeting from the database
func (s *SQLiteStorage) GetMeeting(UUID string) (*zoom.Meeting, error) {
	q := "SELECT * FROM `meetings` WHERE uuid = $1"
	row := s.DB.QueryRowContext(context.Background(), q, UUID)
	meeting := zoom.Meeting{}
	err := row.Scan(&meeting.UUID, &meeting.Id, &meeting.Topic, &meeting.DateTime)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.New("aaaa")
		}
		return nil, err
	}
	return &meeting, nil
}

// GetRecords returns records of specific meeting from the database
func (s *SQLiteStorage) GetRecords(UUID string) ([]zoom.Record, error) {
	q := "SELECT * FROM `records` WHERE meetingId = $1"
	rows, err := s.DB.QueryContext(context.Background(), q, UUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []zoom.Record
	for rows.Next() {
		record := zoom.Record{}
		err := rows.Scan(
			&record.Id,
			&record.MeetingId,
			&record.Type,
			&record.DateTime,
			&record.FileExtension,
			&record.FileSize,
			&record.DownloadURL,
			&record.PlayURL,
			&record.Status,
			&record.FilePath)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

// GetMeeting returns a meeting from the database
func (s *SQLiteStorage) GetMeetingWithRecords(UUID string) (*zoom.Meeting, error) {
	q := "SELECT meetings.uuid, meetings.id, meetings.topic, meetings.startTime, records.id, records.meetingId, records.type, records.startTime, records.fileExtension, records.fileSize, records.downUrl, records.playUrl, records.status  FROM `meetings` JOIN `records` ON meetings.uuid = records.meetingId WHERE meetings.uuid = $1"
	rows, err := s.DB.Query(q, UUID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.New("aaaa")
		}
		return nil, err
	}
	meeting := &zoom.Meeting{}
	for rows.Next() {
		record := &zoom.Record{}
		err = rows.Scan(
			meeting.UUID,
			meeting.Id,
			meeting.Topic,
			meeting.StartTime,
			record.Id,
			record.MeetingId,
			record.Type,
			record.StartTime,
			record.FileExtension,
			record.FileExtension,
			record.DownloadURL,
			record.PlayURL,
			record.Status,
		)
		if err != nil {
			return nil, err
		}
		meeting.Records = append(meeting.Records, *record)
	}
	return meeting, nil
}
