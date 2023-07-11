package main

import (
	"context"
	"database/sql"
	"errors"
	"time"

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
func (s *SQLiteStorage) SaveMeeting(meeting Meeting) error {
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
func (s *SQLiteStorage) saveRecord(record Record) error {
	if record.Status == "" {
		record.Status = Queued
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
func (s *SQLiteStorage) GetMeeting(UUID string) (*Meeting, error) {
	q := "SELECT * FROM `meetings` WHERE uuid = $1"
	row := s.DB.QueryRowContext(context.Background(), q, UUID)
	meeting := Meeting{}
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
func (s *SQLiteStorage) GetRecords(UUID string) ([]Record, error) {
	q := "SELECT * FROM `records` WHERE meetingId = $1"
	rows, err := s.DB.QueryContext(context.Background(), q, UUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []Record
	for rows.Next() {
		record := Record{}
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

func (s *SQLiteStorage) GetRecordsByFileExtensionAndRecordType(UUID, recordType, fileExtension string) ([]Record, error) {
	q := "SELECT * FROM `records` WHERE meetingId = $1 AND fileExtension = $2 AND type = $3"
	if recordType == "all" {
		q = "SELECT * FROM `records` WHERE meetingId = $1 AND fileExtension = $2"
	}
	log.Debug().Any("query", q).Msg("Find records by query")
	rows, err := s.DB.QueryContext(context.Background(), q, UUID, fileExtension, recordType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []Record
	for rows.Next() {
		record := Record{}
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

func (s *SQLiteStorage) GetUniqueMeetingByFileExtensionAndRecordType(fileExtension, recordType, cutoff string) ([]Meeting, error) {
	q := "SELECT meetings.uuid, meetings.id, meetings.topic, meetings.startTime FROM `meetings` JOIN `records` ON meetings.uuid = records.meetingId WHERE meetings.startTime >= $1 AND records.status != 'synced' AND records.fileExtension = $2 AND records.type = $3 GROUP BY meetings.uuid ORDER BY meetings.startTime DESC;"
	if recordType == "all" {
		q = "SELECT meetings.uuid, meetings.id, meetings.topic, meetings.startTime FROM `meetings` JOIN `records` ON meetings.uuid = records.meetingId WHERE meetings.startTime >= $1 AND records.status != 'synced' AND records.fileExtension = $2 GROUP BY meetings.uuid ORDER BY meetings.startTime DESC;"
	}
	log.Debug().Any("query", q).Msg("Find meetings by query")
	rows, err := s.DB.QueryContext(context.Background(), q, cutoff, fileExtension, recordType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var meetings []Meeting
	for rows.Next() {
		meeting := Meeting{}
		err := rows.Scan(
			&meeting.UUID,
			&meeting.Id,
			&meeting.Topic,
			&meeting.DateTime,
		)
		if err != nil {
			return nil, err
		}
		records, err := s.GetRecordsByFileExtensionAndRecordType(meeting.UUID, recordType, fileExtension)
		if err != nil {
			return nil, err
		}
		meeting.Records = records
		meetings = append(meetings, meeting)
	}
	return meetings, nil
}

// GetMeeting returns a meeting from the database
func (s *SQLiteStorage) GetMeetingWithRecords(UUID string) (*Meeting, error) {
	q := "SELECT meetings.uuid, meetings.id, meetings.topic, meetings.startTime, records.id, records.meetingId, records.type, records.startTime, records.fileExtension, records.fileSize, records.downUrl, records.playUrl, records.status  FROM `meetings` JOIN `records` ON meetings.uuid = records.meetingId WHERE meetings.uuid = $1"
	rows, err := s.DB.Query(q, UUID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.New("aaaa")
		}
		return nil, err
	}
	meeting := &Meeting{}
	for rows.Next() {
		record := &Record{}
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

// UpdateRecord updates a record in the database
func (s *SQLiteStorage) UpdateRecord(Id string, status RecordStatus) error {
	q := "UPDATE `records` SET status = $1 WHERE id = $2"
	_, err := s.DB.ExecContext(context.Background(), q, status, Id)
	return err
}

// ResetFailedRecords resets all failed records to queued
func (s *SQLiteStorage) ResetFailedRecords() error {
	q := "UPDATE `records` SET status = 'queued' WHERE status !=  'synced'"
	_, err := s.DB.ExecContext(context.Background(), q)
	return err
}

func (s *SQLiteStorage) CountRecordsByFileExtensionAndTypeAndStatus(fileExtension string, recordType RecordType, status RecordStatus) (uint, error) {
	q := "SELECT COUNT(*) FROM `records` WHERE status =  $1 AND fileExtension = $2 AND type = $3"
	if recordType == "all" {
		q = "SELECT COUNT(*) FROM `records` WHERE status =  $1 AND fileExtension = $2"
	}

	var count uint
	rows, err := s.DB.Query(q, status, fileExtension, recordType)
	if err != nil {
		return count, err
	}

	for rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return count, err
		}
	}
	return count, err
}

func (s *SQLiteStorage) CountUnsuccessSyncRecords(fileExtension string, recordType RecordType, cutoff string) (uint, error) {
	q := "SELECT COUNT(*) FROM `records` WHERE startTime >= $1 AND status != 'synced' AND fileExtension = $2 AND type = $3"
	if recordType == "all" {
		q = "SELECT COUNT(*) FROM `records` WHERE startTime >= $1 AND status != 'synced' AND fileExtension = $2"
	}

	var count uint
	rows, err := s.DB.Query(q, cutoff, fileExtension, recordType)
	if err != nil {
		return count, err
	}

	for rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return count, err
		}
	}
	return count, err
}
