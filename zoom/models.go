package zoom

import (
	"fmt"
	"time"
)

// RecordStatus describes the recording status
type RecordStatus string

const (
	Queued      RecordStatus = "queued"
	Downloading RecordStatus = "downloading"
	Downloaded  RecordStatus = "downloaded"
	Failed      RecordStatus = "failed"
)

// RecordType describes the cloud recording types
type RecordType string

func (r RecordType) String() string {
	return string(r)
}

const (
	AudioOnly                   RecordType = "audio_only"
	ChatFile                    RecordType = "chat_file"
	SharedScreenWithSpeakerView RecordType = "shared_screen_with_speaker_view"
	SharedScreenWithGalleryView RecordType = "shared_screen_with_gallery_view"
)

// Recordings - json response from zoom api
type Recordings struct {
	From          string    `json:"from"`
	To            string    `json:"to"`
	PageSize      int       `json:"page_size"`
	PageCount     int       `json:"page_count"`
	TotalRecords  int       `json:"total_records"`
	NextPageToken string    `json:"next_page_token"`
	Meetings      []Meeting `json:"meetings"`
}

// Meeting contains the meeting details
type Meeting struct {
	UUID      string    `json:"uuid"` // primary key
	Id        uint64    `json:"id"`
	Topic     string    `json:"topic"`
	Records   []Record  `json:"recording_files"`
	StartTime time.Time `json:"start_time"`
	DateTime  string    `json:"date_time"`
	Duration  int       `json:"duration"`
	AccessKey string    `json:"access_key"`
}

// Record describes the records in recording_file array field
type Record struct {
	Id            string       `json:"id"`         // primary key for Record
	MeetingId     string       `json:"meeting_id"` // foreign key to Meeting.UUID
	Type          RecordType   `json:"recording_type"`
	StartTime     time.Time    `json:"recording_start"` // DateTime in RFC3339
	DateTime      string       `json:"date_time"`
	FileExtension string       `json:"file_extension"` // M4A, MP4
	FileSize      FileSize     `json:"file_size"`      // bytes
	DownloadURL   string       `json:"download_url"`
	PlayURL       string       `json:"play_url"`
	Status        RecordStatus `json:"-"`
	FilePath      string       `json:"file_path"` // local file path
}

// RecordInfo describes the records for API response
type RecordInfo struct {
	Id        string       `json:"id"`         // primary key for Record
	MeetingId string       `json:"meeting_id"` // foreign key to Meeting.UUID
	Type      RecordType   `json:"recording_type"`
	DateTime  string       `json:"date_time"`
	FileSize  FileSize     `json:"file_size"` // bytes
	Status    RecordStatus `json:"status"`
	FilePath  string       `json:"file_path"` // local file path
}

// FileSize describes the file size
type FileSize int64

// String returns the string representation of the file size
// in human readable format
func (f FileSize) String() string {
	const unit = 1024
	if f < unit {
		return fmt.Sprintf("%dB", f)
	}
	div, exp := int64(unit), 0
	for n := f / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB",
		float64(f)/float64(div), "kMGTPE"[exp])
}

func (f FileSize) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, f.String())), nil
}

func FilterRecordFiletype(ms []Meeting, recordFileExtension string) []Meeting {
	nms := ms[:0]
	for _, m := range ms {
		isAppend := false
		nmr := m.Records[:0]
		for _, r := range m.Records {
			if r.FileExtension == recordFileExtension {
				isAppend = true
				nmr = append(nmr, r)
			}
		}
		if isAppend {
			m.Records = nmr
			nms = append(nms, m)
		}
	}
	return nms
}

func FilterRecordType(ms []Meeting, recordType RecordType) []Meeting {
	nms := ms[:0]
	for _, m := range ms {
		for _, r := range m.Records {
			if r.Type == recordType {
				nmr := m.Records[:0]
				nmr = append(nmr, r)
				m.Records = nmr
				nms = append(nms, m)
				break
			}
		}
	}
	return nms
}

func FilterRecordUniqueStartTimeAndId(ms []Meeting) []Meeting {
	// we need only to check the map's keys so we use the empty struct as values
	// since it consumes 0 bytes of memory.
	processed := make(map[int64]struct{})

	uniqMeets := make([]Meeting, 0)
	for _, uid := range ms {
		uniqness := uid.StartTime.Unix() + int64(uid.Id)
		// if the user ID has been processed already, we skip it
		if _, ok := processed[uniqness]; ok {
			continue
		}

		// append a unique user ID to the resulting slice.
		uniqMeets = append(uniqMeets, uid)

		// mark the user ID as existing.
		processed[uniqness] = struct{}{}
	}

	return uniqMeets
}
