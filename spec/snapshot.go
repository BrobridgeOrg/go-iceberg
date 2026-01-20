package spec

import (
	"encoding/json"
	"fmt"
	"time"
)

// Operation represents the type of operation that produced a snapshot.
type Operation string

const (
	OpAppend    Operation = "append"
	OpReplace   Operation = "replace"
	OpOverwrite Operation = "overwrite"
	OpDelete    Operation = "delete"
)

// Summary contains snapshot summary information.
type Summary struct {
	Operation                 Operation `json:"operation"`
	AddedDataFilesCount       int64     `json:"added-data-files,string,omitempty"`
	AddedRecordsCount         int64     `json:"added-records,string,omitempty"`
	AddedFileSizeInBytes      int64     `json:"added-files-size,string,omitempty"`
	RemovedDataFilesCount     int64     `json:"removed-data-files,string,omitempty"`
	RemovedRecordsCount       int64     `json:"removed-records,string,omitempty"`
	RemovedFileSizeInBytes    int64     `json:"removed-files-size,string,omitempty"`
	DeletedDataFilesCount     int64     `json:"deleted-data-files,string,omitempty"`
	DeletedRecordsCount       int64     `json:"deleted-records,string,omitempty"`
	AddedDeleteFilesCount     int64     `json:"added-delete-files,string,omitempty"`
	AddedPositionDeletesCount int64     `json:"added-position-deletes,string,omitempty"`
	AddedEqualityDeletesCount int64     `json:"added-equality-deletes,string,omitempty"`
	TotalRecordsCount         int64     `json:"total-records,string,omitempty"`
	TotalDataFilesCount       int64     `json:"total-data-files,string,omitempty"`
	TotalDeleteFilesCount     int64     `json:"total-delete-files,string,omitempty"`
	TotalPositionDeletesCount int64     `json:"total-position-deletes,string,omitempty"`
	TotalEqualityDeletesCount int64     `json:"total-equality-deletes,string,omitempty"`
	// Additional properties can be stored here
	Extra map[string]string `json:"-"`
}

// Snapshot represents an Iceberg table snapshot.
type Snapshot struct {
	SnapshotID       int64    `json:"snapshot-id"`
	ParentSnapshotID *int64   `json:"parent-snapshot-id,omitempty"`
	SequenceNumber   int64    `json:"sequence-number"`
	TimestampMs      int64    `json:"timestamp-ms"`
	ManifestList     string   `json:"manifest-list"`
	Summary          *Summary `json:"summary,omitempty"`
	SchemaID         *int     `json:"schema-id,omitempty"`
}

// Timestamp returns the snapshot timestamp as a time.Time.
func (s *Snapshot) Timestamp() time.Time {
	return time.UnixMilli(s.TimestampMs)
}

// HasParent returns true if this snapshot has a parent.
func (s *Snapshot) HasParent() bool {
	return s.ParentSnapshotID != nil
}

// SnapshotRef represents a reference to a snapshot (branch or tag).
type SnapshotRef struct {
	SnapshotID             int64  `json:"snapshot-id"`
	Type                   string `json:"type"` // "branch" or "tag"
	MinSnapshotsToKeep     *int   `json:"min-snapshots-to-keep,omitempty"`
	MaxSnapshotAgeMs       *int64 `json:"max-snapshot-age-ms,omitempty"`
	MaxRefAgeMs            *int64 `json:"max-ref-age-ms,omitempty"`
}

// SnapshotLog represents an entry in the snapshot log.
type SnapshotLog struct {
	SnapshotID  int64 `json:"snapshot-id"`
	TimestampMs int64 `json:"timestamp-ms"`
}

// Timestamp returns the log entry timestamp as a time.Time.
func (l *SnapshotLog) Timestamp() time.Time {
	return time.UnixMilli(l.TimestampMs)
}

// MarshalJSON implements json.Marshaler for Summary.
func (s *Summary) MarshalJSON() ([]byte, error) {
	// Create a map with all fields
	m := map[string]string{
		"operation": string(s.Operation),
	}

	// Add numeric fields as strings if non-zero
	if s.AddedDataFilesCount > 0 {
		m["added-data-files"] = formatInt64(s.AddedDataFilesCount)
	}
	if s.AddedRecordsCount > 0 {
		m["added-records"] = formatInt64(s.AddedRecordsCount)
	}
	if s.AddedFileSizeInBytes > 0 {
		m["added-files-size"] = formatInt64(s.AddedFileSizeInBytes)
	}
	if s.RemovedDataFilesCount > 0 {
		m["removed-data-files"] = formatInt64(s.RemovedDataFilesCount)
	}
	if s.RemovedRecordsCount > 0 {
		m["removed-records"] = formatInt64(s.RemovedRecordsCount)
	}
	if s.RemovedFileSizeInBytes > 0 {
		m["removed-files-size"] = formatInt64(s.RemovedFileSizeInBytes)
	}
	if s.DeletedDataFilesCount > 0 {
		m["deleted-data-files"] = formatInt64(s.DeletedDataFilesCount)
	}
	if s.DeletedRecordsCount > 0 {
		m["deleted-records"] = formatInt64(s.DeletedRecordsCount)
	}
	if s.AddedDeleteFilesCount > 0 {
		m["added-delete-files"] = formatInt64(s.AddedDeleteFilesCount)
	}
	if s.AddedPositionDeletesCount > 0 {
		m["added-position-deletes"] = formatInt64(s.AddedPositionDeletesCount)
	}
	if s.AddedEqualityDeletesCount > 0 {
		m["added-equality-deletes"] = formatInt64(s.AddedEqualityDeletesCount)
	}
	if s.TotalRecordsCount > 0 {
		m["total-records"] = formatInt64(s.TotalRecordsCount)
	}
	if s.TotalDataFilesCount > 0 {
		m["total-data-files"] = formatInt64(s.TotalDataFilesCount)
	}
	if s.TotalDeleteFilesCount > 0 {
		m["total-delete-files"] = formatInt64(s.TotalDeleteFilesCount)
	}
	if s.TotalPositionDeletesCount > 0 {
		m["total-position-deletes"] = formatInt64(s.TotalPositionDeletesCount)
	}
	if s.TotalEqualityDeletesCount > 0 {
		m["total-equality-deletes"] = formatInt64(s.TotalEqualityDeletesCount)
	}

	// Add extra properties
	for k, v := range s.Extra {
		m[k] = v
	}

	return json.Marshal(m)
}

func formatInt64(v int64) string {
	return fmt.Sprintf("%d", v)
}

// UnmarshalJSON implements json.Unmarshaler for Summary.
func (s *Summary) UnmarshalJSON(data []byte) error {
	var m map[string]string
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}

	s.Extra = make(map[string]string)

	for k, v := range m {
		switch k {
		case "operation":
			s.Operation = Operation(v)
		case "added-data-files":
			s.AddedDataFilesCount = parseInt64(v)
		case "added-records":
			s.AddedRecordsCount = parseInt64(v)
		case "added-files-size":
			s.AddedFileSizeInBytes = parseInt64(v)
		case "removed-data-files":
			s.RemovedDataFilesCount = parseInt64(v)
		case "removed-records":
			s.RemovedRecordsCount = parseInt64(v)
		case "removed-files-size":
			s.RemovedFileSizeInBytes = parseInt64(v)
		case "deleted-data-files":
			s.DeletedDataFilesCount = parseInt64(v)
		case "deleted-records":
			s.DeletedRecordsCount = parseInt64(v)
		case "added-delete-files":
			s.AddedDeleteFilesCount = parseInt64(v)
		case "added-position-deletes":
			s.AddedPositionDeletesCount = parseInt64(v)
		case "added-equality-deletes":
			s.AddedEqualityDeletesCount = parseInt64(v)
		case "total-records":
			s.TotalRecordsCount = parseInt64(v)
		case "total-data-files":
			s.TotalDataFilesCount = parseInt64(v)
		case "total-delete-files":
			s.TotalDeleteFilesCount = parseInt64(v)
		case "total-position-deletes":
			s.TotalPositionDeletesCount = parseInt64(v)
		case "total-equality-deletes":
			s.TotalEqualityDeletesCount = parseInt64(v)
		default:
			s.Extra[k] = v
		}
	}

	return nil
}

func parseInt64(s string) int64 {
	var v int64
	fmt.Sscanf(s, "%d", &v)
	return v
}
