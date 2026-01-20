package spec

import (
	"fmt"
)

// ManifestContent represents the content type of a manifest.
type ManifestContent int

const (
	ManifestContentData   ManifestContent = 0
	ManifestContentDelete ManifestContent = 1
)

// String returns the string representation.
func (c ManifestContent) String() string {
	switch c {
	case ManifestContentData:
		return "data"
	case ManifestContentDelete:
		return "deletes"
	default:
		return "unknown"
	}
}

// FileContent represents the content type of a data file.
type FileContent int

const (
	FileContentData             FileContent = 0
	FileContentPositionDeletes  FileContent = 1
	FileContentEqualityDeletes  FileContent = 2
)

// String returns the string representation.
func (c FileContent) String() string {
	switch c {
	case FileContentData:
		return "data"
	case FileContentPositionDeletes:
		return "position-deletes"
	case FileContentEqualityDeletes:
		return "equality-deletes"
	default:
		return "unknown"
	}
}

// FileFormat represents the format of a data file.
type FileFormat string

const (
	FileFormatParquet FileFormat = "PARQUET"
	FileFormatAvro    FileFormat = "AVRO"
	FileFormatORC     FileFormat = "ORC"
)

// ManifestEntry represents an entry in a manifest file.
type ManifestEntry struct {
	// Status indicates whether the file is added, deleted, or existing
	Status EntryStatus `avro:"status"`

	// SnapshotID is the snapshot that added this file
	SnapshotID *int64 `avro:"snapshot_id"`

	// SequenceNumber is the data sequence number
	SequenceNumber *int64 `avro:"sequence_number"`

	// FileSequenceNumber is the file sequence number
	FileSequenceNumber *int64 `avro:"file_sequence_number"`

	// DataFile contains the data file information
	DataFile DataFile `avro:"data_file"`
}

// EntryStatus represents the status of a manifest entry.
type EntryStatus int

const (
	EntryStatusExisting EntryStatus = 0
	EntryStatusAdded    EntryStatus = 1
	EntryStatusDeleted  EntryStatus = 2
)

// String returns the string representation.
func (s EntryStatus) String() string {
	switch s {
	case EntryStatusExisting:
		return "existing"
	case EntryStatusAdded:
		return "added"
	case EntryStatusDeleted:
		return "deleted"
	default:
		return "unknown"
	}
}

// DataFile represents a data file in a manifest.
type DataFile struct {
	// Content type (data, position deletes, equality deletes)
	Content FileContent `avro:"content"`

	// FilePath is the full URI path to the file
	FilePath string `avro:"file_path"`

	// FileFormat is the file format (parquet, avro, orc)
	FileFormat FileFormat `avro:"file_format"`

	// PartitionData contains partition tuple data
	PartitionData map[string]any `avro:"partition"`

	// RecordCount is the number of records in the file
	RecordCount int64 `avro:"record_count"`

	// FileSizeInBytes is the size of the file in bytes
	FileSizeInBytes int64 `avro:"file_size_in_bytes"`

	// ColumnSizes maps column ID to size in bytes
	ColumnSizes map[int]int64 `avro:"column_sizes"`

	// ValueCounts maps column ID to count of values
	ValueCounts map[int]int64 `avro:"value_counts"`

	// NullValueCounts maps column ID to count of null values
	NullValueCounts map[int]int64 `avro:"null_value_counts"`

	// NaNValueCounts maps column ID to count of NaN values (for floating point)
	NaNValueCounts map[int]int64 `avro:"nan_value_counts"`

	// LowerBounds maps column ID to lower bound value
	LowerBounds map[int][]byte `avro:"lower_bounds"`

	// UpperBounds maps column ID to upper bound value
	UpperBounds map[int][]byte `avro:"upper_bounds"`

	// KeyMetadata is implementation-specific key metadata
	KeyMetadata []byte `avro:"key_metadata"`

	// SplitOffsets is a list of split offsets for the file
	SplitOffsets []int64 `avro:"split_offsets"`

	// EqualityIDs is the list of field IDs for equality deletes
	EqualityIDs []int `avro:"equality_ids"`

	// SortOrderID is the sort order ID
	SortOrderID *int `avro:"sort_order_id"`
}

// ManifestFile represents a manifest file entry in a manifest list.
type ManifestFile struct {
	// ManifestPath is the location of the manifest file
	ManifestPath string `avro:"manifest_path"`

	// ManifestLength is the length of the manifest file
	ManifestLength int64 `avro:"manifest_length"`

	// PartitionSpecID is the spec ID for partition data
	PartitionSpecID int `avro:"partition_spec_id"`

	// Content indicates if this manifest contains data or delete files
	Content ManifestContent `avro:"content"`

	// SequenceNumber is the sequence number when the manifest was added
	SequenceNumber int64 `avro:"sequence_number"`

	// MinSequenceNumber is the minimum data sequence number in this manifest
	MinSequenceNumber int64 `avro:"min_sequence_number"`

	// AddedSnapshotID is the snapshot ID that added this manifest
	AddedSnapshotID int64 `avro:"added_snapshot_id"`

	// AddedFilesCount is the count of files added
	AddedFilesCount int `avro:"added_files_count"`

	// ExistingFilesCount is the count of existing files
	ExistingFilesCount int `avro:"existing_files_count"`

	// DeletedFilesCount is the count of deleted files
	DeletedFilesCount int `avro:"deleted_files_count"`

	// AddedRowsCount is the count of rows added
	AddedRowsCount int64 `avro:"added_rows_count"`

	// ExistingRowsCount is the count of existing rows
	ExistingRowsCount int64 `avro:"existing_rows_count"`

	// DeletedRowsCount is the count of deleted rows
	DeletedRowsCount int64 `avro:"deleted_rows_count"`

	// Partitions is a summary of partitions in this manifest
	Partitions []PartitionFieldSummary `avro:"partitions"`

	// KeyMetadata is implementation-specific key metadata
	KeyMetadata []byte `avro:"key_metadata"`
}

// PartitionFieldSummary summarizes partition values in a manifest.
type PartitionFieldSummary struct {
	ContainsNull bool   `avro:"contains_null"`
	ContainsNaN  *bool  `avro:"contains_nan"`
	LowerBound   []byte `avro:"lower_bound"`
	UpperBound   []byte `avro:"upper_bound"`
}

// HasAddedFiles returns true if this manifest contains added files.
func (m *ManifestFile) HasAddedFiles() bool {
	return m.AddedFilesCount > 0
}

// HasDeletedFiles returns true if this manifest contains deleted files.
func (m *ManifestFile) HasDeletedFiles() bool {
	return m.DeletedFilesCount > 0
}

// HasExistingFiles returns true if this manifest contains existing files.
func (m *ManifestFile) HasExistingFiles() bool {
	return m.ExistingFilesCount > 0
}

// TotalFilesCount returns the total number of files in this manifest.
func (m *ManifestFile) TotalFilesCount() int {
	return m.AddedFilesCount + m.ExistingFilesCount + m.DeletedFilesCount
}

// Manifest represents a parsed manifest file.
type Manifest struct {
	// Metadata from the Avro file
	SchemaID        int             `json:"schema-id"`
	PartitionSpecID int             `json:"partition-spec-id"`
	Content         ManifestContent `json:"content"`
	SequenceNumber  int64           `json:"sequence-number"`

	// Entries in this manifest
	Entries []ManifestEntry `json:"-"`
}

// LiveEntries returns entries that are not deleted.
func (m *Manifest) LiveEntries() []ManifestEntry {
	result := make([]ManifestEntry, 0, len(m.Entries))
	for _, entry := range m.Entries {
		if entry.Status != EntryStatusDeleted {
			result = append(result, entry)
		}
	}
	return result
}

// AddedEntries returns entries that were added.
func (m *Manifest) AddedEntries() []ManifestEntry {
	result := make([]ManifestEntry, 0)
	for _, entry := range m.Entries {
		if entry.Status == EntryStatusAdded {
			result = append(result, entry)
		}
	}
	return result
}

// DeletedEntries returns entries that were deleted.
func (m *Manifest) DeletedEntries() []ManifestEntry {
	result := make([]ManifestEntry, 0)
	for _, entry := range m.Entries {
		if entry.Status == EntryStatusDeleted {
			result = append(result, entry)
		}
	}
	return result
}

// DataFileBuilder helps build DataFile structs.
type DataFileBuilder struct {
	file DataFile
}

// NewDataFileBuilder creates a new data file builder.
func NewDataFileBuilder() *DataFileBuilder {
	return &DataFileBuilder{
		file: DataFile{
			Content:         FileContentData,
			ColumnSizes:     make(map[int]int64),
			ValueCounts:     make(map[int]int64),
			NullValueCounts: make(map[int]int64),
			NaNValueCounts:  make(map[int]int64),
			LowerBounds:     make(map[int][]byte),
			UpperBounds:     make(map[int][]byte),
		},
	}
}

// WithContent sets the file content type.
func (b *DataFileBuilder) WithContent(content FileContent) *DataFileBuilder {
	b.file.Content = content
	return b
}

// WithPath sets the file path.
func (b *DataFileBuilder) WithPath(path string) *DataFileBuilder {
	b.file.FilePath = path
	return b
}

// WithFormat sets the file format.
func (b *DataFileBuilder) WithFormat(format FileFormat) *DataFileBuilder {
	b.file.FileFormat = format
	return b
}

// WithPartition sets the partition data.
func (b *DataFileBuilder) WithPartition(partition map[string]any) *DataFileBuilder {
	b.file.PartitionData = partition
	return b
}

// WithRecordCount sets the record count.
func (b *DataFileBuilder) WithRecordCount(count int64) *DataFileBuilder {
	b.file.RecordCount = count
	return b
}

// WithFileSizeInBytes sets the file size.
func (b *DataFileBuilder) WithFileSizeInBytes(size int64) *DataFileBuilder {
	b.file.FileSizeInBytes = size
	return b
}

// WithColumnSizes sets the column sizes.
func (b *DataFileBuilder) WithColumnSizes(sizes map[int]int64) *DataFileBuilder {
	b.file.ColumnSizes = sizes
	return b
}

// WithValueCounts sets the value counts.
func (b *DataFileBuilder) WithValueCounts(counts map[int]int64) *DataFileBuilder {
	b.file.ValueCounts = counts
	return b
}

// WithNullValueCounts sets the null value counts.
func (b *DataFileBuilder) WithNullValueCounts(counts map[int]int64) *DataFileBuilder {
	b.file.NullValueCounts = counts
	return b
}

// WithBounds sets the lower and upper bounds.
func (b *DataFileBuilder) WithBounds(lower, upper map[int][]byte) *DataFileBuilder {
	b.file.LowerBounds = lower
	b.file.UpperBounds = upper
	return b
}

// WithSplitOffsets sets the split offsets.
func (b *DataFileBuilder) WithSplitOffsets(offsets []int64) *DataFileBuilder {
	b.file.SplitOffsets = offsets
	return b
}

// WithEqualityIDs sets the equality field IDs (for equality delete files).
func (b *DataFileBuilder) WithEqualityIDs(ids []int) *DataFileBuilder {
	b.file.EqualityIDs = ids
	return b
}

// WithSortOrderID sets the sort order ID.
func (b *DataFileBuilder) WithSortOrderID(id int) *DataFileBuilder {
	b.file.SortOrderID = &id
	return b
}

// Build returns the built DataFile.
func (b *DataFileBuilder) Build() DataFile {
	return b.file
}

// Validate validates the data file.
func (f *DataFile) Validate() error {
	if f.FilePath == "" {
		return fmt.Errorf("file path is required")
	}
	if f.FileFormat == "" {
		return fmt.Errorf("file format is required")
	}
	if f.RecordCount < 0 {
		return fmt.Errorf("record count must be non-negative")
	}
	if f.FileSizeInBytes < 0 {
		return fmt.Errorf("file size must be non-negative")
	}

	// Validate content-specific requirements
	switch f.Content {
	case FileContentPositionDeletes:
		// Position deletes must have file_path and pos columns
	case FileContentEqualityDeletes:
		if len(f.EqualityIDs) == 0 {
			return fmt.Errorf("equality delete file must have equality field IDs")
		}
	}

	return nil
}
