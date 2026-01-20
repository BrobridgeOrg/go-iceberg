package table

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/BrobridgeOrg/go-iceberg/spec"
)

// ScanBuilder builds a table scan with various options.
type ScanBuilder struct {
	table          *Table
	snapshotID     *int64
	asOfTimestamp  *time.Time
	filter         *Expression
	selectedFields []string
	limit          *int64
	caseSensitive  bool
	options        map[string]string
}

// NewScanBuilder creates a new scan builder for the given table.
func NewScanBuilder(t *Table) *ScanBuilder {
	return &ScanBuilder{
		table:         t,
		caseSensitive: true,
		options:       make(map[string]string),
	}
}

// WithSnapshot sets the snapshot ID to scan.
func (sb *ScanBuilder) WithSnapshot(snapshotID int64) *ScanBuilder {
	sb.snapshotID = &snapshotID
	return sb
}

// AsOf sets the timestamp for time travel queries.
func (sb *ScanBuilder) AsOf(timestamp time.Time) *ScanBuilder {
	sb.asOfTimestamp = &timestamp
	return sb
}

// Filter sets the filter expression for the scan.
func (sb *ScanBuilder) Filter(filter *Expression) *ScanBuilder {
	sb.filter = filter
	return sb
}

// Select specifies the columns to return.
func (sb *ScanBuilder) Select(columns ...string) *ScanBuilder {
	sb.selectedFields = columns
	return sb
}

// Limit sets the maximum number of rows to return.
func (sb *ScanBuilder) Limit(n int64) *ScanBuilder {
	sb.limit = &n
	return sb
}

// CaseSensitive sets whether column names are case-sensitive.
func (sb *ScanBuilder) CaseSensitive(b bool) *ScanBuilder {
	sb.caseSensitive = b
	return sb
}

// Option sets a scan option.
func (sb *ScanBuilder) Option(key, value string) *ScanBuilder {
	sb.options[key] = value
	return sb
}

// resolveSnapshot resolves the snapshot to scan.
func (sb *ScanBuilder) resolveSnapshot() (*spec.Snapshot, error) {
	if sb.asOfTimestamp != nil {
		return sb.table.SnapshotAt(*sb.asOfTimestamp)
	}

	if sb.snapshotID != nil {
		snap := sb.table.SnapshotByID(*sb.snapshotID)
		if snap == nil {
			return nil, fmt.Errorf("snapshot %d not found", *sb.snapshotID)
		}
		return snap, nil
	}

	snap := sb.table.CurrentSnapshot()
	if snap == nil {
		return nil, nil // Empty table
	}
	return snap, nil
}

// PlanFiles returns the file scan tasks for the scan.
func (sb *ScanBuilder) PlanFiles(ctx context.Context) ([]FileScanTask, error) {
	snapshot, err := sb.resolveSnapshot()
	if err != nil {
		return nil, err
	}

	if snapshot == nil {
		return nil, nil // Empty table
	}

	// Read manifest list
	if snapshot.ManifestList == "" {
		return nil, nil
	}

	manifestListFile, err := sb.table.fileIO.Open(ctx, snapshot.ManifestList)
	if err != nil {
		return nil, fmt.Errorf("failed to open manifest list: %w", err)
	}

	reader, err := manifestListFile.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest list: %w", err)
	}
	defer reader.Close()

	// Read all bytes
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(reader); err != nil {
		return nil, fmt.Errorf("failed to read manifest list data: %w", err)
	}

	listReader, err := spec.NewManifestListReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		return nil, fmt.Errorf("failed to create manifest list reader: %w", err)
	}

	manifests, err := listReader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest list: %w", err)
	}

	// Read each manifest and collect data files
	var tasks []FileScanTask
	for _, mf := range manifests {
		// Only include data manifests for regular scans
		if mf.Content != spec.ManifestContentData {
			continue
		}

		manifestFile, err := sb.table.fileIO.Open(ctx, mf.ManifestPath)
		if err != nil {
			return nil, fmt.Errorf("failed to open manifest: %w", err)
		}

		mReader, err := manifestFile.Open(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to read manifest: %w", err)
		}

		manifestBuf := new(bytes.Buffer)
		if _, err := manifestBuf.ReadFrom(mReader); err != nil {
			mReader.Close()
			return nil, fmt.Errorf("failed to read manifest data: %w", err)
		}
		mReader.Close()

		manifestReader, err := spec.NewManifestReader(bytes.NewReader(manifestBuf.Bytes()))
		if err != nil {
			return nil, fmt.Errorf("failed to create manifest reader: %w", err)
		}

		manifest, err := manifestReader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read manifest: %w", err)
		}

		// Add live entries as scan tasks
		for _, entry := range manifest.LiveEntries() {
			tasks = append(tasks, FileScanTask{
				File:           entry.DataFile,
				Start:          0,
				Length:         entry.DataFile.FileSizeInBytes,
				PartitionSpecID: mf.PartitionSpecID,
			})
		}
	}

	return tasks, nil
}

// FileScanTask represents a file scan task.
type FileScanTask struct {
	File            spec.DataFile
	Start           int64
	Length          int64
	PartitionSpecID int
	DeleteFiles     []spec.DataFile
}

// ToArrowTable executes the scan and returns the result as an Arrow table.
func (sb *ScanBuilder) ToArrowTable(ctx context.Context) (arrow.Table, error) {
	tasks, err := sb.PlanFiles(ctx)
	if err != nil {
		return nil, err
	}

	if len(tasks) == 0 {
		// Return empty table with schema
		return sb.emptyTable(), nil
	}

	// TODO: Read Parquet files and convert to Arrow
	// For now, return empty table
	return sb.emptyTable(), nil
}

// ToArrowBatches executes the scan and returns record batches.
func (sb *ScanBuilder) ToArrowBatches(ctx context.Context) ([]arrow.Record, error) {
	tasks, err := sb.PlanFiles(ctx)
	if err != nil {
		return nil, err
	}

	if len(tasks) == 0 {
		return nil, nil
	}

	// TODO: Read Parquet files and convert to Arrow records
	return nil, nil
}

// Count returns the count of rows matching the scan criteria.
func (sb *ScanBuilder) Count(ctx context.Context) (int64, error) {
	tasks, err := sb.PlanFiles(ctx)
	if err != nil {
		return 0, err
	}

	var count int64
	for _, task := range tasks {
		count += task.File.RecordCount
	}

	if sb.limit != nil && *sb.limit < count {
		count = *sb.limit
	}

	return count, nil
}

// emptyTable returns an empty Arrow table with the table's schema.
func (sb *ScanBuilder) emptyTable() arrow.Table {
	schema := sb.table.Schema()
	if schema == nil {
		// Return table with empty schema
		arrowSchema := arrow.NewSchema([]arrow.Field{}, nil)
		return array.NewTable(arrowSchema, nil, 0)
	}

	// Convert spec.Schema to arrow.Schema
	fields := make([]arrow.Field, len(schema.Fields))
	for i, f := range schema.Fields {
		fields[i] = arrow.Field{
			Name:     f.Name,
			Type:     specTypeToArrow(f.Type),
			Nullable: !f.Required,
		}
	}

	arrowSchema := arrow.NewSchema(fields, nil)
	pool := memory.NewGoAllocator()
	cols := make([]arrow.Column, len(fields))

	for i, field := range arrowSchema.Fields() {
		builder := array.NewBuilder(pool, field.Type)
		arr := builder.NewArray()
		chunked := arrow.NewChunked(field.Type, []arrow.Array{arr})
		cols[i] = *arrow.NewColumn(field, chunked)
		chunked.Release()
		builder.Release()
	}

	return array.NewTable(arrowSchema, cols, 0)
}

// specTypeToArrow converts a spec.Type to an arrow.DataType.
func specTypeToArrow(t spec.Type) arrow.DataType {
	switch v := t.(type) {
	case spec.PrimitiveType:
		switch v.TypeID() {
		case spec.TypeBoolean:
			return arrow.FixedWidthTypes.Boolean
		case spec.TypeInt:
			return arrow.PrimitiveTypes.Int32
		case spec.TypeLong:
			return arrow.PrimitiveTypes.Int64
		case spec.TypeFloat:
			return arrow.PrimitiveTypes.Float32
		case spec.TypeDouble:
			return arrow.PrimitiveTypes.Float64
		case spec.TypeString:
			return arrow.BinaryTypes.String
		case spec.TypeBinary:
			return arrow.BinaryTypes.Binary
		case spec.TypeDate:
			return arrow.FixedWidthTypes.Date32
		case spec.TypeTime:
			return arrow.FixedWidthTypes.Time64us
		case spec.TypeTimestamp:
			return arrow.FixedWidthTypes.Timestamp_us
		case spec.TypeTimestampTz:
			return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
		case spec.TypeUUID:
			return &arrow.FixedSizeBinaryType{ByteWidth: 16}
		default:
			return arrow.BinaryTypes.String
		}
	case spec.DecimalType:
		return &arrow.Decimal128Type{Precision: int32(v.Precision), Scale: int32(v.Scale)}
	case spec.FixedType:
		return &arrow.FixedSizeBinaryType{ByteWidth: v.Length}
	case spec.ListType:
		elemType := specTypeToArrow(v.Element)
		return arrow.ListOf(elemType)
	case spec.MapType:
		keyType := specTypeToArrow(v.Key)
		valueType := specTypeToArrow(v.Value)
		return arrow.MapOf(keyType, valueType)
	case spec.StructType:
		fields := make([]arrow.Field, len(v.Fields))
		for i, f := range v.Fields {
			fields[i] = arrow.Field{
				Name:     f.Name,
				Type:     specTypeToArrow(f.Type),
				Nullable: !f.Required,
			}
		}
		return arrow.StructOf(fields...)
	default:
		return arrow.BinaryTypes.String
	}
}
