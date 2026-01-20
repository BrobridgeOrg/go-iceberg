package table

import (
	"context"
	"crypto/rand"
	"fmt"
	"path"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/go-iceberg/go-iceberg/io"
	"github.com/go-iceberg/go-iceberg/spec"
)

// DataWriter writes data files for an Iceberg table.
type DataWriter struct {
	table      *Table
	fileIO     io.FileIO
	targetSize int64 // target file size in bytes
}

// NewDataWriter creates a new data writer.
func NewDataWriter(table *Table, fileIO io.FileIO) *DataWriter {
	return &DataWriter{
		table:      table,
		fileIO:     fileIO,
		targetSize: 128 * 1024 * 1024, // 128MB default
	}
}

// WithTargetFileSize sets the target file size for written files.
func (w *DataWriter) WithTargetFileSize(size int64) *DataWriter {
	w.targetSize = size
	return w
}

// WriteResult contains information about written data files.
type WriteResult struct {
	DataFiles []spec.DataFile
}

// Write writes Arrow records to Parquet files and returns the data file metadata.
func (w *DataWriter) Write(ctx context.Context, records []arrow.Record) (*WriteResult, error) {
	if len(records) == 0 {
		return &WriteResult{}, nil
	}

	result := &WriteResult{
		DataFiles: make([]spec.DataFile, 0),
	}

	// For simplicity, write all records to a single file
	// In production, you'd split based on target file size and partitions
	dataFile, err := w.writeRecordsToFile(ctx, records)
	if err != nil {
		return nil, err
	}

	result.DataFiles = append(result.DataFiles, *dataFile)

	return result, nil
}

// WriteTable writes an Arrow table to Parquet files.
func (w *DataWriter) WriteTable(ctx context.Context, tbl arrow.Table) (*WriteResult, error) {
	// Convert table to records
	records := make([]arrow.Record, 0)

	reader := array.NewTableReader(tbl, tbl.NumRows())
	defer reader.Release()

	for reader.Next() {
		rec := reader.Record()
		rec.Retain()
		records = append(records, rec)
	}

	return w.Write(ctx, records)
}

// writeRecordsToFile writes records to a single Parquet file.
func (w *DataWriter) writeRecordsToFile(ctx context.Context, records []arrow.Record) (*spec.DataFile, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("no records to write")
	}

	// Generate unique file path
	filePath := w.generateFilePath()

	// Create output file
	outputFile, err := w.fileIO.Create(ctx, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create output file: %w", err)
	}

	// Get writer from output file
	writer, err := outputFile.CreateOverwrite(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get writer: %w", err)
	}
	defer writer.Close()

	// Configure Parquet writer
	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
	)
	arrowProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
	)

	// Create Parquet writer with the first record's schema
	schema := records[0].Schema()
	pqWriter, err := pqarrow.NewFileWriter(schema, writer, writerProps, arrowProps)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	// Write all records
	var totalRecords int64
	for _, rec := range records {
		if err := pqWriter.WriteBuffered(rec); err != nil {
			pqWriter.Close()
			return nil, fmt.Errorf("failed to write record: %w", err)
		}
		totalRecords += rec.NumRows()
	}

	if err := pqWriter.Close(); err != nil {
		return nil, fmt.Errorf("failed to close parquet writer: %w", err)
	}

	// Get file size
	fileSize, err := w.getFileSize(ctx, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get file size: %w", err)
	}

	// Build column statistics
	columnSizes := make(map[int]int64)
	valueCounts := make(map[int]int64)
	nullCounts := make(map[int]int64)

	tableSchema := w.table.Schema()
	for _, field := range tableSchema.Fields {
		valueCounts[field.ID] = totalRecords
		// Approximate column sizes (would need actual stats from parquet)
		columnSizes[field.ID] = fileSize / int64(len(tableSchema.Fields))
	}

	// Create DataFile entry
	dataFile := &spec.DataFile{
		Content:         spec.FileContentData,
		FilePath:        filePath,
		FileFormat:      spec.FileFormatParquet,
		RecordCount:     totalRecords,
		FileSizeInBytes: fileSize,
		ColumnSizes:     columnSizes,
		ValueCounts:     valueCounts,
		NullValueCounts: nullCounts,
		LowerBounds:     make(map[int][]byte),
		UpperBounds:     make(map[int][]byte),
	}

	return dataFile, nil
}

// generateFilePath generates a unique file path for a new data file.
func (w *DataWriter) generateFilePath() string {
	// Generate UUID for uniqueness
	uuid := generateUUID()

	// Use table location as base
	dataDir := path.Join(w.table.Location(), "data")

	// Format: data/{uuid}-{timestamp}.parquet
	filename := fmt.Sprintf("%s-%d.parquet", uuid, time.Now().UnixNano())

	return path.Join(dataDir, filename)
}

// getFileSize gets the size of a file.
func (w *DataWriter) getFileSize(ctx context.Context, filePath string) (int64, error) {
	inputFile, err := w.fileIO.Open(ctx, filePath)
	if err != nil {
		return 0, err
	}

	return inputFile.Length(ctx)
}

// generateUUID generates a random UUID string.
func generateUUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// PartitionedWriter writes data files with partition awareness.
type PartitionedWriter struct {
	*DataWriter
	partitionSpec *spec.PartitionSpec
}

// NewPartitionedWriter creates a new partitioned data writer.
func NewPartitionedWriter(table *Table, fileIO io.FileIO, partitionSpec *spec.PartitionSpec) *PartitionedWriter {
	return &PartitionedWriter{
		DataWriter:    NewDataWriter(table, fileIO),
		partitionSpec: partitionSpec,
	}
}

// Write writes Arrow records with partition awareness.
func (w *PartitionedWriter) Write(ctx context.Context, records []arrow.Record) (*WriteResult, error) {
	if w.partitionSpec == nil || len(w.partitionSpec.Fields) == 0 {
		// No partitioning, use simple write
		return w.DataWriter.Write(ctx, records)
	}

	// Group records by partition values
	partitioned, err := w.partitionRecords(records)
	if err != nil {
		return nil, err
	}

	result := &WriteResult{
		DataFiles: make([]spec.DataFile, 0),
	}

	// Write each partition
	for partitionKey, partitionRecords := range partitioned {
		dataFile, err := w.writePartition(ctx, partitionKey, partitionRecords)
		if err != nil {
			return nil, fmt.Errorf("failed to write partition %s: %w", partitionKey, err)
		}
		result.DataFiles = append(result.DataFiles, *dataFile)
	}

	return result, nil
}

// partitionRecords groups records by partition values.
func (w *PartitionedWriter) partitionRecords(records []arrow.Record) (map[string][]arrow.Record, error) {
	// For simplicity, return all records as a single partition
	// A full implementation would extract partition values and group by them
	result := make(map[string][]arrow.Record)
	result["__default__"] = records
	return result, nil
}

// writePartition writes records for a single partition.
func (w *PartitionedWriter) writePartition(ctx context.Context, partitionKey string, records []arrow.Record) (*spec.DataFile, error) {
	dataFile, err := w.DataWriter.writeRecordsToFile(ctx, records)
	if err != nil {
		return nil, err
	}

	// Add partition data to the data file
	// In a full implementation, we'd parse the partition key and set PartitionData

	return dataFile, nil
}

// DeleteFileWriter writes delete files (position or equality deletes).
type DeleteFileWriter struct {
	table  *Table
	fileIO io.FileIO
}

// NewDeleteFileWriter creates a new delete file writer.
func NewDeleteFileWriter(table *Table, fileIO io.FileIO) *DeleteFileWriter {
	return &DeleteFileWriter{
		table:  table,
		fileIO: fileIO,
	}
}

// WritePositionDeletes writes a position delete file.
// Position deletes specify file_path and pos (row position) to delete.
func (w *DeleteFileWriter) WritePositionDeletes(ctx context.Context, deletes []PositionDelete) (*spec.DataFile, error) {
	if len(deletes) == 0 {
		return nil, nil
	}

	// Create Arrow schema for position deletes
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "file_path", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "pos", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	// Build record batch
	mem := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	filePathBuilder := builder.Field(0).(*array.StringBuilder)
	posBuilder := builder.Field(1).(*array.Int64Builder)

	for _, d := range deletes {
		filePathBuilder.Append(d.FilePath)
		posBuilder.Append(d.Position)
	}

	record := builder.NewRecord()
	defer record.Release()

	// Write to Parquet file
	filePath := w.generateDeleteFilePath("position")

	outputFile, err := w.fileIO.Create(ctx, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create delete file: %w", err)
	}

	writer, err := outputFile.CreateOverwrite(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get writer: %w", err)
	}
	defer writer.Close()

	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
	)
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())

	pqWriter, err := pqarrow.NewFileWriter(schema, writer, writerProps, arrowProps)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	if err := pqWriter.WriteBuffered(record); err != nil {
		pqWriter.Close()
		return nil, fmt.Errorf("failed to write deletes: %w", err)
	}

	if err := pqWriter.Close(); err != nil {
		return nil, fmt.Errorf("failed to close writer: %w", err)
	}

	// Get file size
	inputFile, err := w.fileIO.Open(ctx, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get file size: %w", err)
	}
	fileSize, _ := inputFile.Length(ctx)

	return &spec.DataFile{
		Content:         spec.FileContentPositionDeletes,
		FilePath:        filePath,
		FileFormat:      spec.FileFormatParquet,
		RecordCount:     int64(len(deletes)),
		FileSizeInBytes: fileSize,
	}, nil
}

// WriteEqualityDeletes writes an equality delete file.
// Equality deletes specify column values that should match rows to delete.
func (w *DeleteFileWriter) WriteEqualityDeletes(ctx context.Context, schema *arrow.Schema, equalityIDs []int, records []arrow.Record) (*spec.DataFile, error) {
	if len(records) == 0 {
		return nil, nil
	}

	// Write records to Parquet
	filePath := w.generateDeleteFilePath("equality")

	outputFile, err := w.fileIO.Create(ctx, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create delete file: %w", err)
	}

	writer, err := outputFile.CreateOverwrite(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get writer: %w", err)
	}
	defer writer.Close()

	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
	)
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())

	pqWriter, err := pqarrow.NewFileWriter(schema, writer, writerProps, arrowProps)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	var totalRecords int64
	for _, rec := range records {
		if err := pqWriter.WriteBuffered(rec); err != nil {
			pqWriter.Close()
			return nil, fmt.Errorf("failed to write deletes: %w", err)
		}
		totalRecords += rec.NumRows()
	}

	if err := pqWriter.Close(); err != nil {
		return nil, fmt.Errorf("failed to close writer: %w", err)
	}

	// Get file size
	inputFile, err := w.fileIO.Open(ctx, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get file size: %w", err)
	}
	fileSize, _ := inputFile.Length(ctx)

	return &spec.DataFile{
		Content:         spec.FileContentEqualityDeletes,
		FilePath:        filePath,
		FileFormat:      spec.FileFormatParquet,
		RecordCount:     totalRecords,
		FileSizeInBytes: fileSize,
		EqualityIDs:     equalityIDs,
	}, nil
}

// generateDeleteFilePath generates a path for a delete file.
func (w *DeleteFileWriter) generateDeleteFilePath(deleteType string) string {
	uuid := generateUUID()
	dataDir := path.Join(w.table.Location(), "data")
	filename := fmt.Sprintf("%s-delete-%s-%d.parquet", uuid, deleteType, time.Now().UnixNano())
	return path.Join(dataDir, filename)
}

// PositionDelete represents a single position delete.
type PositionDelete struct {
	FilePath string
	Position int64
}
