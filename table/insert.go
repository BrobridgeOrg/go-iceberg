package table

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/BrobridgeOrg/go-iceberg/io"
	"github.com/BrobridgeOrg/go-iceberg/spec"
)

// InsertOption configures insert operations.
type InsertOption func(*InsertConfig)

// InsertConfig holds configuration for insert operations.
type InsertConfig struct {
	// TargetFileSizeBytes is the target size for data files
	TargetFileSizeBytes int64
	// Overwrite specifies whether to overwrite existing data (dynamic overwrite)
	Overwrite bool
	// OverwriteFilter specifies a filter for selective overwrite
	OverwriteFilter *Expression
}

// WithTargetFileSizeBytes sets the target file size for inserts.
func WithTargetFileSizeBytes(size int64) InsertOption {
	return func(c *InsertConfig) {
		c.TargetFileSizeBytes = size
	}
}

// WithOverwrite enables overwrite mode.
func WithOverwrite(overwrite bool) InsertOption {
	return func(c *InsertConfig) {
		c.Overwrite = overwrite
	}
}

// WithOverwriteFilter sets a filter for selective overwrite.
func WithOverwriteFilter(filter *Expression) InsertOption {
	return func(c *InsertConfig) {
		c.OverwriteFilter = filter
		c.Overwrite = true
	}
}

// Insert inserts Arrow records into the table.
func (t *Table) Insert(ctx context.Context, records []arrow.Record, opts ...InsertOption) error {
	cfg := &InsertConfig{
		TargetFileSizeBytes: 128 * 1024 * 1024, // 128MB default
	}
	for _, opt := range opts {
		opt(cfg)
	}

	if len(records) == 0 {
		return nil
	}

	// Create data writer
	writer := NewDataWriter(t, t.fileIO)
	if cfg.TargetFileSizeBytes > 0 {
		writer.WithTargetFileSize(cfg.TargetFileSizeBytes)
	}

	// Write data files
	result, err := writer.Write(ctx, records)
	if err != nil {
		return fmt.Errorf("failed to write data files: %w", err)
	}

	if len(result.DataFiles) == 0 {
		return nil
	}

	// Start transaction
	tx := t.NewTransaction()
	if err != nil {
		return fmt.Errorf("failed to create transaction: %w", err)
	}

	// Handle overwrite mode
	if cfg.Overwrite {
		if cfg.OverwriteFilter != nil {
			// Selective overwrite: delete matching data first
			if err := t.deleteMatchingFiles(ctx, tx, cfg.OverwriteFilter); err != nil {
				return fmt.Errorf("failed to delete matching files: %w", err)
			}
		} else {
			// Full overwrite: mark all existing files as deleted
			if err := t.deleteAllFiles(ctx, tx); err != nil {
				return fmt.Errorf("failed to delete existing files: %w", err)
			}
		}
	}

	// Add new data files
	for _, dataFile := range result.DataFiles {
		tx.AppendDataFile(dataFile)
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// InsertTable inserts an Arrow table into the Iceberg table.
func (t *Table) InsertTable(ctx context.Context, tbl arrow.Table, opts ...InsertOption) error {
	cfg := &InsertConfig{
		TargetFileSizeBytes: 128 * 1024 * 1024, // 128MB default
	}
	for _, opt := range opts {
		opt(cfg)
	}

	// Create data writer
	writer := NewDataWriter(t, t.fileIO)
	if cfg.TargetFileSizeBytes > 0 {
		writer.WithTargetFileSize(cfg.TargetFileSizeBytes)
	}

	// Write data files
	result, err := writer.WriteTable(ctx, tbl)
	if err != nil {
		return fmt.Errorf("failed to write data files: %w", err)
	}

	if len(result.DataFiles) == 0 {
		return nil
	}

	// Start transaction
	tx := t.NewTransaction()
	if err != nil {
		return fmt.Errorf("failed to create transaction: %w", err)
	}

	// Handle overwrite mode
	if cfg.Overwrite {
		if err := t.deleteAllFiles(ctx, tx); err != nil {
			return fmt.Errorf("failed to delete existing files: %w", err)
		}
	}

	// Add new data files
	for _, dataFile := range result.DataFiles {
		tx.AppendDataFile(dataFile)
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Append is an alias for Insert without overwrite.
func (t *Table) Append(ctx context.Context, records []arrow.Record, opts ...InsertOption) error {
	return t.Insert(ctx, records, opts...)
}

// AppendTable is an alias for InsertTable without overwrite.
func (t *Table) AppendTable(ctx context.Context, tbl arrow.Table, opts ...InsertOption) error {
	return t.InsertTable(ctx, tbl, opts...)
}

// Overwrite overwrites the table with new data.
func (t *Table) Overwrite(ctx context.Context, records []arrow.Record, opts ...InsertOption) error {
	opts = append([]InsertOption{WithOverwrite(true)}, opts...)
	return t.Insert(ctx, records, opts...)
}

// OverwriteTable overwrites the table with a new Arrow table.
func (t *Table) OverwriteTable(ctx context.Context, tbl arrow.Table, opts ...InsertOption) error {
	opts = append([]InsertOption{WithOverwrite(true)}, opts...)
	return t.InsertTable(ctx, tbl, opts...)
}

// deleteAllFiles marks all existing data files as deleted.
func (t *Table) deleteAllFiles(ctx context.Context, tx *Transaction) error {
	// Get current snapshot
	currentSnapshot := t.metadata.CurrentSnapshot()
	if currentSnapshot == nil {
		return nil // No existing data
	}

	// Read manifest list
	manifests, err := t.readManifestList(ctx, currentSnapshot.ManifestList)
	if err != nil {
		return fmt.Errorf("failed to read manifest list: %w", err)
	}

	// Mark all data files from data manifests as deleted
	for _, mf := range manifests {
		if mf.Content == spec.ManifestContentData {
			manifest, err := t.readManifest(ctx, mf.ManifestPath)
			if err != nil {
				return fmt.Errorf("failed to read manifest: %w", err)
			}

			for _, entry := range manifest.LiveEntries() {
				tx.DeleteDataFile(entry.DataFile)
			}
		}
	}

	return nil
}

// deleteMatchingFiles deletes files that match the given filter.
func (t *Table) deleteMatchingFiles(ctx context.Context, tx *Transaction, filter *Expression) error {
	// Get current snapshot
	currentSnapshot := t.metadata.CurrentSnapshot()
	if currentSnapshot == nil {
		return nil // No existing data
	}

	// Read manifest list
	manifests, err := t.readManifestList(ctx, currentSnapshot.ManifestList)
	if err != nil {
		return fmt.Errorf("failed to read manifest list: %w", err)
	}

	// Find files that might match the filter
	for _, mf := range manifests {
		if mf.Content == spec.ManifestContentData {
			// Check if manifest bounds could match filter
			if !t.manifestMightMatch(mf, filter) {
				continue
			}

			manifest, err := t.readManifest(ctx, mf.ManifestPath)
			if err != nil {
				return fmt.Errorf("failed to read manifest: %w", err)
			}

			for _, entry := range manifest.LiveEntries() {
				// Check if file bounds could match filter
				if t.fileMightMatch(entry.DataFile, filter) {
					tx.DeleteDataFile(entry.DataFile)
				}
			}
		}
	}

	return nil
}

// manifestMightMatch checks if a manifest might contain matching data.
func (t *Table) manifestMightMatch(mf spec.ManifestFile, filter *Expression) bool {
	// For now, assume all manifests might match
	// A full implementation would check partition summaries
	return true
}

// fileMightMatch checks if a file might contain matching data.
func (t *Table) fileMightMatch(df spec.DataFile, filter *Expression) bool {
	// For now, assume all files might match
	// A full implementation would check column bounds
	return true
}

// readManifestList reads a manifest list file.
func (t *Table) readManifestList(ctx context.Context, filePath string) ([]spec.ManifestFile, error) {
	inputFile, err := t.fileIO.Open(ctx, filePath)
	if err != nil {
		return nil, err
	}

	reader, err := inputFile.Open(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return spec.ReadManifestList(reader)
}

// readManifest reads a manifest file.
func (t *Table) readManifest(ctx context.Context, filePath string) (*spec.Manifest, error) {
	inputFile, err := t.fileIO.Open(ctx, filePath)
	if err != nil {
		return nil, err
	}

	reader, err := inputFile.Open(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return spec.ReadManifest(reader)
}

// InsertBuilder provides a fluent interface for insert operations.
type InsertBuilder struct {
	table   *Table
	records []arrow.Record
	arrowTable arrow.Table
	config  *InsertConfig
}

// NewInsert creates a new insert builder.
func (t *Table) NewInsert() *InsertBuilder {
	return &InsertBuilder{
		table: t,
		config: &InsertConfig{
			TargetFileSizeBytes: 128 * 1024 * 1024,
		},
	}
}

// Records sets the records to insert.
func (b *InsertBuilder) Records(records []arrow.Record) *InsertBuilder {
	b.records = records
	return b
}

// Table sets the Arrow table to insert.
func (b *InsertBuilder) Table(tbl arrow.Table) *InsertBuilder {
	b.arrowTable = tbl
	return b
}

// TargetFileSize sets the target file size.
func (b *InsertBuilder) TargetFileSize(size int64) *InsertBuilder {
	b.config.TargetFileSizeBytes = size
	return b
}

// Overwrite enables overwrite mode.
func (b *InsertBuilder) Overwrite() *InsertBuilder {
	b.config.Overwrite = true
	return b
}

// OverwriteFilter sets a filter for selective overwrite.
func (b *InsertBuilder) OverwriteFilter(filter *Expression) *InsertBuilder {
	b.config.OverwriteFilter = filter
	b.config.Overwrite = true
	return b
}

// Execute executes the insert operation.
func (b *InsertBuilder) Execute(ctx context.Context) error {
	opts := []InsertOption{
		WithTargetFileSizeBytes(b.config.TargetFileSizeBytes),
	}

	if b.config.Overwrite {
		if b.config.OverwriteFilter != nil {
			opts = append(opts, WithOverwriteFilter(b.config.OverwriteFilter))
		} else {
			opts = append(opts, WithOverwrite(true))
		}
	}

	if b.arrowTable != nil {
		return b.table.InsertTable(ctx, b.arrowTable, opts...)
	}

	return b.table.Insert(ctx, b.records, opts...)
}

// BulkWriter provides efficient bulk writing capabilities.
type BulkWriter struct {
	table       *Table
	fileIO      io.FileIO
	dataWriter  *DataWriter
	pendingFiles []spec.DataFile
	config      *BulkWriterConfig
}

// BulkWriterConfig configures the bulk writer.
type BulkWriterConfig struct {
	TargetFileSizeBytes int64
	MaxPendingFiles     int
	AutoCommit          bool
	CommitInterval      time.Duration
}

// NewBulkWriter creates a new bulk writer.
func (t *Table) NewBulkWriter(fileIO io.FileIO, cfg *BulkWriterConfig) *BulkWriter {
	if cfg == nil {
		cfg = &BulkWriterConfig{
			TargetFileSizeBytes: 128 * 1024 * 1024,
			MaxPendingFiles:     100,
			AutoCommit:          false,
		}
	}

	return &BulkWriter{
		table:        t,
		fileIO:       fileIO,
		dataWriter:   NewDataWriter(t, fileIO).WithTargetFileSize(cfg.TargetFileSizeBytes),
		pendingFiles: make([]spec.DataFile, 0),
		config:       cfg,
	}
}

// Write writes records and accumulates data files.
func (bw *BulkWriter) Write(ctx context.Context, records []arrow.Record) error {
	result, err := bw.dataWriter.Write(ctx, records)
	if err != nil {
		return err
	}

	bw.pendingFiles = append(bw.pendingFiles, result.DataFiles...)

	// Auto-commit if configured and threshold reached
	if bw.config.AutoCommit && len(bw.pendingFiles) >= bw.config.MaxPendingFiles {
		return bw.Commit(ctx)
	}

	return nil
}

// Commit commits all pending data files as a single snapshot.
func (bw *BulkWriter) Commit(ctx context.Context) error {
	if len(bw.pendingFiles) == 0 {
		return nil
	}

	tx := bw.table.NewTransaction()

	for _, df := range bw.pendingFiles {
		tx.AppendDataFile(df)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	// Clear pending files
	bw.pendingFiles = bw.pendingFiles[:0]

	return nil
}

// Abort discards all pending data files.
func (bw *BulkWriter) Abort(ctx context.Context) error {
	// Delete written files
	for _, df := range bw.pendingFiles {
		if err := bw.fileIO.Delete(ctx, df.FilePath); err != nil {
			// Log error but continue cleanup
		}
	}

	bw.pendingFiles = bw.pendingFiles[:0]
	return nil
}

// PendingFileCount returns the number of pending files.
func (bw *BulkWriter) PendingFileCount() int {
	return len(bw.pendingFiles)
}
