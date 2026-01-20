package table

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/BrobridgeOrg/go-iceberg/spec"
)

// UpdateOption configures update operations.
type UpdateOption func(*UpdateConfig)

// UpdateConfig holds configuration for update operations.
type UpdateConfig struct {
	// Mode specifies copy-on-write vs merge-on-read
	Mode DeleteMode
}

// WithUpdateMode sets the update mode.
func WithUpdateMode(mode DeleteMode) UpdateOption {
	return func(c *UpdateConfig) {
		c.Mode = mode
	}
}

// Update updates rows matching the filter with the given values.
// Updates are implemented as Delete + Insert within a single transaction.
func (t *Table) Update(ctx context.Context, filter *Expression, updates map[string]any, opts ...UpdateOption) error {
	cfg := &UpdateConfig{
		Mode: CopyOnWrite,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	if filter == nil {
		return fmt.Errorf("update filter cannot be nil")
	}

	if len(updates) == 0 {
		return nil // Nothing to update
	}

	// Get current snapshot
	currentSnapshot := t.metadata.CurrentSnapshot()
	if currentSnapshot == nil {
		return nil // No data to update
	}

	// Read manifest list
	manifests, err := t.readManifestList(ctx, currentSnapshot.ManifestList)
	if err != nil {
		return fmt.Errorf("failed to read manifest list: %w", err)
	}

	// Start transaction
	tx := t.NewTransaction()
	if err != nil {
		return fmt.Errorf("failed to create transaction: %w", err)
	}

	// Process each data manifest
	for _, mf := range manifests {
		if mf.Content != spec.ManifestContentData {
			continue
		}

		manifest, err := t.readManifest(ctx, mf.ManifestPath)
		if err != nil {
			return fmt.Errorf("failed to read manifest: %w", err)
		}

		for _, entry := range manifest.LiveEntries() {
			// Check if file might contain matching rows
			if !t.fileMightMatch(entry.DataFile, filter) {
				continue
			}

			// Process file: update matching rows
			newFile, hasChanges, err := t.updateFileRows(ctx, entry.DataFile, filter, updates)
			if err != nil {
				return fmt.Errorf("failed to update file %s: %w", entry.DataFile.FilePath, err)
			}

			if hasChanges {
				// Mark old file as deleted
				tx.DeleteDataFile(entry.DataFile)

				// Add new file with updated rows
				if newFile != nil {
					tx.AppendDataFile(*newFile)
				}
			}
		}
	}

	return tx.Commit(ctx)
}

// updateFileRows reads a file, updates matching rows, and writes a new file.
func (t *Table) updateFileRows(ctx context.Context, df spec.DataFile, filter *Expression, updates map[string]any) (*spec.DataFile, bool, error) {
	// Read Parquet file into Arrow table
	table, err := t.readParquetFile(ctx, df.FilePath)
	if err != nil {
		return nil, false, fmt.Errorf("failed to read parquet file: %w", err)
	}
	defer table.Release()

	// Update matching rows
	updatedRecords, updated, err := t.applyUpdates(ctx, table, filter, updates)
	if err != nil {
		return nil, false, fmt.Errorf("failed to apply updates: %w", err)
	}

	if !updated {
		return nil, false, nil
	}

	// Write new file with updated rows
	writer := NewDataWriter(t, t.fileIO)
	result, err := writer.Write(ctx, updatedRecords)
	if err != nil {
		return nil, false, fmt.Errorf("failed to write updated file: %w", err)
	}

	// Clean up records
	for _, rec := range updatedRecords {
		rec.Release()
	}

	if len(result.DataFiles) == 0 {
		return nil, true, nil
	}

	return &result.DataFiles[0], true, nil
}

// applyUpdates applies updates to matching rows.
func (t *Table) applyUpdates(ctx context.Context, table arrow.Table, filter *Expression, updates map[string]any) ([]arrow.Record, bool, error) {
	schema := table.Schema()
	mem := memory.NewGoAllocator()

	var updatedRecords []arrow.Record
	hasUpdates := false

	// Map column names to indices
	updateColIndices := make(map[int]any)
	for colName, newValue := range updates {
		for i, field := range schema.Fields() {
			if field.Name == colName {
				updateColIndices[i] = newValue
				break
			}
		}
	}

	// Process table
	reader := array.NewTableReader(table, 1024)
	defer reader.Release()

	for reader.Next() {
		rec := reader.Record()
		newRec, updated := t.applyUpdatesToRecord(rec, schema, filter, updateColIndices, mem)
		if updated {
			hasUpdates = true
		}
		if newRec != nil {
			updatedRecords = append(updatedRecords, newRec)
		}
	}

	return updatedRecords, hasUpdates, nil
}

// applyUpdatesToRecord applies updates to a single record.
func (t *Table) applyUpdatesToRecord(rec arrow.Record, schema *arrow.Schema, filter *Expression, updateColIndices map[int]any, mem memory.Allocator) (arrow.Record, bool) {
	numRows := rec.NumRows()
	if numRows == 0 {
		return nil, false
	}

	// Find which rows match the filter
	matchMask := make([]bool, numRows)
	anyMatch := false
	for i := int64(0); i < numRows; i++ {
		if t.evaluateExpressionOnRow(rec, i, filter) {
			matchMask[i] = true
			anyMatch = true
		}
	}

	if !anyMatch {
		rec.Retain()
		return rec, false
	}

	// Build new record with updates applied
	builders := make([]array.Builder, rec.NumCols())
	for i := 0; i < int(rec.NumCols()); i++ {
		builders[i] = array.NewBuilder(mem, rec.Column(i).DataType())
	}

	for rowIdx := int64(0); rowIdx < numRows; rowIdx++ {
		for colIdx := 0; colIdx < int(rec.NumCols()); colIdx++ {
			col := rec.Column(colIdx)

			if matchMask[rowIdx] {
				// Check if this column has an update
				if newValue, ok := updateColIndices[colIdx]; ok {
					appendTypedValue(builders[colIdx], newValue)
				} else {
					appendValue(builders[colIdx], col, int(rowIdx))
				}
			} else {
				appendValue(builders[colIdx], col, int(rowIdx))
			}
		}
	}

	// Build arrays
	arrays := make([]arrow.Array, rec.NumCols())
	for i, b := range builders {
		arrays[i] = b.NewArray()
		b.Release()
	}

	newRec := array.NewRecord(schema, arrays, numRows)

	// Release built arrays
	for _, arr := range arrays {
		arr.Release()
	}

	return newRec, true
}

// appendTypedValue appends a typed value to a builder.
func appendTypedValue(builder array.Builder, value any) {
	if value == nil {
		builder.AppendNull()
		return
	}

	switch b := builder.(type) {
	case *array.Int32Builder:
		switch v := value.(type) {
		case int:
			b.Append(int32(v))
		case int32:
			b.Append(v)
		case int64:
			b.Append(int32(v))
		default:
			b.AppendNull()
		}
	case *array.Int64Builder:
		switch v := value.(type) {
		case int:
			b.Append(int64(v))
		case int32:
			b.Append(int64(v))
		case int64:
			b.Append(v)
		default:
			b.AppendNull()
		}
	case *array.Float32Builder:
		switch v := value.(type) {
		case float32:
			b.Append(v)
		case float64:
			b.Append(float32(v))
		default:
			b.AppendNull()
		}
	case *array.Float64Builder:
		switch v := value.(type) {
		case float32:
			b.Append(float64(v))
		case float64:
			b.Append(v)
		default:
			b.AppendNull()
		}
	case *array.StringBuilder:
		switch v := value.(type) {
		case string:
			b.Append(v)
		default:
			b.Append(fmt.Sprintf("%v", v))
		}
	case *array.BooleanBuilder:
		switch v := value.(type) {
		case bool:
			b.Append(v)
		default:
			b.AppendNull()
		}
	default:
		builder.AppendNull()
	}
}

// UpdateBuilder provides a fluent interface for update operations.
type UpdateBuilder struct {
	table   *Table
	filter  *Expression
	updates map[string]any
	config  *UpdateConfig
}

// NewUpdate creates a new update builder.
func (t *Table) NewUpdate() *UpdateBuilder {
	return &UpdateBuilder{
		table:   t,
		updates: make(map[string]any),
		config:  &UpdateConfig{Mode: CopyOnWrite},
	}
}

// Where sets the update filter.
func (b *UpdateBuilder) Where(filter *Expression) *UpdateBuilder {
	b.filter = filter
	return b
}

// Set sets a column value.
func (b *UpdateBuilder) Set(column string, value any) *UpdateBuilder {
	b.updates[column] = value
	return b
}

// SetMap sets multiple column values.
func (b *UpdateBuilder) SetMap(updates map[string]any) *UpdateBuilder {
	for k, v := range updates {
		b.updates[k] = v
	}
	return b
}

// CopyOnWrite sets copy-on-write mode.
func (b *UpdateBuilder) CopyOnWrite() *UpdateBuilder {
	b.config.Mode = CopyOnWrite
	return b
}

// Execute executes the update operation.
func (b *UpdateBuilder) Execute(ctx context.Context) error {
	opts := []UpdateOption{
		WithUpdateMode(b.config.Mode),
	}
	return b.table.Update(ctx, b.filter, b.updates, opts...)
}

// Upsert performs an upsert (insert or update) operation.
// Rows with matching keys are updated, new rows are inserted.
func (t *Table) Upsert(ctx context.Context, records []arrow.Record, keyColumns []string, opts ...UpdateOption) error {
	cfg := &UpdateConfig{
		Mode: CopyOnWrite,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	if len(records) == 0 {
		return nil
	}

	if len(keyColumns) == 0 {
		return fmt.Errorf("upsert requires at least one key column")
	}

	// Get current snapshot
	currentSnapshot := t.metadata.CurrentSnapshot()
	if currentSnapshot == nil {
		// No existing data, just insert
		return t.Insert(ctx, records)
	}

	// Build lookup of incoming keys
	incomingKeys := make(map[string]arrow.Record)
	for _, rec := range records {
		for i := int64(0); i < rec.NumRows(); i++ {
			key := extractKeyString(rec, i, keyColumns)
			incomingKeys[key] = rec
		}
	}

	// Read manifest list
	manifests, err := t.readManifestList(ctx, currentSnapshot.ManifestList)
	if err != nil {
		return fmt.Errorf("failed to read manifest list: %w", err)
	}

	// Start transaction
	tx := t.NewTransaction()
	if err != nil {
		return fmt.Errorf("failed to create transaction: %w", err)
	}

	// Track which keys exist
	existingKeys := make(map[string]bool)

	// Process existing files
	for _, mf := range manifests {
		if mf.Content != spec.ManifestContentData {
			continue
		}

		manifest, err := t.readManifest(ctx, mf.ManifestPath)
		if err != nil {
			return fmt.Errorf("failed to read manifest: %w", err)
		}

		for _, entry := range manifest.LiveEntries() {
			newFile, hasChanges, keysFound, err := t.upsertFile(ctx, entry.DataFile, records, keyColumns, incomingKeys)
			if err != nil {
				return fmt.Errorf("failed to process file: %w", err)
			}

			for k := range keysFound {
				existingKeys[k] = true
			}

			if hasChanges {
				tx.DeleteDataFile(entry.DataFile)
				if newFile != nil {
					tx.AppendDataFile(*newFile)
				}
			}
		}
	}

	// Insert rows for keys that don't exist
	var newRecords []arrow.Record
	for _, rec := range records {
		newRec := filterRecordByKeyExistence(rec, keyColumns, existingKeys, false)
		if newRec != nil && newRec.NumRows() > 0 {
			newRecords = append(newRecords, newRec)
		}
	}

	if len(newRecords) > 0 {
		writer := NewDataWriter(t, t.fileIO)
		result, err := writer.Write(ctx, newRecords)
		if err != nil {
			return fmt.Errorf("failed to write new records: %w", err)
		}

		for _, df := range result.DataFiles {
			tx.AppendDataFile(df)
		}
	}

	return tx.Commit(ctx)
}

// upsertFile processes a file for upsert.
func (t *Table) upsertFile(ctx context.Context, df spec.DataFile, newRecords []arrow.Record, keyColumns []string, incomingKeys map[string]arrow.Record) (*spec.DataFile, bool, map[string]bool, error) {
	table, err := t.readParquetFile(ctx, df.FilePath)
	if err != nil {
		return nil, false, nil, err
	}
	defer table.Release()

	// Process rows
	keysFound := make(map[string]bool)
	hasChanges := false
	mem := memory.NewGoAllocator()

	schema := table.Schema()
	tableReader := array.NewTableReader(table, 1024)
	defer tableReader.Release()

	var updatedRecords []arrow.Record

	for tableReader.Next() {
		rec := tableReader.Record()
		newRec, changed, keys := t.upsertRecord(rec, schema, keyColumns, incomingKeys, mem)
		if changed {
			hasChanges = true
		}
		for k := range keys {
			keysFound[k] = true
		}
		if newRec != nil {
			updatedRecords = append(updatedRecords, newRec)
		}
	}

	if !hasChanges {
		return nil, false, keysFound, nil
	}

	// Write updated file
	writer := NewDataWriter(t, t.fileIO)
	result, err := writer.Write(ctx, updatedRecords)
	if err != nil {
		return nil, false, nil, err
	}

	for _, rec := range updatedRecords {
		rec.Release()
	}

	if len(result.DataFiles) == 0 {
		return nil, true, keysFound, nil
	}

	return &result.DataFiles[0], true, keysFound, nil
}

// upsertRecord updates a record with upsert logic.
func (t *Table) upsertRecord(rec arrow.Record, schema *arrow.Schema, keyColumns []string, incomingKeys map[string]arrow.Record, mem memory.Allocator) (arrow.Record, bool, map[string]bool) {
	numRows := rec.NumRows()
	keysFound := make(map[string]bool)
	hasChanges := false

	builders := make([]array.Builder, rec.NumCols())
	for i := 0; i < int(rec.NumCols()); i++ {
		builders[i] = array.NewBuilder(mem, rec.Column(i).DataType())
	}

	for rowIdx := int64(0); rowIdx < numRows; rowIdx++ {
		key := extractKeyString(rec, rowIdx, keyColumns)

		if incomingRec, exists := incomingKeys[key]; exists {
			keysFound[key] = true
			hasChanges = true

			// Find matching row in incoming record
			for i := int64(0); i < incomingRec.NumRows(); i++ {
				if extractKeyString(incomingRec, i, keyColumns) == key {
					// Use values from incoming record
					for colIdx := 0; colIdx < int(rec.NumCols()); colIdx++ {
						colName := schema.Field(colIdx).Name
						// Find this column in incoming record
						incomingColIdx := -1
						for j := 0; j < int(incomingRec.NumCols()); j++ {
							if incomingRec.ColumnName(j) == colName {
								incomingColIdx = j
								break
							}
						}
						if incomingColIdx >= 0 {
							appendValue(builders[colIdx], incomingRec.Column(incomingColIdx), int(i))
						} else {
							appendValue(builders[colIdx], rec.Column(colIdx), int(rowIdx))
						}
					}
					break
				}
			}
		} else {
			// Keep original row
			for colIdx := 0; colIdx < int(rec.NumCols()); colIdx++ {
				appendValue(builders[colIdx], rec.Column(colIdx), int(rowIdx))
			}
		}
	}

	arrays := make([]arrow.Array, rec.NumCols())
	for i, b := range builders {
		arrays[i] = b.NewArray()
		b.Release()
	}

	newRec := array.NewRecord(schema, arrays, numRows)

	for _, arr := range arrays {
		arr.Release()
	}

	return newRec, hasChanges, keysFound
}

// extractKeyString extracts a key string from a record row.
func extractKeyString(rec arrow.Record, rowIdx int64, keyColumns []string) string {
	parts := make([]string, len(keyColumns))
	for i, colName := range keyColumns {
		for colIdx := 0; colIdx < int(rec.NumCols()); colIdx++ {
			if rec.ColumnName(colIdx) == colName {
				value := getValueAt(rec.Column(colIdx), int(rowIdx))
				parts[i] = fmt.Sprintf("%v", value)
				break
			}
		}
	}
	return fmt.Sprintf("%v", parts)
}

// filterRecordByKeyExistence filters a record by whether keys exist.
func filterRecordByKeyExistence(rec arrow.Record, keyColumns []string, existingKeys map[string]bool, keepExisting bool) arrow.Record {
	numRows := rec.NumRows()
	mem := memory.NewGoAllocator()

	keepMask := make([]bool, numRows)
	keepCount := int64(0)

	for i := int64(0); i < numRows; i++ {
		key := extractKeyString(rec, i, keyColumns)
		exists := existingKeys[key]
		keep := (keepExisting && exists) || (!keepExisting && !exists)
		keepMask[i] = keep
		if keep {
			keepCount++
		}
	}

	if keepCount == 0 {
		return nil
	}

	if keepCount == numRows {
		rec.Retain()
		return rec
	}

	builders := make([]array.Builder, rec.NumCols())
	for i := 0; i < int(rec.NumCols()); i++ {
		builders[i] = array.NewBuilder(mem, rec.Column(i).DataType())
	}

	for rowIdx := int64(0); rowIdx < numRows; rowIdx++ {
		if !keepMask[rowIdx] {
			continue
		}
		for colIdx := 0; colIdx < int(rec.NumCols()); colIdx++ {
			appendValue(builders[colIdx], rec.Column(colIdx), int(rowIdx))
		}
	}

	arrays := make([]arrow.Array, rec.NumCols())
	for i, b := range builders {
		arrays[i] = b.NewArray()
		b.Release()
	}

	schema := rec.Schema()
	newRec := array.NewRecord(schema, arrays, keepCount)

	for _, arr := range arrays {
		arr.Release()
	}

	return newRec
}

// UpsertBuilder provides a fluent interface for upsert operations.
type UpsertBuilder struct {
	table      *Table
	records    []arrow.Record
	keyColumns []string
	config     *UpdateConfig
}

// NewUpsert creates a new upsert builder.
func (t *Table) NewUpsert() *UpsertBuilder {
	return &UpsertBuilder{
		table:  t,
		config: &UpdateConfig{Mode: CopyOnWrite},
	}
}

// Records sets the records to upsert.
func (b *UpsertBuilder) Records(records []arrow.Record) *UpsertBuilder {
	b.records = records
	return b
}

// KeyColumns sets the key columns for matching.
func (b *UpsertBuilder) KeyColumns(columns ...string) *UpsertBuilder {
	b.keyColumns = columns
	return b
}

// Execute executes the upsert operation.
func (b *UpsertBuilder) Execute(ctx context.Context) error {
	opts := []UpdateOption{
		WithUpdateMode(b.config.Mode),
	}
	return b.table.Upsert(ctx, b.records, b.keyColumns, opts...)
}
