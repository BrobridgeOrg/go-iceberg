package table

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/go-iceberg/go-iceberg/spec"
)

// DeleteMode specifies the delete strategy.
type DeleteMode int

const (
	// CopyOnWrite rewrites data files without deleted rows.
	CopyOnWrite DeleteMode = iota
	// MergeOnRead writes delete files that are merged at read time.
	MergeOnRead
)

// DeleteOption configures delete operations.
type DeleteOption func(*DeleteConfig)

// DeleteConfig holds configuration for delete operations.
type DeleteConfig struct {
	Mode DeleteMode
	// For MergeOnRead, specify whether to use position or equality deletes
	UseEqualityDeletes bool
	// Equality delete field IDs (required for equality deletes)
	EqualityFieldIDs []int
}

// WithDeleteMode sets the delete mode.
func WithDeleteMode(mode DeleteMode) DeleteOption {
	return func(c *DeleteConfig) {
		c.Mode = mode
	}
}

// WithEqualityDeletes enables equality deletes with the specified field IDs.
func WithEqualityDeletes(fieldIDs []int) DeleteOption {
	return func(c *DeleteConfig) {
		c.Mode = MergeOnRead
		c.UseEqualityDeletes = true
		c.EqualityFieldIDs = fieldIDs
	}
}

// Delete deletes rows matching the given filter.
func (t *Table) Delete(ctx context.Context, filter *Expression, opts ...DeleteOption) error {
	cfg := &DeleteConfig{
		Mode: CopyOnWrite, // Default to Copy-on-Write
	}
	for _, opt := range opts {
		opt(cfg)
	}

	if filter == nil {
		return fmt.Errorf("delete filter cannot be nil")
	}

	switch cfg.Mode {
	case CopyOnWrite:
		return t.deleteCopyOnWrite(ctx, filter)
	case MergeOnRead:
		if cfg.UseEqualityDeletes {
			return t.deleteWithEqualityDeletes(ctx, filter, cfg.EqualityFieldIDs)
		}
		return t.deleteWithPositionDeletes(ctx, filter)
	default:
		return fmt.Errorf("unknown delete mode: %d", cfg.Mode)
	}
}

// deleteCopyOnWrite implements Copy-on-Write delete strategy.
// It reads matching files, filters out deleted rows, and writes new files.
func (t *Table) deleteCopyOnWrite(ctx context.Context, filter *Expression) error {
	// Get current snapshot
	currentSnapshot := t.metadata.CurrentSnapshot()
	if currentSnapshot == nil {
		return nil // No data to delete
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

			// Read file and filter rows
			newFile, rowsDeleted, err := t.rewriteFileWithoutMatches(ctx, entry.DataFile, filter)
			if err != nil {
				return fmt.Errorf("failed to rewrite file %s: %w", entry.DataFile.FilePath, err)
			}

			if rowsDeleted > 0 {
				// Mark old file as deleted
				tx.DeleteDataFile(entry.DataFile)

				// Add new file if it has remaining rows
				if newFile != nil {
					tx.AppendDataFile(*newFile)
				}
			}
		}
	}

	// Commit transaction
	return tx.Commit(ctx)
}

// rewriteFileWithoutMatches reads a file and writes a new file without matching rows.
func (t *Table) rewriteFileWithoutMatches(ctx context.Context, df spec.DataFile, filter *Expression) (*spec.DataFile, int64, error) {
	// Read Parquet file into Arrow table
	table, err := t.readParquetFile(ctx, df.FilePath)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read parquet file: %w", err)
	}
	defer table.Release()

	// Filter rows - keep rows that DON'T match the delete filter
	filteredRecords, deletedCount, err := t.filterRecords(ctx, table, filter, true)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to filter records: %w", err)
	}

	if deletedCount == 0 {
		// No rows matched, no changes needed
		return nil, 0, nil
	}

	if len(filteredRecords) == 0 {
		// All rows deleted, no new file needed
		return nil, deletedCount, nil
	}

	// Write new file with remaining rows
	writer := NewDataWriter(t, t.fileIO)
	result, err := writer.Write(ctx, filteredRecords)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to write new file: %w", err)
	}

	// Clean up records
	for _, rec := range filteredRecords {
		rec.Release()
	}

	if len(result.DataFiles) == 0 {
		return nil, deletedCount, nil
	}

	return &result.DataFiles[0], deletedCount, nil
}

// filterRecords filters records based on expression.
// If negate is true, returns rows that DON'T match (for delete).
func (t *Table) filterRecords(ctx context.Context, table arrow.Table, filter *Expression, negate bool) ([]arrow.Record, int64, error) {
	schema := table.Schema()
	mem := memory.NewGoAllocator()

	var filteredRecords []arrow.Record
	var deletedCount int64

	// Process table row by row (simplified - production would use batch operations)
	reader := array.NewTableReader(table, 1024)
	defer reader.Release()

	for reader.Next() {
		rec := reader.Record()
		newRec, deleted := t.filterRecord(rec, schema, filter, negate, mem)
		deletedCount += deleted
		if newRec != nil && newRec.NumRows() > 0 {
			filteredRecords = append(filteredRecords, newRec)
		}
	}

	return filteredRecords, deletedCount, nil
}

// filterRecord filters a single record.
func (t *Table) filterRecord(rec arrow.Record, schema *arrow.Schema, filter *Expression, negate bool, mem memory.Allocator) (arrow.Record, int64) {
	numRows := rec.NumRows()
	if numRows == 0 {
		return nil, 0
	}

	// Evaluate filter for each row
	keepMask := make([]bool, numRows)
	var keepCount int64
	var deleteCount int64

	for i := int64(0); i < numRows; i++ {
		matches := t.evaluateExpressionOnRow(rec, i, filter)
		if negate {
			// For delete: keep rows that don't match
			keepMask[i] = !matches
		} else {
			// For select: keep rows that match
			keepMask[i] = matches
		}
		if keepMask[i] {
			keepCount++
		} else {
			deleteCount++
		}
	}

	if keepCount == numRows {
		// All rows kept
		rec.Retain()
		return rec, 0
	}

	if keepCount == 0 {
		// All rows filtered out
		return nil, numRows
	}

	// Build new record with only kept rows
	builders := make([]array.Builder, rec.NumCols())
	for i := 0; i < int(rec.NumCols()); i++ {
		builders[i] = array.NewBuilder(mem, rec.Column(i).DataType())
	}

	for rowIdx := int64(0); rowIdx < numRows; rowIdx++ {
		if !keepMask[rowIdx] {
			continue
		}

		for colIdx := 0; colIdx < int(rec.NumCols()); colIdx++ {
			col := rec.Column(colIdx)
			appendValue(builders[colIdx], col, int(rowIdx))
		}
	}

	// Build arrays
	arrays := make([]arrow.Array, rec.NumCols())
	for i, b := range builders {
		arrays[i] = b.NewArray()
		b.Release()
	}

	newRec := array.NewRecord(schema, arrays, keepCount)

	// Release built arrays (record took ownership)
	for _, arr := range arrays {
		arr.Release()
	}

	return newRec, deleteCount
}

// evaluateExpressionOnRow evaluates a filter expression on a single row.
func (t *Table) evaluateExpressionOnRow(rec arrow.Record, rowIdx int64, filter *Expression) bool {
	if filter == nil {
		return true
	}

	switch filter.Op {
	case OpAnd:
		for _, child := range filter.Children {
			if !t.evaluateExpressionOnRow(rec, rowIdx, child) {
				return false
			}
		}
		return true

	case OpOr:
		for _, child := range filter.Children {
			if t.evaluateExpressionOnRow(rec, rowIdx, child) {
				return true
			}
		}
		return false

	case OpNot:
		if len(filter.Children) > 0 {
			return !t.evaluateExpressionOnRow(rec, rowIdx, filter.Children[0])
		}
		return true

	case OpEq, OpNotEq, OpLt, OpLte, OpGt, OpGte:
		return t.evaluateComparisonOnRow(rec, rowIdx, filter)

	case OpIn:
		return t.evaluateInOnRow(rec, rowIdx, filter)

	case OpIsNull:
		return t.evaluateIsNullOnRow(rec, rowIdx, filter, true)

	case OpNotNull:
		return t.evaluateIsNullOnRow(rec, rowIdx, filter, false)

	default:
		return true
	}
}

// evaluateComparisonOnRow evaluates a comparison expression on a row.
func (t *Table) evaluateComparisonOnRow(rec arrow.Record, rowIdx int64, filter *Expression) bool {
	// Find column
	colIdx := -1
	for i := 0; i < int(rec.NumCols()); i++ {
		if rec.ColumnName(i) == filter.Column {
			colIdx = i
			break
		}
	}
	if colIdx == -1 {
		return false
	}

	col := rec.Column(colIdx)
	if col.IsNull(int(rowIdx)) {
		return false
	}

	// Get value and compare
	value := getValueAt(col, int(rowIdx))
	return compareValues(value, filter.Value, filter.Op)
}

// evaluateInOnRow evaluates an IN expression on a row.
func (t *Table) evaluateInOnRow(rec arrow.Record, rowIdx int64, filter *Expression) bool {
	// Find column
	colIdx := -1
	for i := 0; i < int(rec.NumCols()); i++ {
		if rec.ColumnName(i) == filter.Column {
			colIdx = i
			break
		}
	}
	if colIdx == -1 {
		return false
	}

	col := rec.Column(colIdx)
	if col.IsNull(int(rowIdx)) {
		return false
	}

	value := getValueAt(col, int(rowIdx))

	for _, v := range filter.Values {
		if compareValues(value, v, OpEq) {
			return true
		}
	}
	return false
}

// evaluateIsNullOnRow evaluates an IS NULL expression on a row.
func (t *Table) evaluateIsNullOnRow(rec arrow.Record, rowIdx int64, filter *Expression, expectNull bool) bool {
	colIdx := -1
	for i := 0; i < int(rec.NumCols()); i++ {
		if rec.ColumnName(i) == filter.Column {
			colIdx = i
			break
		}
	}
	if colIdx == -1 {
		return false
	}

	col := rec.Column(colIdx)
	isNull := col.IsNull(int(rowIdx))
	return isNull == expectNull
}

// deleteWithPositionDeletes implements Merge-on-Read with position deletes.
func (t *Table) deleteWithPositionDeletes(ctx context.Context, filter *Expression) error {
	currentSnapshot := t.metadata.CurrentSnapshot()
	if currentSnapshot == nil {
		return nil
	}

	manifests, err := t.readManifestList(ctx, currentSnapshot.ManifestList)
	if err != nil {
		return fmt.Errorf("failed to read manifest list: %w", err)
	}

	tx := t.NewTransaction()
	if err != nil {
		return fmt.Errorf("failed to create transaction: %w", err)
	}

	deleteWriter := NewDeleteFileWriter(t, t.fileIO)
	var allDeletes []PositionDelete

	// Find all rows to delete
	for _, mf := range manifests {
		if mf.Content != spec.ManifestContentData {
			continue
		}

		manifest, err := t.readManifest(ctx, mf.ManifestPath)
		if err != nil {
			return fmt.Errorf("failed to read manifest: %w", err)
		}

		for _, entry := range manifest.LiveEntries() {
			if !t.fileMightMatch(entry.DataFile, filter) {
				continue
			}

			// Find matching row positions
			positions, err := t.findMatchingPositions(ctx, entry.DataFile, filter)
			if err != nil {
				return fmt.Errorf("failed to find matching positions: %w", err)
			}

			for _, pos := range positions {
				allDeletes = append(allDeletes, PositionDelete{
					FilePath: entry.DataFile.FilePath,
					Position: pos,
				})
			}
		}
	}

	if len(allDeletes) == 0 {
		return nil // No rows to delete
	}

	// Write position delete file
	deleteFile, err := deleteWriter.WritePositionDeletes(ctx, allDeletes)
	if err != nil {
		return fmt.Errorf("failed to write delete file: %w", err)
	}

	tx.AppendDeleteFile(*deleteFile)

	return tx.Commit(ctx)
}

// findMatchingPositions finds row positions that match the filter.
func (t *Table) findMatchingPositions(ctx context.Context, df spec.DataFile, filter *Expression) ([]int64, error) {
	table, err := t.readParquetFile(ctx, df.FilePath)
	if err != nil {
		return nil, err
	}
	defer table.Release()

	var positions []int64
	var currentPos int64

	tableReader := array.NewTableReader(table, 1024)
	defer tableReader.Release()

	for tableReader.Next() {
		rec := tableReader.Record()
		for i := int64(0); i < rec.NumRows(); i++ {
			if t.evaluateExpressionOnRow(rec, i, filter) {
				positions = append(positions, currentPos+i)
			}
		}
		currentPos += rec.NumRows()
	}

	return positions, nil
}

// deleteWithEqualityDeletes implements Merge-on-Read with equality deletes.
func (t *Table) deleteWithEqualityDeletes(ctx context.Context, filter *Expression, equalityFieldIDs []int) error {
	// For equality deletes, we write a delete file containing the column values
	// that identify rows to delete

	// This is a simplified implementation
	// A full implementation would extract the equality values from the filter
	return fmt.Errorf("equality deletes not yet fully implemented")
}

// DeleteBuilder provides a fluent interface for delete operations.
type DeleteBuilder struct {
	table  *Table
	filter *Expression
	config *DeleteConfig
}

// NewDelete creates a new delete builder.
func (t *Table) NewDelete() *DeleteBuilder {
	return &DeleteBuilder{
		table:  t,
		config: &DeleteConfig{Mode: CopyOnWrite},
	}
}

// Where sets the delete filter.
func (b *DeleteBuilder) Where(filter *Expression) *DeleteBuilder {
	b.filter = filter
	return b
}

// CopyOnWrite sets copy-on-write mode.
func (b *DeleteBuilder) CopyOnWrite() *DeleteBuilder {
	b.config.Mode = CopyOnWrite
	return b
}

// MergeOnRead sets merge-on-read mode with position deletes.
func (b *DeleteBuilder) MergeOnRead() *DeleteBuilder {
	b.config.Mode = MergeOnRead
	return b
}

// Execute executes the delete operation.
func (b *DeleteBuilder) Execute(ctx context.Context) error {
	opts := []DeleteOption{
		WithDeleteMode(b.config.Mode),
	}

	if b.config.UseEqualityDeletes {
		opts = append(opts, WithEqualityDeletes(b.config.EqualityFieldIDs))
	}

	return b.table.Delete(ctx, b.filter, opts...)
}

// Helper functions

// getValueAt gets the value at a specific index from an Arrow array.
func getValueAt(arr arrow.Array, idx int) any {
	if arr.IsNull(idx) {
		return nil
	}

	switch a := arr.(type) {
	case *array.Int32:
		return a.Value(idx)
	case *array.Int64:
		return a.Value(idx)
	case *array.Float32:
		return a.Value(idx)
	case *array.Float64:
		return a.Value(idx)
	case *array.String:
		return a.Value(idx)
	case *array.Boolean:
		return a.Value(idx)
	case *array.Binary:
		return a.Value(idx)
	case *array.Date32:
		return a.Value(idx)
	case *array.Date64:
		return a.Value(idx)
	case *array.Timestamp:
		return a.Value(idx)
	default:
		return nil
	}
}

// compareValues compares two values with the given operator.
func compareValues(left, right any, op ExprOp) bool {
	if left == nil || right == nil {
		return false
	}

	// Convert to comparable types
	switch l := left.(type) {
	case int32:
		r := toInt64(right)
		return compareInt64(int64(l), r, op)
	case int64:
		r := toInt64(right)
		return compareInt64(l, r, op)
	case float32:
		r := toFloat64(right)
		return compareFloat64(float64(l), r, op)
	case float64:
		r := toFloat64(right)
		return compareFloat64(l, r, op)
	case string:
		r := toString(right)
		return compareString(l, r, op)
	case bool:
		r := toBool(right)
		return compareBool(l, r, op)
	default:
		return false
	}
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case int64:
		return x
	case float32:
		return int64(x)
	case float64:
		return int64(x)
	default:
		return 0
	}
}

func toFloat64(v any) float64 {
	switch x := v.(type) {
	case int:
		return float64(x)
	case int32:
		return float64(x)
	case int64:
		return float64(x)
	case float32:
		return float64(x)
	case float64:
		return x
	default:
		return 0
	}
}

func toString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	default:
		return fmt.Sprintf("%v", v)
	}
}

func toBool(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	default:
		return false
	}
}

func compareInt64(l, r int64, op ExprOp) bool {
	switch op {
	case OpEq:
		return l == r
	case OpNotEq:
		return l != r
	case OpLt:
		return l < r
	case OpLte:
		return l <= r
	case OpGt:
		return l > r
	case OpGte:
		return l >= r
	default:
		return false
	}
}

func compareFloat64(l, r float64, op ExprOp) bool {
	switch op {
	case OpEq:
		return l == r
	case OpNotEq:
		return l != r
	case OpLt:
		return l < r
	case OpLte:
		return l <= r
	case OpGt:
		return l > r
	case OpGte:
		return l >= r
	default:
		return false
	}
}

func compareString(l, r string, op ExprOp) bool {
	switch op {
	case OpEq:
		return l == r
	case OpNotEq:
		return l != r
	case OpLt:
		return l < r
	case OpLte:
		return l <= r
	case OpGt:
		return l > r
	case OpGte:
		return l >= r
	default:
		return false
	}
}

func compareBool(l, r bool, op ExprOp) bool {
	switch op {
	case OpEq:
		return l == r
	case OpNotEq:
		return l != r
	default:
		return false
	}
}

// appendValue appends a value from an array to a builder.
func appendValue(builder array.Builder, arr arrow.Array, idx int) {
	if arr.IsNull(idx) {
		builder.AppendNull()
		return
	}

	switch b := builder.(type) {
	case *array.Int32Builder:
		b.Append(arr.(*array.Int32).Value(idx))
	case *array.Int64Builder:
		b.Append(arr.(*array.Int64).Value(idx))
	case *array.Float32Builder:
		b.Append(arr.(*array.Float32).Value(idx))
	case *array.Float64Builder:
		b.Append(arr.(*array.Float64).Value(idx))
	case *array.StringBuilder:
		b.Append(arr.(*array.String).Value(idx))
	case *array.BooleanBuilder:
		b.Append(arr.(*array.Boolean).Value(idx))
	case *array.BinaryBuilder:
		b.Append(arr.(*array.Binary).Value(idx))
	case *array.Date32Builder:
		b.Append(arr.(*array.Date32).Value(idx))
	case *array.Date64Builder:
		b.Append(arr.(*array.Date64).Value(idx))
	case *array.TimestampBuilder:
		b.Append(arr.(*array.Timestamp).Value(idx))
	default:
		builder.AppendNull()
	}
}

// readParquetFile reads a Parquet file into an Arrow table.
func (t *Table) readParquetFile(ctx context.Context, filePath string) (arrow.Table, error) {
	inputFile, err := t.fileIO.Open(ctx, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	reader, err := inputFile.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open reader: %w", err)
	}
	defer reader.Close()

	// Read file content into memory for Parquet (needs ReaderAt)
	buf := new(bytes.Buffer)
	if _, err := io.Copy(buf, reader); err != nil {
		return nil, fmt.Errorf("failed to read file content: %w", err)
	}

	// Create Parquet reader from bytes
	pqReader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	defer pqReader.Close()

	// Create Arrow reader
	mem := memory.NewGoAllocator()
	arrowReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{}, mem)
	if err != nil {
		return nil, fmt.Errorf("failed to create arrow reader: %w", err)
	}

	return arrowReader.ReadTable(ctx)
}
