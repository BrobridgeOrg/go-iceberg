// Package table provides table operations for Iceberg tables.
package table

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/BrobridgeOrg/go-iceberg/catalog"
	"github.com/BrobridgeOrg/go-iceberg/io"
	"github.com/BrobridgeOrg/go-iceberg/spec"
	"github.com/google/uuid"
)

// Table represents an Iceberg table.
type Table struct {
	identifier       catalog.TableIdentifier
	metadata         *spec.TableMetadata
	metadataLocation string
	cat              catalog.Catalog
	fileIO           io.FileIO
}

// NewTable creates a new Table wrapper.
func NewTable(
	identifier catalog.TableIdentifier,
	metadata *spec.TableMetadata,
	metadataLocation string,
	cat catalog.Catalog,
	fileIO io.FileIO,
) *Table {
	return &Table{
		identifier:       identifier,
		metadata:         metadata,
		metadataLocation: metadataLocation,
		cat:              cat,
		fileIO:           fileIO,
	}
}

// Identifier returns the table identifier.
func (t *Table) Identifier() catalog.TableIdentifier {
	return t.identifier
}

// Metadata returns the table metadata.
func (t *Table) Metadata() *spec.TableMetadata {
	return t.metadata
}

// MetadataLocation returns the metadata file location.
func (t *Table) MetadataLocation() string {
	return t.metadataLocation
}

// Location returns the table location.
func (t *Table) Location() string {
	return t.metadata.Location
}

// Schema returns the current schema.
func (t *Table) Schema() *spec.Schema {
	return t.metadata.CurrentSchema()
}

// PartitionSpec returns the default partition spec.
func (t *Table) PartitionSpec() *spec.PartitionSpec {
	return t.metadata.DefaultPartitionSpec()
}

// SortOrder returns the default sort order.
func (t *Table) SortOrder() *spec.SortOrder {
	return t.metadata.DefaultSortOrder()
}

// Properties returns the table properties.
func (t *Table) Properties() map[string]string {
	return t.metadata.Properties
}

// CurrentSnapshot returns the current snapshot.
func (t *Table) CurrentSnapshot() *spec.Snapshot {
	return t.metadata.CurrentSnapshot()
}

// Snapshots returns all snapshots.
func (t *Table) Snapshots() []spec.Snapshot {
	return t.metadata.Snapshots
}

// SnapshotByID returns a snapshot by ID.
func (t *Table) SnapshotByID(id int64) *spec.Snapshot {
	return t.metadata.SnapshotByID(id)
}

// SnapshotAt returns the snapshot that was current at the given timestamp.
func (t *Table) SnapshotAt(timestamp time.Time) (*spec.Snapshot, error) {
	snapshots := t.Snapshots()
	if len(snapshots) == 0 {
		return nil, fmt.Errorf("table has no snapshots")
	}

	ts := timestamp.UnixMilli()
	var result *spec.Snapshot

	for i := range snapshots {
		snap := &snapshots[i]
		if snap.TimestampMs <= ts {
			if result == nil || snap.TimestampMs > result.TimestampMs {
				result = snap
			}
		}
	}

	if result == nil {
		return nil, fmt.Errorf("no snapshot found at or before %v", timestamp)
	}

	return result, nil
}

// FileIO returns the file I/O handler.
func (t *Table) FileIO() io.FileIO {
	return t.fileIO
}

// Catalog returns the catalog.
func (t *Table) Catalog() catalog.Catalog {
	return t.cat
}

// Refresh refreshes the table metadata from the catalog.
func (t *Table) Refresh(ctx context.Context) error {
	newMeta, err := t.cat.LoadTable(ctx, t.identifier)
	if err != nil {
		return fmt.Errorf("failed to refresh table: %w", err)
	}
	t.metadata = newMeta
	return nil
}

// Scan creates a new scan builder for this table.
func (t *Table) Scan() *ScanBuilder {
	return NewScanBuilder(t)
}

// NewTransaction creates a new transaction for this table.
func (t *Table) NewTransaction() *Transaction {
	return NewTransaction(t)
}

// Transaction represents a pending set of changes to a table.
type Transaction struct {
	table           *Table
	updates         []catalog.TableUpdate
	requirements    []catalog.TableRequirement
	baseMeta        *spec.TableMetadata
	snapshotBuilder *SnapshotBuilder
}

// NewTransaction creates a new transaction.
func NewTransaction(table *Table) *Transaction {
	// Add requirement for current snapshot
	var reqs []catalog.TableRequirement
	if table.metadata.CurrentSnapshotID != nil {
		reqs = append(reqs, catalog.RequireAssertRefSnapshotID("main", *table.metadata.CurrentSnapshotID))
	}

	return &Transaction{
		table:        table,
		updates:      []catalog.TableUpdate{},
		requirements: reqs,
		baseMeta:     table.metadata,
	}
}

// AddSnapshot adds a snapshot to the transaction.
func (tx *Transaction) AddSnapshot(snapshot *spec.Snapshot) *Transaction {
	tx.updates = append(tx.updates, catalog.UpdateAddSnapshot(snapshot))
	tx.updates = append(tx.updates, catalog.UpdateSetSnapshotRef("main", snapshot.SnapshotID, "branch"))
	return tx
}

// SetProperties sets table properties.
func (tx *Transaction) SetProperties(props map[string]string) *Transaction {
	tx.updates = append(tx.updates, catalog.UpdateSetProperties(props))
	return tx
}

// RemoveProperties removes table properties.
func (tx *Transaction) RemoveProperties(keys []string) *Transaction {
	tx.updates = append(tx.updates, catalog.UpdateRemoveProperties(keys))
	return tx
}

// UpdateSchema updates the schema.
func (tx *Transaction) UpdateSchema(schema *spec.Schema) *Transaction {
	tx.updates = append(tx.updates, catalog.UpdateAddSchema(schema, schema.HighestFieldID()))
	tx.updates = append(tx.updates, catalog.UpdateSetCurrentSchema(schema.SchemaID))
	return tx
}

// AppendDataFile adds a data file to the transaction.
// This creates a new snapshot with the appended file.
func (tx *Transaction) AppendDataFile(file spec.DataFile) *Transaction {
	if tx.snapshotBuilder == nil {
		tx.snapshotBuilder = NewSnapshotBuilder(tx.table, spec.OpAppend)
	}
	tx.snapshotBuilder.AddDataFile(file)
	return tx
}

// DeleteDataFile marks a data file for deletion.
func (tx *Transaction) DeleteDataFile(file spec.DataFile) *Transaction {
	if tx.snapshotBuilder == nil {
		tx.snapshotBuilder = NewSnapshotBuilder(tx.table, spec.OpDelete)
	}
	tx.snapshotBuilder.DeleteDataFile(file.FilePath)
	return tx
}

// AppendDeleteFile adds a delete file (for MoR deletes).
func (tx *Transaction) AppendDeleteFile(file spec.DataFile) *Transaction {
	if tx.snapshotBuilder == nil {
		tx.snapshotBuilder = NewSnapshotBuilder(tx.table, spec.OpDelete)
	}
	tx.snapshotBuilder.AddDeleteFile(file)
	return tx
}

// Commit commits the transaction.
func (tx *Transaction) Commit(ctx context.Context) error {
	// Build snapshot if we have pending file operations
	if tx.snapshotBuilder != nil {
		snapshot, err := tx.snapshotBuilder.Build(ctx)
		if err != nil {
			return fmt.Errorf("failed to build snapshot: %w", err)
		}
		tx.AddSnapshot(snapshot)
	}

	if len(tx.updates) == 0 {
		return nil
	}

	newMeta, err := tx.table.cat.CommitTable(ctx, tx.table.identifier, tx.requirements, tx.updates)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	tx.table.metadata = newMeta
	return nil
}

// SnapshotBuilder helps build new snapshots.
type SnapshotBuilder struct {
	table            *Table
	operation        spec.Operation
	parentID         *int64
	addedFiles       []spec.DataFile
	deletedFiles     []string
	addedDeleteFiles []spec.DataFile
}

// NewSnapshotBuilder creates a new snapshot builder.
func NewSnapshotBuilder(table *Table, operation spec.Operation) *SnapshotBuilder {
	var parentID *int64
	if table.metadata.CurrentSnapshotID != nil {
		parentID = table.metadata.CurrentSnapshotID
	}

	return &SnapshotBuilder{
		table:     table,
		operation: operation,
		parentID:  parentID,
	}
}

// AddDataFile adds a data file to the snapshot.
func (b *SnapshotBuilder) AddDataFile(file spec.DataFile) *SnapshotBuilder {
	b.addedFiles = append(b.addedFiles, file)
	return b
}

// DeleteDataFile marks a data file for deletion.
func (b *SnapshotBuilder) DeleteDataFile(path string) *SnapshotBuilder {
	b.deletedFiles = append(b.deletedFiles, path)
	return b
}

// AddDeleteFile adds a delete file to the snapshot.
func (b *SnapshotBuilder) AddDeleteFile(file spec.DataFile) *SnapshotBuilder {
	b.addedDeleteFiles = append(b.addedDeleteFiles, file)
	return b
}

// Build builds the snapshot and writes manifest files.
func (b *SnapshotBuilder) Build(ctx context.Context) (*spec.Snapshot, error) {
	// Generate snapshot ID
	snapshotID := time.Now().UnixNano()

	// Determine sequence number
	var seqNum int64 = 1
	if b.parentID != nil {
		if parent := b.table.metadata.SnapshotByID(*b.parentID); parent != nil {
			seqNum = parent.SequenceNumber + 1
		}
	}

	// Get current schema and partition spec IDs
	schemaID := b.table.metadata.CurrentSchemaID
	specID := b.table.metadata.DefaultSpecID
	partitionSpec := b.table.metadata.DefaultPartitionSpec()

	// Build summary
	summary := &spec.Summary{
		Operation:             b.operation,
		AddedDataFilesCount:   int64(len(b.addedFiles)),
		AddedDeleteFilesCount: int64(len(b.addedDeleteFiles)),
	}

	// Calculate total records
	for _, f := range b.addedFiles {
		summary.AddedRecordsCount += f.RecordCount
		summary.AddedFileSizeInBytes += f.FileSizeInBytes
	}

	// Write data manifest if there are added files
	var manifestListPath string
	var manifests []spec.ManifestFile

	if len(b.addedFiles) > 0 || len(b.addedDeleteFiles) > 0 {
		// Write data manifest
		if len(b.addedFiles) > 0 {
			manifestPath := path.Join(
				b.table.metadata.Location,
				"metadata",
				fmt.Sprintf("manifest-%s-%d.avro", uuid.New().String()[:8], snapshotID),
			)

			writer, err := spec.NewManifestWriter(schemaID, specID, spec.ManifestContentData, seqNum, partitionSpec)
			if err != nil {
				return nil, fmt.Errorf("failed to create manifest writer: %w", err)
			}

			for _, file := range b.addedFiles {
				entry := spec.ManifestEntry{
					Status:         spec.EntryStatusAdded,
					SnapshotID:     &snapshotID,
					SequenceNumber: &seqNum,
					DataFile:       file,
				}
				if err := writer.Append(entry); err != nil {
					return nil, fmt.Errorf("failed to write manifest entry: %w", err)
				}
			}

			// Write to storage
			output, err := b.table.fileIO.Create(ctx, manifestPath)
			if err != nil {
				return nil, fmt.Errorf("failed to create manifest file: %w", err)
			}
			w, err := output.CreateOverwrite(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to create manifest writer: %w", err)
			}
			data := writer.Bytes()
			if _, err := w.Write(data); err != nil {
				w.Close()
				return nil, fmt.Errorf("failed to write manifest: %w", err)
			}
			w.Close()

			manifests = append(manifests, spec.ManifestFile{
				ManifestPath:      manifestPath,
				ManifestLength:    int64(len(data)),
				PartitionSpecID:   specID,
				Content:           spec.ManifestContentData,
				SequenceNumber:    seqNum,
				MinSequenceNumber: seqNum,
				AddedSnapshotID:   snapshotID,
				AddedFilesCount:   len(b.addedFiles),
				AddedRowsCount:    summary.AddedRecordsCount,
			})
		}

		// Write delete manifest if there are delete files
		if len(b.addedDeleteFiles) > 0 {
			manifestPath := path.Join(
				b.table.metadata.Location,
				"metadata",
				fmt.Sprintf("delete-manifest-%s-%d.avro", uuid.New().String()[:8], snapshotID),
			)

			writer, err := spec.NewManifestWriter(schemaID, specID, spec.ManifestContentDelete, seqNum, partitionSpec)
			if err != nil {
				return nil, fmt.Errorf("failed to create delete manifest writer: %w", err)
			}

			for _, file := range b.addedDeleteFiles {
				entry := spec.ManifestEntry{
					Status:         spec.EntryStatusAdded,
					SnapshotID:     &snapshotID,
					SequenceNumber: &seqNum,
					DataFile:       file,
				}
				if err := writer.Append(entry); err != nil {
					return nil, fmt.Errorf("failed to write delete manifest entry: %w", err)
				}
			}

			// Write to storage
			output, err := b.table.fileIO.Create(ctx, manifestPath)
			if err != nil {
				return nil, fmt.Errorf("failed to create delete manifest file: %w", err)
			}
			w, err := output.CreateOverwrite(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to create delete manifest writer: %w", err)
			}
			data := writer.Bytes()
			if _, err := w.Write(data); err != nil {
				w.Close()
				return nil, fmt.Errorf("failed to write delete manifest: %w", err)
			}
			w.Close()

			manifests = append(manifests, spec.ManifestFile{
				ManifestPath:      manifestPath,
				ManifestLength:    int64(len(data)),
				PartitionSpecID:   specID,
				Content:           spec.ManifestContentDelete,
				SequenceNumber:    seqNum,
				MinSequenceNumber: seqNum,
				AddedSnapshotID:   snapshotID,
				AddedFilesCount:   len(b.addedDeleteFiles),
			})
		}

		// Write manifest list
		manifestListPath = path.Join(
			b.table.metadata.Location,
			"metadata",
			fmt.Sprintf("snap-%d-%s.avro", snapshotID, uuid.New().String()[:8]),
		)

		listWriter, err := spec.NewManifestListWriter()
		if err != nil {
			return nil, fmt.Errorf("failed to create manifest list writer: %w", err)
		}

		for _, mf := range manifests {
			if err := listWriter.Append(mf); err != nil {
				return nil, fmt.Errorf("failed to write manifest list entry: %w", err)
			}
		}

		// Write manifest list to storage
		output, err := b.table.fileIO.Create(ctx, manifestListPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create manifest list file: %w", err)
		}
		w, err := output.CreateOverwrite(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create manifest list writer: %w", err)
		}
		if _, err := w.Write(listWriter.Bytes()); err != nil {
			w.Close()
			return nil, fmt.Errorf("failed to write manifest list: %w", err)
		}
		w.Close()
	}

	snapshot := &spec.Snapshot{
		SnapshotID:       snapshotID,
		ParentSnapshotID: b.parentID,
		SequenceNumber:   seqNum,
		TimestampMs:      time.Now().UnixMilli(),
		ManifestList:     manifestListPath,
		Summary:          summary,
		SchemaID:         &schemaID,
	}

	return snapshot, nil
}

// Commit builds and commits the snapshot.
func (b *SnapshotBuilder) Commit(ctx context.Context) (*spec.Snapshot, error) {
	snapshot, err := b.Build(ctx)
	if err != nil {
		return nil, err
	}

	tx := b.table.NewTransaction()
	tx.AddSnapshot(snapshot)

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	return snapshot, nil
}

// History returns the snapshot history of the table.
func (t *Table) History() []spec.SnapshotLog {
	return t.metadata.SnapshotLog
}
