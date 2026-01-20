package spec

import (
	"encoding/json"
	"fmt"
)

// FormatVersion represents the Iceberg format version.
type FormatVersion int

const (
	FormatVersionV1 FormatVersion = 1
	FormatVersionV2 FormatVersion = 2
)

// SortDirection represents the sort direction.
type SortDirection string

const (
	SortAsc  SortDirection = "asc"
	SortDesc SortDirection = "desc"
)

// NullOrder represents the null ordering.
type NullOrder string

const (
	NullsFirst NullOrder = "nulls-first"
	NullsLast  NullOrder = "nulls-last"
)

// SortField represents a field in a sort order.
type SortField struct {
	Transform   string        `json:"transform"`
	SourceID    int           `json:"source-id"`
	Direction   SortDirection `json:"direction"`
	NullOrder   NullOrder     `json:"null-order"`
}

// SortOrder represents a sort order for a table.
type SortOrder struct {
	OrderID int         `json:"order-id"`
	Fields  []SortField `json:"fields"`
}

// UnsortedOrder returns an empty (unsorted) sort order.
func UnsortedOrder() SortOrder {
	return SortOrder{
		OrderID: 0,
		Fields:  []SortField{},
	}
}

// TableMetadata represents the metadata of an Iceberg table.
type TableMetadata struct {
	FormatVersion      FormatVersion            `json:"format-version"`
	TableUUID          string                   `json:"table-uuid"`
	Location           string                   `json:"location"`
	LastUpdatedMs      int64                    `json:"last-updated-ms"`
	LastColumnID       int                      `json:"last-column-id"`
	Schemas            []*Schema                `json:"schemas"`
	CurrentSchemaID    int                      `json:"current-schema-id"`
	PartitionSpecs     []PartitionSpec          `json:"partition-specs"`
	DefaultSpecID      int                      `json:"default-spec-id"`
	LastPartitionID    int                      `json:"last-partition-id"`
	Properties         map[string]string        `json:"properties,omitempty"`
	CurrentSnapshotID  *int64                   `json:"current-snapshot-id,omitempty"`
	Snapshots          []Snapshot               `json:"snapshots,omitempty"`
	SnapshotLog        []SnapshotLog            `json:"snapshot-log,omitempty"`
	MetadataLog        []MetadataLogEntry       `json:"metadata-log,omitempty"`
	SortOrders         []SortOrder              `json:"sort-orders"`
	DefaultSortOrderID int                      `json:"default-sort-order-id"`
	Refs               map[string]SnapshotRef   `json:"refs,omitempty"`

	// V1 compatibility fields (deprecated in V2)
	Schema        *Schema        `json:"schema,omitempty"`         // V1 only
	PartitionSpec []PartitionField `json:"partition-spec,omitempty"` // V1 only
}

// MetadataLogEntry represents an entry in the metadata log.
type MetadataLogEntry struct {
	TimestampMs      int64  `json:"timestamp-ms"`
	MetadataFile     string `json:"metadata-file"`
}

// CurrentSchema returns the current schema.
func (m *TableMetadata) CurrentSchema() *Schema {
	for _, s := range m.Schemas {
		if s.SchemaID == m.CurrentSchemaID {
			return s
		}
	}
	// Fallback to V1 schema
	if m.Schema != nil {
		return m.Schema
	}
	return nil
}

// DefaultPartitionSpec returns the default partition spec.
func (m *TableMetadata) DefaultPartitionSpec() *PartitionSpec {
	for i := range m.PartitionSpecs {
		if m.PartitionSpecs[i].SpecID == m.DefaultSpecID {
			return &m.PartitionSpecs[i]
		}
	}
	return nil
}

// CurrentSnapshot returns the current snapshot.
func (m *TableMetadata) CurrentSnapshot() *Snapshot {
	if m.CurrentSnapshotID == nil {
		return nil
	}
	for i := range m.Snapshots {
		if m.Snapshots[i].SnapshotID == *m.CurrentSnapshotID {
			return &m.Snapshots[i]
		}
	}
	return nil
}

// SnapshotByID returns a snapshot by its ID.
func (m *TableMetadata) SnapshotByID(id int64) *Snapshot {
	for i := range m.Snapshots {
		if m.Snapshots[i].SnapshotID == id {
			return &m.Snapshots[i]
		}
	}
	return nil
}

// SchemaByID returns a schema by its ID.
func (m *TableMetadata) SchemaByID(id int) *Schema {
	for _, s := range m.Schemas {
		if s.SchemaID == id {
			return s
		}
	}
	return nil
}

// DefaultSortOrder returns the default sort order.
func (m *TableMetadata) DefaultSortOrder() *SortOrder {
	for i := range m.SortOrders {
		if m.SortOrders[i].OrderID == m.DefaultSortOrderID {
			return &m.SortOrders[i]
		}
	}
	return nil
}

// ParseTableMetadata parses table metadata from JSON.
func ParseTableMetadata(data []byte) (*TableMetadata, error) {
	var meta TableMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("failed to parse table metadata: %w", err)
	}

	// Handle V1 to V2 migration
	if meta.FormatVersion == FormatVersionV1 {
		// Migrate single schema to schemas list
		if meta.Schema != nil && len(meta.Schemas) == 0 {
			meta.Schemas = []*Schema{meta.Schema}
			meta.CurrentSchemaID = meta.Schema.SchemaID
		}

		// Migrate partition spec
		if len(meta.PartitionSpec) > 0 && len(meta.PartitionSpecs) == 0 {
			meta.PartitionSpecs = []PartitionSpec{
				{
					SpecID: 0,
					Fields: meta.PartitionSpec,
				},
			}
			meta.DefaultSpecID = 0
		}

		// Ensure sort orders exist
		if len(meta.SortOrders) == 0 {
			meta.SortOrders = []SortOrder{UnsortedOrder()}
			meta.DefaultSortOrderID = 0
		}
	}

	return &meta, nil
}

// ToJSON serializes the metadata to JSON.
func (m *TableMetadata) ToJSON() ([]byte, error) {
	return json.MarshalIndent(m, "", "  ")
}

// NewTableMetadataV2 creates a new V2 table metadata.
func NewTableMetadataV2(
	tableUUID string,
	location string,
	schema *Schema,
	partitionSpec *PartitionSpec,
	properties map[string]string,
) *TableMetadata {
	meta := &TableMetadata{
		FormatVersion:      FormatVersionV2,
		TableUUID:          tableUUID,
		Location:           location,
		LastColumnID:       schema.HighestFieldID(),
		Schemas:            []*Schema{schema},
		CurrentSchemaID:    schema.SchemaID,
		PartitionSpecs:     []PartitionSpec{},
		DefaultSpecID:      0,
		LastPartitionID:    0,
		Properties:         properties,
		Snapshots:          []Snapshot{},
		SnapshotLog:        []SnapshotLog{},
		MetadataLog:        []MetadataLogEntry{},
		SortOrders:         []SortOrder{UnsortedOrder()},
		DefaultSortOrderID: 0,
		Refs:               make(map[string]SnapshotRef),
	}

	if partitionSpec != nil {
		meta.PartitionSpecs = []PartitionSpec{*partitionSpec}
		meta.DefaultSpecID = partitionSpec.SpecID
		meta.LastPartitionID = partitionSpec.LastFieldID()
	} else {
		// Empty partition spec (unpartitioned)
		meta.PartitionSpecs = []PartitionSpec{
			{SpecID: 0, Fields: []PartitionField{}},
		}
	}

	return meta
}

// MetadataBuilder helps build table metadata incrementally.
type MetadataBuilder struct {
	meta *TableMetadata
}

// NewMetadataBuilder creates a new metadata builder from existing metadata.
func NewMetadataBuilder(base *TableMetadata) *MetadataBuilder {
	// Deep copy the base metadata
	copied := *base
	copied.Schemas = make([]*Schema, len(base.Schemas))
	copy(copied.Schemas, base.Schemas)
	copied.PartitionSpecs = make([]PartitionSpec, len(base.PartitionSpecs))
	copy(copied.PartitionSpecs, base.PartitionSpecs)
	copied.Snapshots = make([]Snapshot, len(base.Snapshots))
	copy(copied.Snapshots, base.Snapshots)
	copied.SortOrders = make([]SortOrder, len(base.SortOrders))
	copy(copied.SortOrders, base.SortOrders)
	copied.SnapshotLog = make([]SnapshotLog, len(base.SnapshotLog))
	copy(copied.SnapshotLog, base.SnapshotLog)
	copied.MetadataLog = make([]MetadataLogEntry, len(base.MetadataLog))
	copy(copied.MetadataLog, base.MetadataLog)

	if base.Properties != nil {
		copied.Properties = make(map[string]string)
		for k, v := range base.Properties {
			copied.Properties[k] = v
		}
	}

	if base.Refs != nil {
		copied.Refs = make(map[string]SnapshotRef)
		for k, v := range base.Refs {
			copied.Refs[k] = v
		}
	}

	return &MetadataBuilder{meta: &copied}
}

// SetCurrentSnapshotID sets the current snapshot ID.
func (b *MetadataBuilder) SetCurrentSnapshotID(id int64) *MetadataBuilder {
	b.meta.CurrentSnapshotID = &id
	return b
}

// AddSnapshot adds a snapshot to the metadata.
func (b *MetadataBuilder) AddSnapshot(snap Snapshot) *MetadataBuilder {
	b.meta.Snapshots = append(b.meta.Snapshots, snap)
	return b
}

// AddSnapshotLog adds a snapshot log entry.
func (b *MetadataBuilder) AddSnapshotLog(entry SnapshotLog) *MetadataBuilder {
	b.meta.SnapshotLog = append(b.meta.SnapshotLog, entry)
	return b
}

// SetRef sets a snapshot reference (branch or tag).
func (b *MetadataBuilder) SetRef(name string, ref SnapshotRef) *MetadataBuilder {
	if b.meta.Refs == nil {
		b.meta.Refs = make(map[string]SnapshotRef)
	}
	b.meta.Refs[name] = ref
	return b
}

// RemoveRef removes a snapshot reference.
func (b *MetadataBuilder) RemoveRef(name string) *MetadataBuilder {
	delete(b.meta.Refs, name)
	return b
}

// AddSchema adds a new schema and optionally sets it as current.
func (b *MetadataBuilder) AddSchema(schema *Schema, setCurrent bool) *MetadataBuilder {
	// Assign schema ID if not set
	if schema.SchemaID == 0 {
		maxID := 0
		for _, s := range b.meta.Schemas {
			if s.SchemaID > maxID {
				maxID = s.SchemaID
			}
		}
		schema.SchemaID = maxID + 1
	}

	b.meta.Schemas = append(b.meta.Schemas, schema)

	// Update last column ID
	highest := schema.HighestFieldID()
	if highest > b.meta.LastColumnID {
		b.meta.LastColumnID = highest
	}

	if setCurrent {
		b.meta.CurrentSchemaID = schema.SchemaID
	}

	return b
}

// SetProperty sets a table property.
func (b *MetadataBuilder) SetProperty(key, value string) *MetadataBuilder {
	if b.meta.Properties == nil {
		b.meta.Properties = make(map[string]string)
	}
	b.meta.Properties[key] = value
	return b
}

// RemoveProperty removes a table property.
func (b *MetadataBuilder) RemoveProperty(key string) *MetadataBuilder {
	delete(b.meta.Properties, key)
	return b
}

// SetLastUpdatedMs sets the last updated timestamp.
func (b *MetadataBuilder) SetLastUpdatedMs(ts int64) *MetadataBuilder {
	b.meta.LastUpdatedMs = ts
	return b
}

// RemoveSnapshots removes snapshots by their IDs.
func (b *MetadataBuilder) RemoveSnapshots(ids []int64) *MetadataBuilder {
	idSet := make(map[int64]bool)
	for _, id := range ids {
		idSet[id] = true
	}

	filtered := make([]Snapshot, 0, len(b.meta.Snapshots))
	for _, snap := range b.meta.Snapshots {
		if !idSet[snap.SnapshotID] {
			filtered = append(filtered, snap)
		}
	}
	b.meta.Snapshots = filtered

	return b
}

// Build returns the built metadata.
func (b *MetadataBuilder) Build() *TableMetadata {
	return b.meta
}
