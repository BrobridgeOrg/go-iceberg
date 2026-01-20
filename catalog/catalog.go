// Package catalog provides catalog interfaces and implementations for Iceberg.
package catalog

import (
	"context"

	"github.com/go-iceberg/go-iceberg/spec"
)

// Catalog is the interface for Iceberg catalog operations.
type Catalog interface {
	// Name returns the catalog name.
	Name() string

	// ListNamespaces lists all namespaces in the catalog.
	ListNamespaces(ctx context.Context, parent Namespace) ([]Namespace, error)

	// CreateNamespace creates a new namespace.
	CreateNamespace(ctx context.Context, namespace Namespace, properties map[string]string) error

	// DropNamespace drops a namespace.
	DropNamespace(ctx context.Context, namespace Namespace) error

	// NamespaceExists checks if a namespace exists.
	NamespaceExists(ctx context.Context, namespace Namespace) (bool, error)

	// LoadNamespaceProperties loads namespace properties.
	LoadNamespaceProperties(ctx context.Context, namespace Namespace) (map[string]string, error)

	// UpdateNamespaceProperties updates namespace properties.
	UpdateNamespaceProperties(ctx context.Context, namespace Namespace, removals []string, updates map[string]string) error

	// ListTables lists all tables in a namespace.
	ListTables(ctx context.Context, namespace Namespace) ([]TableIdentifier, error)

	// CreateTable creates a new table.
	CreateTable(ctx context.Context, identifier TableIdentifier, schema *spec.Schema, opts ...CreateTableOption) (*spec.TableMetadata, error)

	// LoadTable loads a table's metadata.
	LoadTable(ctx context.Context, identifier TableIdentifier) (*spec.TableMetadata, error)

	// TableExists checks if a table exists.
	TableExists(ctx context.Context, identifier TableIdentifier) (bool, error)

	// DropTable drops a table.
	DropTable(ctx context.Context, identifier TableIdentifier, purge bool) error

	// RenameTable renames a table.
	RenameTable(ctx context.Context, from, to TableIdentifier) error

	// CommitTable commits changes to a table.
	CommitTable(ctx context.Context, identifier TableIdentifier, requirements []TableRequirement, updates []TableUpdate) (*spec.TableMetadata, error)
}

// Namespace represents an Iceberg namespace (database).
type Namespace []string

// String returns the namespace as a dot-separated string.
func (n Namespace) String() string {
	if len(n) == 0 {
		return ""
	}
	result := n[0]
	for i := 1; i < len(n); i++ {
		result += "." + n[i]
	}
	return result
}

// TableIdentifier represents a fully qualified table identifier.
type TableIdentifier struct {
	Namespace Namespace
	Name      string
}

// String returns the table identifier as a dot-separated string.
func (t TableIdentifier) String() string {
	if len(t.Namespace) == 0 {
		return t.Name
	}
	return t.Namespace.String() + "." + t.Name
}

// CreateTableOption configures table creation.
type CreateTableOption func(*CreateTableConfig)

// CreateTableConfig holds table creation configuration.
type CreateTableConfig struct {
	PartitionSpec  *spec.PartitionSpec
	SortOrder      *spec.SortOrder
	Location       string
	Properties     map[string]string
	StageCreate    bool
}

// WithPartitionSpec sets the partition spec for table creation.
func WithPartitionSpec(spec *spec.PartitionSpec) CreateTableOption {
	return func(c *CreateTableConfig) {
		c.PartitionSpec = spec
	}
}

// WithSortOrder sets the sort order for table creation.
func WithSortOrder(order *spec.SortOrder) CreateTableOption {
	return func(c *CreateTableConfig) {
		c.SortOrder = order
	}
}

// WithLocation sets the location for table creation.
func WithLocation(location string) CreateTableOption {
	return func(c *CreateTableConfig) {
		c.Location = location
	}
}

// WithProperties sets properties for table creation.
func WithProperties(props map[string]string) CreateTableOption {
	return func(c *CreateTableConfig) {
		c.Properties = props
	}
}

// WithStageCreate enables staged table creation.
func WithStageCreate(stage bool) CreateTableOption {
	return func(c *CreateTableConfig) {
		c.StageCreate = stage
	}
}

// TableRequirement represents a requirement that must be met before committing changes.
type TableRequirement struct {
	Type                  string  `json:"type"`
	Ref                   *string `json:"ref,omitempty"`
	UUID                  *string `json:"uuid,omitempty"`
	SnapshotID            *int64  `json:"snapshot-id,omitempty"`
	LastAssignedFieldID   *int    `json:"last-assigned-field-id,omitempty"`
	CurrentSchemaID       *int    `json:"current-schema-id,omitempty"`
	LastAssignedPartitionID *int  `json:"last-assigned-partition-id,omitempty"`
	DefaultSpecID         *int    `json:"default-spec-id,omitempty"`
	DefaultSortOrderID    *int    `json:"default-sort-order-id,omitempty"`
}

// TableUpdate represents an update to apply to a table.
type TableUpdate struct {
	Action              string              `json:"action"`
	Format              *int                `json:"format-version,omitempty"`
	Schema              *spec.Schema        `json:"schema,omitempty"`
	SchemaID            *int                `json:"schema-id,omitempty"`
	LastAssignedFieldID *int                `json:"last-assigned-field-id,omitempty"`
	Spec                *spec.PartitionSpec `json:"spec,omitempty"`
	SpecID              *int                `json:"spec-id,omitempty"`
	SortOrder           *spec.SortOrder     `json:"sort-order,omitempty"`
	SortOrderID         *int                `json:"sort-order-id,omitempty"`
	Snapshot            *spec.Snapshot      `json:"snapshot,omitempty"`
	RefName             *string             `json:"ref-name,omitempty"`
	Type                *string             `json:"type,omitempty"`
	SnapshotID          *int64              `json:"snapshot-id,omitempty"`
	MaxRefAgeMs         *int64              `json:"max-ref-age-ms,omitempty"`
	MaxSnapshotAgeMs    *int64              `json:"max-snapshot-age-ms,omitempty"`
	MinSnapshotsToKeep  *int                `json:"min-snapshots-to-keep,omitempty"`
	SnapshotIDs         []int64             `json:"snapshot-ids,omitempty"`
	Location            *string             `json:"location,omitempty"`
	Removals            []string            `json:"removals,omitempty"`
	Updates             map[string]string   `json:"updates,omitempty"`
}

// Requirement constructors

// RequireAssertCreate requires that the table does not exist.
func RequireAssertCreate() TableRequirement {
	return TableRequirement{Type: "assert-create"}
}

// RequireAssertTableUUID requires a specific table UUID.
func RequireAssertTableUUID(uuid string) TableRequirement {
	return TableRequirement{Type: "assert-table-uuid", UUID: &uuid}
}

// RequireAssertRefSnapshotID requires a specific snapshot ID for a ref.
func RequireAssertRefSnapshotID(ref string, snapshotID int64) TableRequirement {
	return TableRequirement{Type: "assert-ref-snapshot-id", Ref: &ref, SnapshotID: &snapshotID}
}

// RequireAssertLastAssignedFieldID requires a specific last assigned field ID.
func RequireAssertLastAssignedFieldID(id int) TableRequirement {
	return TableRequirement{Type: "assert-last-assigned-field-id", LastAssignedFieldID: &id}
}

// RequireAssertCurrentSchemaID requires a specific current schema ID.
func RequireAssertCurrentSchemaID(id int) TableRequirement {
	return TableRequirement{Type: "assert-current-schema-id", CurrentSchemaID: &id}
}

// RequireAssertDefaultSpecID requires a specific default spec ID.
func RequireAssertDefaultSpecID(id int) TableRequirement {
	return TableRequirement{Type: "assert-default-spec-id", DefaultSpecID: &id}
}

// RequireAssertDefaultSortOrderID requires a specific default sort order ID.
func RequireAssertDefaultSortOrderID(id int) TableRequirement {
	return TableRequirement{Type: "assert-default-sort-order-id", DefaultSortOrderID: &id}
}

// Update constructors

// UpdateAssignUUID assigns a UUID to the table.
func UpdateAssignUUID(uuid string) TableUpdate {
	return TableUpdate{Action: "assign-uuid", Location: &uuid}
}

// UpdateUpgradeFormatVersion upgrades the format version.
func UpdateUpgradeFormatVersion(version int) TableUpdate {
	return TableUpdate{Action: "upgrade-format-version", Format: &version}
}

// UpdateAddSchema adds a new schema.
func UpdateAddSchema(schema *spec.Schema, lastColumnID int) TableUpdate {
	return TableUpdate{Action: "add-schema", Schema: schema, LastAssignedFieldID: &lastColumnID}
}

// UpdateSetCurrentSchema sets the current schema.
func UpdateSetCurrentSchema(schemaID int) TableUpdate {
	return TableUpdate{Action: "set-current-schema", SchemaID: &schemaID}
}

// UpdateAddPartitionSpec adds a partition spec.
func UpdateAddPartitionSpec(spec *spec.PartitionSpec) TableUpdate {
	return TableUpdate{Action: "add-spec", Spec: spec}
}

// UpdateSetDefaultSpec sets the default partition spec.
func UpdateSetDefaultSpec(specID int) TableUpdate {
	return TableUpdate{Action: "set-default-spec", SpecID: &specID}
}

// UpdateAddSortOrder adds a sort order.
func UpdateAddSortOrder(order *spec.SortOrder) TableUpdate {
	return TableUpdate{Action: "add-sort-order", SortOrder: order}
}

// UpdateSetDefaultSortOrder sets the default sort order.
func UpdateSetDefaultSortOrder(sortOrderID int) TableUpdate {
	return TableUpdate{Action: "set-default-sort-order", SortOrderID: &sortOrderID}
}

// UpdateAddSnapshot adds a snapshot.
func UpdateAddSnapshot(snapshot *spec.Snapshot) TableUpdate {
	return TableUpdate{Action: "add-snapshot", Snapshot: snapshot}
}

// UpdateSetSnapshotRef sets a snapshot reference.
func UpdateSetSnapshotRef(refName string, snapshotID int64, refType string) TableUpdate {
	return TableUpdate{
		Action:     "set-snapshot-ref",
		RefName:    &refName,
		SnapshotID: &snapshotID,
		Type:       &refType,
	}
}

// UpdateRemoveSnapshots removes snapshots.
func UpdateRemoveSnapshots(snapshotIDs []int64) TableUpdate {
	return TableUpdate{Action: "remove-snapshots", SnapshotIDs: snapshotIDs}
}

// UpdateSetLocation sets the table location.
func UpdateSetLocation(location string) TableUpdate {
	return TableUpdate{Action: "set-location", Location: &location}
}

// UpdateSetProperties sets table properties.
func UpdateSetProperties(updates map[string]string) TableUpdate {
	return TableUpdate{Action: "set-properties", Updates: updates}
}

// UpdateRemoveProperties removes table properties.
func UpdateRemoveProperties(removals []string) TableUpdate {
	return TableUpdate{Action: "remove-properties", Removals: removals}
}
