// Package goiceberg provides a simple, idiomatic Go client for Apache Iceberg tables.
package goiceberg

import (
	"context"
	"fmt"
	"strings"

	"github.com/BrobridgeOrg/go-iceberg/catalog"
	"github.com/BrobridgeOrg/go-iceberg/io"
	"github.com/BrobridgeOrg/go-iceberg/spec"
	"github.com/BrobridgeOrg/go-iceberg/table"
)

// Client is the main entry point for go-iceberg operations.
type Client struct {
	catalog catalog.Catalog
	config  *Config
	io      io.FileIO
}

// NewClient creates a new go-iceberg client with the given configuration.
func NewClient(ctx context.Context, opts ...Option) (*Client, error) {
	config := DefaultConfig()
	for _, opt := range opts {
		opt(config)
	}

	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	cat, err := createCatalog(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create catalog: %w", err)
	}

	fileIO, err := createFileIO(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create file IO: %w", err)
	}

	return &Client{
		catalog: cat,
		config:  config,
		io:      fileIO,
	}, nil
}

// validateConfig validates the client configuration.
func validateConfig(config *Config) error {
	if config.CatalogType == CatalogREST && config.CatalogURI == "" {
		return fmt.Errorf("%w: REST catalog requires CatalogURI", ErrInvalidConfig)
	}
	return nil
}

// createCatalog creates a catalog based on the configuration.
func createCatalog(ctx context.Context, config *Config) (catalog.Catalog, error) {
	switch config.CatalogType {
	case CatalogREST:
		opts := []catalog.RESTCatalogOption{
			catalog.WithWarehouse(config.Warehouse),
		}
		if config.Token != "" {
			opts = append(opts, catalog.WithToken(config.Token))
		}
		if config.Credential != "" {
			opts = append(opts, catalog.WithCredential(config.Credential))
		}
		return catalog.NewRESTCatalog(config.CatalogURI, opts...)

	default:
		return nil, fmt.Errorf("%w: unsupported catalog type: %s", ErrInvalidConfig, config.CatalogType)
	}
}

// createFileIO creates a file IO based on the configuration.
func createFileIO(ctx context.Context, config *Config) (io.FileIO, error) {
	switch config.StorageType {
	case StorageS3:
		if config.S3Config == nil {
			config.S3Config = &S3Config{}
		}
		return io.NewS3FileIO(ctx, &io.S3Config{
			Region:          config.S3Config.Region,
			Endpoint:        config.S3Config.Endpoint,
			AccessKeyID:     config.S3Config.AccessKeyID,
			SecretAccessKey: config.S3Config.SecretAccessKey,
			SessionToken:    config.S3Config.SessionToken,
			ForcePathStyle:  config.S3Config.ForcePathStyle,
		})
	case StorageLocal:
		return io.NewLocalFileIO(), nil
	default:
		// Default to local if not specified
		return io.NewLocalFileIO(), nil
	}
}

// Config returns the client configuration.
func (c *Client) Config() *Config {
	return c.config
}

// Catalog returns the underlying catalog for advanced operations.
func (c *Client) Catalog() catalog.Catalog {
	return c.catalog
}

// FileIO returns the file I/O handler.
func (c *Client) FileIO() io.FileIO {
	return c.io
}

// Table opens an existing table.
func (c *Client) Table(ctx context.Context, namespace, name string) (*table.Table, error) {
	identifier := catalog.TableIdentifier{
		Namespace: catalog.Namespace(strings.Split(namespace, ".")),
		Name:      name,
	}

	meta, err := c.catalog.LoadTable(ctx, identifier)
	if err != nil {
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "NoSuchTableException") {
			return nil, &TableNotFoundError{Namespace: namespace, TableName: name}
		}
		return nil, fmt.Errorf("failed to load table: %w", err)
	}

	return table.NewTable(identifier, meta, "", c.catalog, c.io), nil
}

// CreateTable creates a new table.
func (c *Client) CreateTable(ctx context.Context, namespace, name string, schema *spec.Schema, opts ...CreateTableOption) (*table.Table, error) {
	cfg := &CreateTableConfig{
		Properties: make(map[string]string),
	}
	for _, opt := range opts {
		opt(cfg)
	}

	identifier := catalog.TableIdentifier{
		Namespace: catalog.Namespace(strings.Split(namespace, ".")),
		Name:      name,
	}

	// Build catalog options
	var catOpts []catalog.CreateTableOption
	if cfg.PartitionSpec != nil {
		catOpts = append(catOpts, catalog.WithPartitionSpec(cfg.PartitionSpec))
	}
	if cfg.SortOrder != nil {
		catOpts = append(catOpts, catalog.WithSortOrder(cfg.SortOrder))
	}
	if cfg.Location != "" {
		catOpts = append(catOpts, catalog.WithLocation(cfg.Location))
	}
	if len(cfg.Properties) > 0 {
		catOpts = append(catOpts, catalog.WithProperties(cfg.Properties))
	}

	meta, err := c.catalog.CreateTable(ctx, identifier, schema, catOpts...)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "TableAlreadyExistsException") {
			return nil, fmt.Errorf("%w: %s.%s", ErrTableAlreadyExists, namespace, name)
		}
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	return table.NewTable(identifier, meta, "", c.catalog, c.io), nil
}

// DropTable drops a table.
func (c *Client) DropTable(ctx context.Context, namespace, name string, purge bool) error {
	identifier := catalog.TableIdentifier{
		Namespace: catalog.Namespace(strings.Split(namespace, ".")),
		Name:      name,
	}

	err := c.catalog.DropTable(ctx, identifier, purge)
	if err != nil {
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "NoSuchTableException") {
			return &TableNotFoundError{Namespace: namespace, TableName: name}
		}
		return fmt.Errorf("failed to drop table: %w", err)
	}

	return nil
}

// TableExists checks if a table exists.
func (c *Client) TableExists(ctx context.Context, namespace, name string) (bool, error) {
	identifier := catalog.TableIdentifier{
		Namespace: catalog.Namespace(strings.Split(namespace, ".")),
		Name:      name,
	}

	return c.catalog.TableExists(ctx, identifier)
}

// ListTables lists all tables in a namespace.
func (c *Client) ListTables(ctx context.Context, namespace string) ([]string, error) {
	ns := catalog.Namespace(strings.Split(namespace, "."))

	tables, err := c.catalog.ListTables(ctx, ns)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}

	result := make([]string, len(tables))
	for i, t := range tables {
		result[i] = t.String()
	}

	return result, nil
}

// ListNamespaces lists all namespaces.
func (c *Client) ListNamespaces(ctx context.Context, parent string) ([]string, error) {
	var parentNs catalog.Namespace
	if parent != "" {
		parentNs = catalog.Namespace(strings.Split(parent, "."))
	}

	namespaces, err := c.catalog.ListNamespaces(ctx, parentNs)
	if err != nil {
		return nil, fmt.Errorf("failed to list namespaces: %w", err)
	}

	result := make([]string, len(namespaces))
	for i, ns := range namespaces {
		result[i] = ns.String()
	}

	return result, nil
}

// CreateNamespace creates a new namespace.
func (c *Client) CreateNamespace(ctx context.Context, namespace string, props map[string]string) error {
	ns := catalog.Namespace(strings.Split(namespace, "."))

	err := c.catalog.CreateNamespace(ctx, ns, props)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "NamespaceAlreadyExistsException") {
			return fmt.Errorf("%w: %s", ErrNamespaceAlreadyExists, namespace)
		}
		return fmt.Errorf("failed to create namespace: %w", err)
	}

	return nil
}

// DropNamespace drops a namespace.
func (c *Client) DropNamespace(ctx context.Context, namespace string) error {
	ns := catalog.Namespace(strings.Split(namespace, "."))

	err := c.catalog.DropNamespace(ctx, ns)
	if err != nil {
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "NoSuchNamespaceException") {
			return &NamespaceNotFoundError{Namespace: namespace}
		}
		if strings.Contains(err.Error(), "not empty") || strings.Contains(err.Error(), "NamespaceNotEmptyException") {
			return fmt.Errorf("%w: %s", ErrNamespaceNotEmpty, namespace)
		}
		return fmt.Errorf("failed to drop namespace: %w", err)
	}

	return nil
}

// RenameTable renames a table.
func (c *Client) RenameTable(ctx context.Context, fromNamespace, fromName, toNamespace, toName string) error {
	from := catalog.TableIdentifier{
		Namespace: catalog.Namespace(strings.Split(fromNamespace, ".")),
		Name:      fromName,
	}
	to := catalog.TableIdentifier{
		Namespace: catalog.Namespace(strings.Split(toNamespace, ".")),
		Name:      toName,
	}

	err := c.catalog.RenameTable(ctx, from, to)
	if err != nil {
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "NoSuchTableException") {
			return &TableNotFoundError{Namespace: fromNamespace, TableName: fromName}
		}
		return fmt.Errorf("failed to rename table: %w", err)
	}

	return nil
}

// CreateTableConfig holds options for table creation.
type CreateTableConfig struct {
	PartitionSpec *spec.PartitionSpec
	SortOrder     *spec.SortOrder
	Location      string
	Properties    map[string]string
}

// CreateTableOption configures table creation.
type CreateTableOption func(*CreateTableConfig)

// WithTablePartitionSpec sets the partition spec for table creation.
func WithTablePartitionSpec(spec *spec.PartitionSpec) CreateTableOption {
	return func(c *CreateTableConfig) {
		c.PartitionSpec = spec
	}
}

// WithTableSortOrder sets the sort order for table creation.
func WithTableSortOrder(order *spec.SortOrder) CreateTableOption {
	return func(c *CreateTableConfig) {
		c.SortOrder = order
	}
}

// WithTableLocation sets the location for table creation.
func WithTableLocation(location string) CreateTableOption {
	return func(c *CreateTableConfig) {
		c.Location = location
	}
}

// WithTableProperties sets properties for table creation.
func WithTableProperties(props map[string]string) CreateTableOption {
	return func(c *CreateTableConfig) {
		for k, v := range props {
			c.Properties[k] = v
		}
	}
}
