package goiceberg

import (
	"time"
)

// CatalogType represents supported catalog types.
type CatalogType string

const (
	// CatalogREST represents the Iceberg REST Catalog.
	CatalogREST CatalogType = "rest"
	// CatalogGlue represents AWS Glue Catalog.
	CatalogGlue CatalogType = "glue"
	// CatalogSQL represents SQL-based catalog.
	CatalogSQL CatalogType = "sql"
)

// StorageType represents supported storage backends.
type StorageType string

const (
	// StorageLocal represents local filesystem storage.
	StorageLocal StorageType = "local"
	// StorageS3 represents Amazon S3 storage.
	StorageS3 StorageType = "s3"
	// StorageGCS represents Google Cloud Storage.
	StorageGCS StorageType = "gcs"
	// StorageAzure represents Azure Blob Storage.
	StorageAzure StorageType = "azure"
)

// WriteMode determines how updates and deletes are handled.
type WriteMode string

const (
	// CopyOnWrite rewrites affected files without deleted/updated rows.
	// Better read performance, suitable for OLAP workloads.
	CopyOnWrite WriteMode = "copy-on-write"

	// MergeOnRead writes delete files that are merged at read time.
	// Faster writes, suitable for frequent update workloads.
	MergeOnRead WriteMode = "merge-on-read"
)

// Config holds the client configuration.
type Config struct {
	// Catalog configuration
	CatalogType CatalogType
	CatalogURI  string
	Warehouse   string

	// Authentication
	Credential string // client_id:client_secret for OAuth2
	Token      string // Bearer token
	Scope      string // OAuth2 scope

	// Storage configuration
	StorageType StorageType
	S3Config    *S3Config
	LocalConfig *LocalConfig

	// Write configuration
	WriteMode      WriteMode
	TargetFileSize int64 // Target file size in bytes (default 512MB)

	// Retry configuration
	MaxRetries   int
	RetryBackoff time.Duration
}

// S3Config holds S3-specific configuration.
type S3Config struct {
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Endpoint        string // For MinIO, LocalStack, etc.
	ForcePathStyle  bool
}

// LocalConfig holds local filesystem configuration.
type LocalConfig struct {
	BasePath string
}

// DefaultConfig returns a Config with default values.
func DefaultConfig() *Config {
	return &Config{
		CatalogType:    CatalogREST,
		WriteMode:      CopyOnWrite,
		TargetFileSize: 512 * 1024 * 1024, // 512MB
		MaxRetries:     3,
		RetryBackoff:   100 * time.Millisecond,
	}
}

// Option is a functional option for client configuration.
type Option func(*Config)

// WithRESTCatalog configures the client to use a REST catalog.
func WithRESTCatalog(uri string) Option {
	return func(c *Config) {
		c.CatalogType = CatalogREST
		c.CatalogURI = uri
	}
}

// WithGlueCatalog configures the client to use AWS Glue catalog.
func WithGlueCatalog() Option {
	return func(c *Config) {
		c.CatalogType = CatalogGlue
	}
}

// WithWarehouse sets the warehouse location.
func WithWarehouse(warehouse string) Option {
	return func(c *Config) {
		c.Warehouse = warehouse
	}
}

// WithCredential sets OAuth2 credentials for authentication.
func WithCredential(clientID, clientSecret string) Option {
	return func(c *Config) {
		c.Credential = clientID + ":" + clientSecret
	}
}

// WithToken sets a bearer token for authentication.
func WithToken(token string) Option {
	return func(c *Config) {
		c.Token = token
	}
}

// WithScope sets the OAuth2 scope.
func WithScope(scope string) Option {
	return func(c *Config) {
		c.Scope = scope
	}
}

// WithS3 configures S3 storage backend.
func WithS3(cfg *S3Config) Option {
	return func(c *Config) {
		c.StorageType = StorageS3
		c.S3Config = cfg
	}
}

// WithLocalStorage configures local filesystem storage.
func WithLocalStorage(basePath string) Option {
	return func(c *Config) {
		c.StorageType = StorageLocal
		c.LocalConfig = &LocalConfig{BasePath: basePath}
	}
}

// WithWriteMode sets the write mode for updates and deletes.
func WithWriteMode(mode WriteMode) Option {
	return func(c *Config) {
		c.WriteMode = mode
	}
}

// WithTargetFileSize sets the target file size for data files.
func WithTargetFileSize(size int64) Option {
	return func(c *Config) {
		c.TargetFileSize = size
	}
}

// WithMaxRetries sets the maximum number of retry attempts.
func WithMaxRetries(n int) Option {
	return func(c *Config) {
		c.MaxRetries = n
	}
}

// WithRetryBackoff sets the initial backoff duration for retries.
func WithRetryBackoff(d time.Duration) Option {
	return func(c *Config) {
		c.RetryBackoff = d
	}
}

// Note: CreateTableOption and CreateTableConfig are defined in iceberg.go
// along with WithTablePartitionSpec, WithTableSortOrder, WithTableLocation, and WithTableProperties
