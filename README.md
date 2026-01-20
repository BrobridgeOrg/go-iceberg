# go-iceberg

[![Go Version](https://img.shields.io/badge/Go-1.24+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

A Go library for [Apache Iceberg](https://iceberg.apache.org/) table operations. This library provides a simple, idiomatic Go API for creating and managing Iceberg tables with full CRUD support.

## Features

- **Full CRUD Operations** - Insert, Delete, Update, and Upsert support
- **Dual Delete Modes** - Copy-on-Write (optimized for reads) and Merge-on-Read (optimized for writes)
- **REST Catalog Support** - Compatible with Polaris, Nessie, Tabular, and other REST catalogs
- **Multiple Storage Backends** - Local filesystem and Amazon S3 support
- **Apache Arrow Integration** - Native Arrow format for efficient data processing
- **Time Travel Queries** - Query data at specific snapshots or timestamps
- **Fluent Expression Builder** - Intuitive filter expressions for queries and mutations
- **Transaction Support** - Atomic operations with optimistic concurrency control

## Installation

```bash
go get github.com/go-iceberg/go-iceberg
```

## Quick Start

```go
package main

import (
    "context"
    "log"

    goiceberg "github.com/go-iceberg/go-iceberg"
    "github.com/go-iceberg/go-iceberg/spec"
    "github.com/go-iceberg/go-iceberg/table"
)

func main() {
    ctx := context.Background()

    // Create a client
    client, err := goiceberg.NewClient(ctx,
        goiceberg.WithRESTCatalog("http://localhost:8181"),
        goiceberg.WithWarehouse("s3://my-bucket/warehouse"),
        goiceberg.WithS3(&goiceberg.S3Config{
            Region: "us-east-1",
        }),
    )
    if err != nil {
        log.Fatal(err)
    }

    // Define a schema
    schema := spec.NewSchema(1, []spec.NestedField{
        {ID: 1, Name: "id", Type: spec.LongType, Required: true},
        {ID: 2, Name: "name", Type: spec.StringType, Required: true},
        {ID: 3, Name: "email", Type: spec.StringType, Required: false},
        {ID: 4, Name: "created_at", Type: spec.TimestampType, Required: true},
    })

    // Create a table
    tbl, err := client.CreateTable(ctx, "default", "users", schema)
    if err != nil {
        log.Fatal(err)
    }

    // Insert data (using Arrow records)
    // records := []arrow.Record{...}
    // err = tbl.Insert(ctx, records)

    // Query data
    result, err := tbl.Scan().
        Filter(table.Col("id").Gt(100)).
        Select("id", "name").
        Limit(10).
        ToArrowTable(ctx)
    if err != nil {
        log.Fatal(err)
    }
    _ = result
}
```

## Documentation

### Configuration

#### Client Options

```go
// REST Catalog connection
goiceberg.WithRESTCatalog("http://localhost:8181")

// Warehouse location
goiceberg.WithWarehouse("s3://bucket/warehouse")

// S3 configuration
goiceberg.WithS3(&goiceberg.S3Config{
    Region:          "us-east-1",
    AccessKeyID:     "your-access-key",      // Optional: uses default credentials if not set
    SecretAccessKey: "your-secret-key",
    Endpoint:        "http://localhost:9000", // For MinIO or LocalStack
    ForcePathStyle:  true,                    // Required for MinIO
})

// Local filesystem
goiceberg.WithLocalStorage("/path/to/warehouse")

// Authentication
goiceberg.WithToken("bearer-token")
goiceberg.WithCredential("client-id", "client-secret")

// Write configuration
goiceberg.WithWriteMode(goiceberg.CopyOnWrite)  // or MergeOnRead
goiceberg.WithTargetFileSize(512 * 1024 * 1024) // 512MB default
```

### Schema Definition

#### Primitive Types

```go
spec.BooleanType     // boolean
spec.IntType         // 32-bit integer
spec.LongType        // 64-bit integer
spec.FloatType       // 32-bit float
spec.DoubleType      // 64-bit float
spec.StringType      // UTF-8 string
spec.BinaryType      // byte array
spec.DateType        // date without time
spec.TimeType        // time without date
spec.TimestampType   // timestamp without timezone
spec.TimestampTzType // timestamp with timezone
spec.UUIDType        // UUID
```

#### Complex Types

```go
// Decimal with precision and scale
spec.DecimalType{Precision: 10, Scale: 2}

// Fixed-length binary
spec.FixedType{Length: 16}

// List type
spec.ListType{
    ElementID:       100,
    Element:         spec.StringType,
    ElementRequired: true,
}

// Map type
spec.MapType{
    KeyID:         100,
    Key:           spec.StringType,
    ValueID:       101,
    Value:         spec.LongType,
    ValueRequired: false,
}

// Struct type (nested fields)
spec.StructType{
    Fields: []spec.NestedField{
        {ID: 100, Name: "street", Type: spec.StringType, Required: true},
        {ID: 101, Name: "city", Type: spec.StringType, Required: true},
    },
}
```

#### Creating a Schema

```go
schema := spec.NewSchema(1, []spec.NestedField{
    {ID: 1, Name: "id", Type: spec.LongType, Required: true},
    {ID: 2, Name: "name", Type: spec.StringType, Required: true},
    {ID: 3, Name: "tags", Type: spec.ListType{
        ElementID: 100,
        Element:   spec.StringType,
    }, Required: false},
    {ID: 4, Name: "metadata", Type: spec.MapType{
        KeyID:   101,
        Key:     spec.StringType,
        ValueID: 102,
        Value:   spec.StringType,
    }, Required: false},
})
```

### Table Operations

#### Creating Tables

```go
// Basic table creation
tbl, err := client.CreateTable(ctx, "namespace", "table_name", schema)

// With options
tbl, err := client.CreateTable(ctx, "namespace", "table_name", schema,
    goiceberg.WithTableLocation("s3://bucket/warehouse/namespace/table_name"),
    goiceberg.WithTableProperties(map[string]string{
        "write.format.default": "parquet",
    }),
)
```

#### Opening Existing Tables

```go
tbl, err := client.Table(ctx, "namespace", "table_name")
```

#### Table Management

```go
// Check if table exists
exists, err := client.TableExists(ctx, "namespace", "table_name")

// List tables in a namespace
tables, err := client.ListTables(ctx, "namespace")

// Drop a table
err := client.DropTable(ctx, "namespace", "table_name", false) // purge=false

// Rename a table
err := client.RenameTable(ctx, "old_ns", "old_name", "new_ns", "new_name")
```

#### Namespace Operations

```go
// List namespaces
namespaces, err := client.ListNamespaces(ctx, "")

// Create namespace
err := client.CreateNamespace(ctx, "my_namespace", map[string]string{
    "owner": "data-team",
})

// Drop namespace
err := client.DropNamespace(ctx, "my_namespace")
```

### Insert Operations

```go
// Basic insert
err := tbl.Insert(ctx, records) // records is []arrow.Record

// Insert from Arrow table
err := tbl.InsertTable(ctx, arrowTable)

// Append (alias for Insert)
err := tbl.Append(ctx, records)

// Overwrite all data
err := tbl.Overwrite(ctx, records)

// With options
err := tbl.Insert(ctx, records,
    table.WithTargetFileSizeBytes(256 * 1024 * 1024), // 256MB files
)
```

### Delete Operations

#### Copy-on-Write Mode (Default)

Rewrites data files excluding deleted rows. Best for read-heavy workloads.

```go
// Delete by filter
err := tbl.Delete(ctx, table.Col("id").Eq(123))

// Delete multiple rows
err := tbl.Delete(ctx, table.Col("status").Eq("deleted"))

// Complex filter
err := tbl.Delete(ctx, table.And(
    table.Col("created_at").Lt(cutoffDate),
    table.Col("status").Eq("expired"),
))
```

#### Merge-on-Read Mode

Writes delete files instead of rewriting data. Best for write-heavy workloads.

```go
err := tbl.Delete(ctx,
    table.Col("id").Eq(123),
    table.WithDeleteMode(table.MergeOnRead),
)
```

### Update Operations

```go
// Update with filter and values
err := tbl.Update(ctx,
    table.Col("id").Eq(123),
    map[string]any{
        "name":       "Updated Name",
        "updated_at": time.Now(),
    },
)

// Update multiple rows
err := tbl.Update(ctx,
    table.Col("status").Eq("pending"),
    map[string]any{"status": "processed"},
)
```

### Upsert Operations

Insert or update based on key columns:

```go
// Upsert: insert new rows, update existing ones based on "id" column
err := tbl.Upsert(ctx, records, []string{"id"})

// Composite key
err := tbl.Upsert(ctx, records, []string{"tenant_id", "user_id"})
```

### Scan & Query

#### Basic Scanning

```go
// Full table scan
result, err := tbl.Scan().ToArrowTable(ctx)

// With filter
result, err := tbl.Scan().
    Filter(table.Col("age").Gte(18)).
    ToArrowTable(ctx)

// Column projection
result, err := tbl.Scan().
    Select("id", "name", "email").
    ToArrowTable(ctx)

// With limit
result, err := tbl.Scan().
    Limit(100).
    ToArrowTable(ctx)
```

#### Time Travel

```go
// Query at a specific timestamp
result, err := tbl.Scan().
    AsOf(time.Now().Add(-24 * time.Hour)).
    ToArrowTable(ctx)

// Query at a specific snapshot
result, err := tbl.Scan().
    WithSnapshot(snapshotID).
    ToArrowTable(ctx)
```

#### Snapshot Management

```go
// Get current snapshot
snapshot := tbl.CurrentSnapshot()

// List all snapshots
snapshots := tbl.Snapshots()

// Get snapshot by ID
snapshot := tbl.SnapshotByID(snapshotID)
```

### Expression Builder

#### Column Builder Pattern

```go
table.Col("column_name").Eq(value)      // column = value
table.Col("column_name").NotEq(value)   // column != value
table.Col("column_name").Lt(value)      // column < value
table.Col("column_name").Lte(value)     // column <= value
table.Col("column_name").Gt(value)      // column > value
table.Col("column_name").Gte(value)     // column >= value
table.Col("column_name").In(v1, v2, v3) // column IN (v1, v2, v3)
table.Col("column_name").NotIn(v1, v2)  // column NOT IN (v1, v2)
table.Col("column_name").IsNull()       // column IS NULL
table.Col("column_name").IsNotNull()    // column IS NOT NULL
table.Col("column_name").StartsWith("prefix") // column STARTS WITH 'prefix'
```

#### Convenience Functions

```go
table.Eq("id", 123)
table.NotEq("status", "deleted")
table.Gt("age", 18)
table.Gte("score", 0.5)
table.Lt("price", 100)
table.Lte("quantity", 10)
table.In("category", "A", "B", "C")
table.IsNull("deleted_at")
table.IsNotNull("email")
table.Between("price", 10.0, 100.0) // price >= 10 AND price <= 100
```

#### Logical Operators

```go
// AND
table.And(
    table.Col("age").Gte(18),
    table.Col("status").Eq("active"),
)

// OR
table.Or(
    table.Col("role").Eq("admin"),
    table.Col("role").Eq("moderator"),
)

// NOT
table.Not(table.Col("deleted").Eq(true))

// Complex nested expressions
table.Or(
    table.And(
        table.Col("age").Gte(18),
        table.Col("verified").Eq(true),
    ),
    table.Col("role").Eq("admin"),
)
```

## Project Structure

```
go-iceberg/
├── iceberg.go           # Client entry point and public API
├── config.go            # Configuration types and options
├── errors.go            # Error type definitions
├── spec/                # Iceberg specification types
│   ├── types.go         # Data types (primitive, complex)
│   ├── schema.go        # Schema definitions
│   ├── partition.go     # Partition specifications
│   ├── snapshot.go      # Snapshot management
│   ├── metadata.go      # Table metadata
│   ├── manifest.go      # Manifest and data file definitions
│   └── avro.go          # Avro serialization for manifests
├── catalog/             # Catalog implementations
│   ├── catalog.go       # Catalog interface
│   └── rest.go          # REST Catalog client
├── io/                  # File I/O abstraction
│   ├── fileio.go        # FileIO interface
│   ├── local.go         # Local filesystem implementation
│   └── s3.go            # S3 storage implementation
├── table/               # Table operations
│   ├── table.go         # Table wrapper and methods
│   ├── scan.go          # Scan builder
│   ├── insert.go        # Insert operations
│   ├── delete.go        # Delete operations (CoW & MoR)
│   ├── update.go        # Update and Upsert operations
│   ├── writer.go        # Data file writing
│   └── expression.go    # Filter expression builder
└── examples/
    └── basic/           # Usage examples
```

## Examples

See the [examples](examples/) directory for complete working examples.

### Basic Example

```go
// examples/basic/main.go
go run examples/basic/main.go
```

## Compatibility

### Iceberg Specification
- Implements Apache Iceberg table format specification

### Compatible Catalogs
- **Polaris** - Snowflake's open-source Iceberg catalog
- **Nessie** - Git-like version control for data lakes
- **Tabular** - Managed Iceberg service
- Any REST Catalog implementing the [Iceberg REST OpenAPI spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)

### Storage Backends
- Local filesystem
- Amazon S3 (including MinIO, LocalStack)

### File Formats
- Parquet (primary format)

## Dependencies

- [apache/arrow-go](https://github.com/apache/arrow-go) - Apache Arrow for Go
- [aws/aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2) - AWS SDK for S3 access
- [linkedin/goavro](https://github.com/linkedin/goavro) - Avro serialization
- [google/uuid](https://github.com/google/uuid) - UUID generation

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

### Development Setup

```bash
# Clone the repository
git clone https://github.com/go-iceberg/go-iceberg.git
cd go-iceberg

# Install dependencies
go mod download

# Run tests
go test ./...

# Build
go build ./...
```

### Running Tests

```bash
# Unit tests
go test ./...

# With verbose output
go test -v ./...

# Specific package
go test -v ./table/...
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Apache Iceberg](https://iceberg.apache.org/) - The table format specification
- [Apache Arrow](https://arrow.apache.org/) - Columnar data format
