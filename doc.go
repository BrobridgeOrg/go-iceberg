// Package goiceberg provides a user-friendly Go library for Apache Iceberg table operations.
//
// This library wraps and extends the apache/iceberg-go implementation to provide
// simplified APIs for common Iceberg operations including:
//
//   - Table creation and management
//   - Insert, Update, Delete, and Upsert operations
//   - Snapshot management and time travel queries
//   - Schema and partition evolution
//   - REST Catalog integration
//   - Local filesystem and S3 storage support
//
// # Quick Start
//
// Create a client connected to a REST catalog:
//
//	client, err := goiceberg.NewClient(ctx,
//	    goiceberg.WithRESTCatalog("http://localhost:8181"),
//	    goiceberg.WithWarehouse("s3://my-bucket/warehouse"),
//	)
//
// Open and query a table:
//
//	table, err := client.Table(ctx, "analytics", "events")
//	result, err := table.Scan().
//	    Select("id", "name", "created_at").
//	    Filter(operations.Expr("id").Gt(100)).
//	    ToArrowTable(ctx)
//
// Insert data:
//
//	err := table.Insert(ctx, recordBatch)
//
// Delete rows:
//
//	err := table.Delete(ctx, operations.Expr("status").Eq("deleted"))
//
// # Architecture
//
// This library is built on top of github.com/apache/iceberg-go and provides:
//
//   - High-level Client API for common operations
//   - Fluent expression builder for filters
//   - Delete operations (Copy-on-Write and Merge-on-Read)
//   - Update and Upsert operations
//   - Snapshot management utilities
//
// # Delete Modes
//
// The library supports two delete modes:
//
//   - Copy-on-Write (default): Rewrites affected files without deleted rows.
//     Better read performance, suitable for OLAP workloads.
//
//   - Merge-on-Read: Writes delete files that are merged at read time.
//     Faster writes, suitable for frequent update workloads.
package goiceberg
