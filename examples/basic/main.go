// Package main demonstrates basic usage of the go-iceberg library.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	goiceberg "github.com/BrobridgeOrg/go-iceberg"
	"github.com/BrobridgeOrg/go-iceberg/spec"
	"github.com/BrobridgeOrg/go-iceberg/table"
)

func main() {
	ctx := context.Background()

	// Example 1: Connect to a REST Catalog
	fmt.Println("=== Go-Iceberg Library Demo ===")
	fmt.Println()

	// Note: This example shows the API usage. To run it, you need:
	// 1. A running REST Catalog (like Apache Polaris, Nessie, or Tabular)
	// 2. S3 or local storage configured

	// Create client with REST Catalog
	client, err := goiceberg.NewClient(ctx,
		goiceberg.WithRESTCatalog("http://localhost:8181"),
		goiceberg.WithWarehouse("s3://my-bucket/warehouse"),
		goiceberg.WithS3(&goiceberg.S3Config{
			Region:   "us-east-1",
			Endpoint: "http://localhost:9000", // MinIO endpoint
		}),
	)
	if err != nil {
		log.Printf("Note: Could not connect to catalog (expected if no catalog is running): %v", err)
		fmt.Println()
		demonstrateAPIUsage()
		return
	}
	defer func() {
		// Client cleanup if needed
	}()

	// Example 2: Create a table
	fmt.Println("Creating a table...")
	schema := spec.NewSchema(1, []spec.NestedField{
		{ID: 1, Name: "id", Type: spec.LongType, Required: true},
		{ID: 2, Name: "name", Type: spec.StringType, Required: true},
		{ID: 3, Name: "email", Type: spec.StringType, Required: false},
		{ID: 4, Name: "created_at", Type: spec.TimestampType, Required: true},
	})

	tbl, err := client.CreateTable(ctx, "default", "users", schema,
		goiceberg.WithTableLocation("s3://my-bucket/warehouse/default/users"),
	)
	if err != nil {
		log.Printf("Note: Could not create table (expected if no catalog is running): %v", err)
		fmt.Println()
		demonstrateAPIUsage()
		return
	}

	fmt.Printf("Created table: %s\n", tbl.Identifier())
	fmt.Println()

	// Example 3: Insert data
	fmt.Println("Inserting data...")
	records := createSampleRecords()
	if err := tbl.Insert(ctx, records); err != nil {
		log.Printf("Failed to insert data: %v", err)
		return
	}
	fmt.Println("Data inserted successfully")
	fmt.Println()

	// Example 4: Query data
	fmt.Println("Querying data...")
	result, err := tbl.Scan().ToArrowTable(ctx)
	if err != nil {
		log.Printf("Failed to scan table: %v", err)
		return
	}
	fmt.Printf("Found %d rows\n", result.NumRows())
	result.Release()
	fmt.Println()

	// Example 5: Delete data
	fmt.Println("Deleting data where id > 5...")
	err = tbl.Delete(ctx, table.Col("id").Gt(5))
	if err != nil {
		log.Printf("Failed to delete data: %v", err)
		return
	}
	fmt.Println("Delete completed")
	fmt.Println()

	// Example 6: Update data
	fmt.Println("Updating data where id = 1...")
	err = tbl.Update(ctx,
		table.Col("id").Eq(1),
		map[string]any{"name": "Updated Name"},
	)
	if err != nil {
		log.Printf("Failed to update data: %v", err)
		return
	}
	fmt.Println("Update completed")
}

// createSampleRecords creates sample Arrow records for testing.
func createSampleRecords() []arrow.Record {
	mem := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "email", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "created_at", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int64Builder)
	nameBuilder := builder.Field(1).(*array.StringBuilder)
	emailBuilder := builder.Field(2).(*array.StringBuilder)
	createdBuilder := builder.Field(3).(*array.TimestampBuilder)

	// Add sample data
	for i := int64(1); i <= 10; i++ {
		idBuilder.Append(i)
		nameBuilder.Append(fmt.Sprintf("User %d", i))
		if i%2 == 0 {
			emailBuilder.Append(fmt.Sprintf("user%d@example.com", i))
		} else {
			emailBuilder.AppendNull()
		}
		createdBuilder.Append(arrow.Timestamp(1704067200000000 + i*1000000)) // 2024-01-01 + offset
	}

	record := builder.NewRecord()
	return []arrow.Record{record}
}

// demonstrateAPIUsage shows the API usage without a running catalog.
func demonstrateAPIUsage() {
	fmt.Println("=== API Usage Examples ===")
	fmt.Println()

	fmt.Println("1. Creating a Client:")
	fmt.Println(`
   client, err := goiceberg.NewClient(ctx,
       goiceberg.WithRESTCatalog("http://localhost:8181"),
       goiceberg.WithWarehouse("s3://bucket/warehouse"),
       goiceberg.WithS3(&goiceberg.S3Config{Region: "us-east-1"}),
   )`)
	fmt.Println()

	fmt.Println("2. Creating a Table:")
	fmt.Println(`
   schema := spec.NewSchema(1,
       spec.NewField(1, "id", spec.TypeLong, true),
       spec.NewField(2, "name", spec.TypeString, true),
   )
   table, err := client.CreateTable(ctx, "namespace", "table_name", schema)`)
	fmt.Println()

	fmt.Println("3. Opening an Existing Table:")
	fmt.Println(`
   table, err := client.Table(ctx, "namespace", "table_name")`)
	fmt.Println()

	fmt.Println("4. Inserting Data:")
	fmt.Println(`
   records := []arrow.Record{...}
   err := table.Insert(ctx, records)

   // Or with overwrite:
   err := table.Overwrite(ctx, records)`)
	fmt.Println()

	fmt.Println("5. Deleting Data:")
	fmt.Println(`
   // Copy-on-Write (default)
   err := table.Delete(ctx, table.Col("id").Eq(123))

   // Merge-on-Read
   err := table.Delete(ctx, table.Col("id").Eq(123),
       table.WithDeleteMode(table.MergeOnRead))`)
	fmt.Println()

	fmt.Println("6. Updating Data:")
	fmt.Println(`
   err := table.Update(ctx,
       table.Col("id").Eq(123),
       map[string]any{"name": "New Name", "score": 0.95},
   )`)
	fmt.Println()

	fmt.Println("7. Upsert (Insert or Update):")
	fmt.Println(`
   err := table.Upsert(ctx, records, []string{"id"})  // "id" is the key column`)
	fmt.Println()

	fmt.Println("8. Scanning Data:")
	fmt.Println(`
   // Full scan
   result, err := table.Scan().ToArrowTable(ctx)

   // With filter and limit
   result, err := table.Scan().
       Filter(table.Col("age").Gte(18)).
       Select("id", "name").
       Limit(100).
       ToArrowTable(ctx)

   // Time travel
   result, err := table.Scan().
       AsOf(time.Now().Add(-24 * time.Hour)).
       ToArrowTable(ctx)`)
	fmt.Println()

	fmt.Println("9. Expression Builder:")
	fmt.Println(`
   // Comparisons
   table.Col("id").Eq(1)
   table.Col("age").Gte(18)
   table.Col("name").In("Alice", "Bob")
   table.Col("email").IsNotNull()

   // Logical operators
   table.And(
       table.Col("age").Gte(18),
       table.Col("status").Eq("active"),
   )
   table.Or(
       table.Col("role").Eq("admin"),
       table.Col("role").Eq("moderator"),
   )

   // Between
   table.Between("price", 10.0, 100.0)`)
	fmt.Println()

	fmt.Println("10. Snapshot Management:")
	fmt.Println(`
   // List snapshots
   snapshots := table.Snapshots()

   // Get current snapshot
   current := table.CurrentSnapshot()

   // Scan at specific snapshot
   result, err := table.Scan().WithSnapshot(snapshotID).ToArrowTable(ctx)`)
	fmt.Println()
}
