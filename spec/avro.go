package spec

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/linkedin/goavro/v2"
)

// AvroSchemaManifestListV2 is the Avro schema for manifest list files (V2).
const AvroSchemaManifestListV2 = `{
  "type": "record",
  "name": "manifest_file",
  "fields": [
    {"name": "manifest_path", "type": "string"},
    {"name": "manifest_length", "type": "long"},
    {"name": "partition_spec_id", "type": "int"},
    {"name": "content", "type": "int", "default": 0},
    {"name": "sequence_number", "type": "long", "default": 0},
    {"name": "min_sequence_number", "type": "long", "default": 0},
    {"name": "added_snapshot_id", "type": "long"},
    {"name": "added_files_count", "type": "int", "default": 0},
    {"name": "existing_files_count", "type": "int", "default": 0},
    {"name": "deleted_files_count", "type": "int", "default": 0},
    {"name": "added_rows_count", "type": "long", "default": 0},
    {"name": "existing_rows_count", "type": "long", "default": 0},
    {"name": "deleted_rows_count", "type": "long", "default": 0},
    {"name": "partitions", "type": {
      "type": "array",
      "items": {
        "type": "record",
        "name": "field_summary",
        "fields": [
          {"name": "contains_null", "type": "boolean"},
          {"name": "contains_nan", "type": ["null", "boolean"], "default": null},
          {"name": "lower_bound", "type": ["null", "bytes"], "default": null},
          {"name": "upper_bound", "type": ["null", "bytes"], "default": null}
        ]
      }
    }, "default": []},
    {"name": "key_metadata", "type": ["null", "bytes"], "default": null}
  ]
}`

// AvroSchemaManifestEntryV2 is the Avro schema for manifest entries (V2).
// Note: The actual schema depends on the table's partition spec and is embedded in the manifest file.
const AvroSchemaManifestEntryV2Template = `{
  "type": "record",
  "name": "manifest_entry",
  "fields": [
    {"name": "status", "type": "int"},
    {"name": "snapshot_id", "type": ["null", "long"], "default": null},
    {"name": "sequence_number", "type": ["null", "long"], "default": null},
    {"name": "file_sequence_number", "type": ["null", "long"], "default": null},
    {"name": "data_file", "type": {
      "type": "record",
      "name": "data_file",
      "fields": [
        {"name": "content", "type": "int", "default": 0},
        {"name": "file_path", "type": "string"},
        {"name": "file_format", "type": "string"},
        {"name": "partition", "type": %s},
        {"name": "record_count", "type": "long"},
        {"name": "file_size_in_bytes", "type": "long"},
        {"name": "column_sizes", "type": ["null", {"type": "map", "values": "long"}], "default": null},
        {"name": "value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": null},
        {"name": "null_value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": null},
        {"name": "nan_value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": null},
        {"name": "lower_bounds", "type": ["null", {"type": "map", "values": "bytes"}], "default": null},
        {"name": "upper_bounds", "type": ["null", {"type": "map", "values": "bytes"}], "default": null},
        {"name": "key_metadata", "type": ["null", "bytes"], "default": null},
        {"name": "split_offsets", "type": ["null", {"type": "array", "items": "long"}], "default": null},
        {"name": "equality_ids", "type": ["null", {"type": "array", "items": "int"}], "default": null},
        {"name": "sort_order_id", "type": ["null", "int"], "default": null}
      ]
    }}
  ]
}`

// ManifestListWriter writes manifest list files in Avro format.
type ManifestListWriter struct {
	codec  *goavro.Codec
	buffer *bytes.Buffer
	ocf    *goavro.OCFWriter
}

// NewManifestListWriter creates a new manifest list writer.
func NewManifestListWriter() (*ManifestListWriter, error) {
	codec, err := goavro.NewCodec(AvroSchemaManifestListV2)
	if err != nil {
		return nil, fmt.Errorf("failed to create avro codec: %w", err)
	}

	buf := new(bytes.Buffer)
	ocf, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:               buf,
		Codec:           codec,
		CompressionName: "deflate",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create OCF writer: %w", err)
	}

	return &ManifestListWriter{
		codec:  codec,
		buffer: buf,
		ocf:    ocf,
	}, nil
}

// Append appends a manifest file entry.
func (w *ManifestListWriter) Append(mf ManifestFile) error {
	record := map[string]any{
		"manifest_path":        mf.ManifestPath,
		"manifest_length":      mf.ManifestLength,
		"partition_spec_id":    mf.PartitionSpecID,
		"content":              int(mf.Content),
		"sequence_number":      mf.SequenceNumber,
		"min_sequence_number":  mf.MinSequenceNumber,
		"added_snapshot_id":    mf.AddedSnapshotID,
		"added_files_count":    mf.AddedFilesCount,
		"existing_files_count": mf.ExistingFilesCount,
		"deleted_files_count":  mf.DeletedFilesCount,
		"added_rows_count":     mf.AddedRowsCount,
		"existing_rows_count":  mf.ExistingRowsCount,
		"deleted_rows_count":   mf.DeletedRowsCount,
	}

	// Handle partitions
	partitions := make([]any, len(mf.Partitions))
	for i, p := range mf.Partitions {
		ps := map[string]any{
			"contains_null": p.ContainsNull,
		}
		if p.ContainsNaN != nil {
			ps["contains_nan"] = goavro.Union("boolean", *p.ContainsNaN)
		} else {
			ps["contains_nan"] = nil
		}
		if p.LowerBound != nil {
			ps["lower_bound"] = goavro.Union("bytes", p.LowerBound)
		} else {
			ps["lower_bound"] = nil
		}
		if p.UpperBound != nil {
			ps["upper_bound"] = goavro.Union("bytes", p.UpperBound)
		} else {
			ps["upper_bound"] = nil
		}
		partitions[i] = ps
	}
	record["partitions"] = partitions

	// Handle key metadata
	if mf.KeyMetadata != nil {
		record["key_metadata"] = goavro.Union("bytes", mf.KeyMetadata)
	} else {
		record["key_metadata"] = nil
	}

	return w.ocf.Append([]any{record})
}

// Bytes returns the written Avro data.
func (w *ManifestListWriter) Bytes() []byte {
	return w.buffer.Bytes()
}

// ManifestListReader reads manifest list files.
type ManifestListReader struct {
	ocf *goavro.OCFReader
}

// NewManifestListReader creates a new manifest list reader.
func NewManifestListReader(r io.Reader) (*ManifestListReader, error) {
	ocf, err := goavro.NewOCFReader(r)
	if err != nil {
		return nil, fmt.Errorf("failed to create OCF reader: %w", err)
	}

	return &ManifestListReader{ocf: ocf}, nil
}

// Read reads all manifest files from the list.
func (r *ManifestListReader) Read() ([]ManifestFile, error) {
	var manifests []ManifestFile

	for r.ocf.Scan() {
		record, err := r.ocf.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read manifest entry: %w", err)
		}

		m, ok := record.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("unexpected record type")
		}

		mf := ManifestFile{
			ManifestPath:       getString(m, "manifest_path"),
			ManifestLength:     getInt64(m, "manifest_length"),
			PartitionSpecID:    getInt(m, "partition_spec_id"),
			Content:            ManifestContent(getInt(m, "content")),
			SequenceNumber:     getInt64(m, "sequence_number"),
			MinSequenceNumber:  getInt64(m, "min_sequence_number"),
			AddedSnapshotID:    getInt64(m, "added_snapshot_id"),
			AddedFilesCount:    getInt(m, "added_files_count"),
			ExistingFilesCount: getInt(m, "existing_files_count"),
			DeletedFilesCount:  getInt(m, "deleted_files_count"),
			AddedRowsCount:     getInt64(m, "added_rows_count"),
			ExistingRowsCount:  getInt64(m, "existing_rows_count"),
			DeletedRowsCount:   getInt64(m, "deleted_rows_count"),
		}

		// Parse partitions
		if partitions, ok := m["partitions"].([]any); ok {
			mf.Partitions = make([]PartitionFieldSummary, len(partitions))
			for i, p := range partitions {
				pm, ok := p.(map[string]any)
				if !ok {
					continue
				}
				mf.Partitions[i] = PartitionFieldSummary{
					ContainsNull: getBool(pm, "contains_null"),
					ContainsNaN:  getOptionalBool(pm, "contains_nan"),
					LowerBound:   getOptionalBytes(pm, "lower_bound"),
					UpperBound:   getOptionalBytes(pm, "upper_bound"),
				}
			}
		}

		mf.KeyMetadata = getOptionalBytes(m, "key_metadata")
		manifests = append(manifests, mf)
	}

	if err := r.ocf.Err(); err != nil {
		return nil, fmt.Errorf("error reading manifest list: %w", err)
	}

	return manifests, nil
}

// ManifestWriter writes manifest files in Avro format.
type ManifestWriter struct {
	codec       *goavro.Codec
	buffer      *bytes.Buffer
	ocf         *goavro.OCFWriter
	schemaID    int
	specID      int
	content     ManifestContent
	seqNum      int64
	partitionSchema string
}

// NewManifestWriter creates a new manifest writer.
func NewManifestWriter(schemaID, specID int, content ManifestContent, seqNum int64, partitionSpec *PartitionSpec) (*ManifestWriter, error) {
	// Build partition schema based on partition spec
	partitionSchema := buildPartitionAvroSchema(partitionSpec)
	avroSchema := fmt.Sprintf(AvroSchemaManifestEntryV2Template, partitionSchema)

	codec, err := goavro.NewCodec(avroSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to create avro codec: %w", err)
	}

	buf := new(bytes.Buffer)

	// Write metadata
	metadata := map[string][]byte{
		"schema":           []byte(fmt.Sprintf(`{"schema-id": %d}`, schemaID)),
		"partition-spec":   []byte(fmt.Sprintf(`{"spec-id": %d}`, specID)),
		"content":          []byte(fmt.Sprintf("%d", content)),
		"format-version":   []byte("2"),
	}

	ocf, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:               buf,
		Codec:           codec,
		CompressionName: "deflate",
		MetaData:        metadata,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create OCF writer: %w", err)
	}

	return &ManifestWriter{
		codec:       codec,
		buffer:      buf,
		ocf:         ocf,
		schemaID:    schemaID,
		specID:      specID,
		content:     content,
		seqNum:      seqNum,
		partitionSchema: partitionSchema,
	}, nil
}

// buildPartitionAvroSchema builds the Avro schema for partition data.
func buildPartitionAvroSchema(spec *PartitionSpec) string {
	if spec == nil || len(spec.Fields) == 0 {
		return `{"type": "record", "name": "partition_data", "fields": []}`
	}

	fields := make([]map[string]any, len(spec.Fields))
	for i, f := range spec.Fields {
		// Determine the Avro type based on the transform
		avroType := "string" // Default
		switch f.Transform {
		case "identity":
			avroType = `["null", "string"]`
		case "year", "month", "day", "hour", "bucket", "truncate":
			avroType = `["null", "int"]`
		default:
			avroType = `["null", "string"]`
		}

		fields[i] = map[string]any{
			"name":    f.Name,
			"type":    json.RawMessage(avroType),
			"default": nil,
		}
	}

	schema := map[string]any{
		"type":   "record",
		"name":   "partition_data",
		"fields": fields,
	}

	data, _ := json.Marshal(schema)
	return string(data)
}

// Append appends a manifest entry.
func (w *ManifestWriter) Append(entry ManifestEntry) error {
	record := map[string]any{
		"status": int(entry.Status),
	}

	if entry.SnapshotID != nil {
		record["snapshot_id"] = goavro.Union("long", *entry.SnapshotID)
	} else {
		record["snapshot_id"] = nil
	}

	if entry.SequenceNumber != nil {
		record["sequence_number"] = goavro.Union("long", *entry.SequenceNumber)
	} else {
		record["sequence_number"] = nil
	}

	if entry.FileSequenceNumber != nil {
		record["file_sequence_number"] = goavro.Union("long", *entry.FileSequenceNumber)
	} else {
		record["file_sequence_number"] = nil
	}

	// Build data file record
	df := entry.DataFile
	dataFile := map[string]any{
		"content":            int(df.Content),
		"file_path":          df.FilePath,
		"file_format":        string(df.FileFormat),
		"record_count":       df.RecordCount,
		"file_size_in_bytes": df.FileSizeInBytes,
	}

	// Partition data
	if df.PartitionData != nil {
		dataFile["partition"] = df.PartitionData
	} else {
		dataFile["partition"] = map[string]any{}
	}

	// Optional fields
	dataFile["column_sizes"] = optionalIntMap(df.ColumnSizes)
	dataFile["value_counts"] = optionalIntMap(df.ValueCounts)
	dataFile["null_value_counts"] = optionalIntMap(df.NullValueCounts)
	dataFile["nan_value_counts"] = optionalIntMap(df.NaNValueCounts)
	dataFile["lower_bounds"] = optionalBytesMap(df.LowerBounds)
	dataFile["upper_bounds"] = optionalBytesMap(df.UpperBounds)

	if df.KeyMetadata != nil {
		dataFile["key_metadata"] = goavro.Union("bytes", df.KeyMetadata)
	} else {
		dataFile["key_metadata"] = nil
	}

	if len(df.SplitOffsets) > 0 {
		offsets := make([]any, len(df.SplitOffsets))
		for i, o := range df.SplitOffsets {
			offsets[i] = o
		}
		dataFile["split_offsets"] = goavro.Union("array", offsets)
	} else {
		dataFile["split_offsets"] = nil
	}

	if len(df.EqualityIDs) > 0 {
		ids := make([]any, len(df.EqualityIDs))
		for i, id := range df.EqualityIDs {
			ids[i] = id
		}
		dataFile["equality_ids"] = goavro.Union("array", ids)
	} else {
		dataFile["equality_ids"] = nil
	}

	if df.SortOrderID != nil {
		dataFile["sort_order_id"] = goavro.Union("int", *df.SortOrderID)
	} else {
		dataFile["sort_order_id"] = nil
	}

	record["data_file"] = dataFile

	return w.ocf.Append([]any{record})
}

// Bytes returns the written Avro data.
func (w *ManifestWriter) Bytes() []byte {
	return w.buffer.Bytes()
}

// ManifestReader reads manifest files.
type ManifestReader struct {
	ocf      *goavro.OCFReader
	metadata map[string][]byte
}

// NewManifestReader creates a new manifest reader.
func NewManifestReader(r io.Reader) (*ManifestReader, error) {
	ocf, err := goavro.NewOCFReader(r)
	if err != nil {
		return nil, fmt.Errorf("failed to create OCF reader: %w", err)
	}

	return &ManifestReader{
		ocf:      ocf,
		metadata: ocf.MetaData(),
	}, nil
}

// Metadata returns the manifest metadata.
func (r *ManifestReader) Metadata() map[string][]byte {
	return r.metadata
}

// Read reads all entries from the manifest.
func (r *ManifestReader) Read() (*Manifest, error) {
	manifest := &Manifest{
		Entries: make([]ManifestEntry, 0),
	}

	// Parse metadata
	if schemaData, ok := r.metadata["schema"]; ok {
		var schema struct {
			SchemaID int `json:"schema-id"`
		}
		json.Unmarshal(schemaData, &schema)
		manifest.SchemaID = schema.SchemaID
	}

	if specData, ok := r.metadata["partition-spec"]; ok {
		var spec struct {
			SpecID int `json:"spec-id"`
		}
		json.Unmarshal(specData, &spec)
		manifest.PartitionSpecID = spec.SpecID
	}

	if contentData, ok := r.metadata["content"]; ok {
		var content int
		fmt.Sscanf(string(contentData), "%d", &content)
		manifest.Content = ManifestContent(content)
	}

	// Read entries
	for r.ocf.Scan() {
		record, err := r.ocf.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read manifest entry: %w", err)
		}

		m, ok := record.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("unexpected record type")
		}

		entry := ManifestEntry{
			Status:     EntryStatus(getInt(m, "status")),
			SnapshotID: getOptionalInt64(m, "snapshot_id"),
			SequenceNumber: getOptionalInt64(m, "sequence_number"),
			FileSequenceNumber: getOptionalInt64(m, "file_sequence_number"),
		}

		// Parse data file
		if df, ok := m["data_file"].(map[string]any); ok {
			entry.DataFile = DataFile{
				Content:         FileContent(getInt(df, "content")),
				FilePath:        getString(df, "file_path"),
				FileFormat:      FileFormat(getString(df, "file_format")),
				RecordCount:     getInt64(df, "record_count"),
				FileSizeInBytes: getInt64(df, "file_size_in_bytes"),
				ColumnSizes:     getIntMap(df, "column_sizes"),
				ValueCounts:     getIntMap(df, "value_counts"),
				NullValueCounts: getIntMap(df, "null_value_counts"),
				NaNValueCounts:  getIntMap(df, "nan_value_counts"),
				LowerBounds:     getBytesMap(df, "lower_bounds"),
				UpperBounds:     getBytesMap(df, "upper_bounds"),
				KeyMetadata:     getOptionalBytes(df, "key_metadata"),
				SplitOffsets:    getInt64Array(df, "split_offsets"),
				EqualityIDs:     getIntArray(df, "equality_ids"),
				SortOrderID:     getOptionalInt(df, "sort_order_id"),
			}

			// Parse partition data
			if partition, ok := df["partition"].(map[string]any); ok {
				entry.DataFile.PartitionData = partition
			}
		}

		manifest.Entries = append(manifest.Entries, entry)
	}

	if err := r.ocf.Err(); err != nil {
		return nil, fmt.Errorf("error reading manifest: %w", err)
	}

	return manifest, nil
}

// Helper functions for reading Avro data

func getString(m map[string]any, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func getInt(m map[string]any, key string) int {
	switch v := m[key].(type) {
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	case float64:
		return int(v)
	}
	return 0
}

func getInt64(m map[string]any, key string) int64 {
	switch v := m[key].(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case float64:
		return int64(v)
	}
	return 0
}

func getBool(m map[string]any, key string) bool {
	if v, ok := m[key].(bool); ok {
		return v
	}
	return false
}

func getOptionalBool(m map[string]any, key string) *bool {
	v := m[key]
	if v == nil {
		return nil
	}
	// Handle union types
	if union, ok := v.(map[string]any); ok {
		if b, ok := union["boolean"].(bool); ok {
			return &b
		}
	}
	if b, ok := v.(bool); ok {
		return &b
	}
	return nil
}

func getOptionalBytes(m map[string]any, key string) []byte {
	v := m[key]
	if v == nil {
		return nil
	}
	// Handle union types
	if union, ok := v.(map[string]any); ok {
		if b, ok := union["bytes"].([]byte); ok {
			return b
		}
	}
	if b, ok := v.([]byte); ok {
		return b
	}
	return nil
}

func getOptionalInt64(m map[string]any, key string) *int64 {
	v := m[key]
	if v == nil {
		return nil
	}
	// Handle union types
	if union, ok := v.(map[string]any); ok {
		if i, ok := union["long"].(int64); ok {
			return &i
		}
	}
	switch i := v.(type) {
	case int64:
		return &i
	case int:
		val := int64(i)
		return &val
	case float64:
		val := int64(i)
		return &val
	}
	return nil
}

func getOptionalInt(m map[string]any, key string) *int {
	v := m[key]
	if v == nil {
		return nil
	}
	// Handle union types
	if union, ok := v.(map[string]any); ok {
		if i, ok := union["int"].(int32); ok {
			val := int(i)
			return &val
		}
	}
	switch i := v.(type) {
	case int:
		return &i
	case int32:
		val := int(i)
		return &val
	case int64:
		val := int(i)
		return &val
	case float64:
		val := int(i)
		return &val
	}
	return nil
}

func getIntMap(m map[string]any, key string) map[int]int64 {
	v := m[key]
	if v == nil {
		return nil
	}
	// Handle union types
	data, ok := v.(map[string]any)
	if !ok {
		if union, ok := v.(map[string]any); ok {
			if mapData, ok := union["map"].(map[string]any); ok {
				data = mapData
			}
		}
	}
	if data == nil {
		return nil
	}

	result := make(map[int]int64)
	for k, val := range data {
		var fieldID int
		fmt.Sscanf(k, "%d", &fieldID)
		switch v := val.(type) {
		case int64:
			result[fieldID] = v
		case float64:
			result[fieldID] = int64(v)
		}
	}
	return result
}

func getBytesMap(m map[string]any, key string) map[int][]byte {
	v := m[key]
	if v == nil {
		return nil
	}
	// Handle union types
	data, ok := v.(map[string]any)
	if !ok {
		if union, ok := v.(map[string]any); ok {
			if mapData, ok := union["map"].(map[string]any); ok {
				data = mapData
			}
		}
	}
	if data == nil {
		return nil
	}

	result := make(map[int][]byte)
	for k, val := range data {
		var fieldID int
		fmt.Sscanf(k, "%d", &fieldID)
		if b, ok := val.([]byte); ok {
			result[fieldID] = b
		}
	}
	return result
}

func getInt64Array(m map[string]any, key string) []int64 {
	v := m[key]
	if v == nil {
		return nil
	}
	// Handle union types
	var arr []any
	if union, ok := v.(map[string]any); ok {
		if a, ok := union["array"].([]any); ok {
			arr = a
		}
	} else if a, ok := v.([]any); ok {
		arr = a
	}
	if arr == nil {
		return nil
	}

	result := make([]int64, len(arr))
	for i, val := range arr {
		switch v := val.(type) {
		case int64:
			result[i] = v
		case float64:
			result[i] = int64(v)
		}
	}
	return result
}

func getIntArray(m map[string]any, key string) []int {
	v := m[key]
	if v == nil {
		return nil
	}
	// Handle union types
	var arr []any
	if union, ok := v.(map[string]any); ok {
		if a, ok := union["array"].([]any); ok {
			arr = a
		}
	} else if a, ok := v.([]any); ok {
		arr = a
	}
	if arr == nil {
		return nil
	}

	result := make([]int, len(arr))
	for i, val := range arr {
		switch v := val.(type) {
		case int:
			result[i] = v
		case int32:
			result[i] = int(v)
		case int64:
			result[i] = int(v)
		case float64:
			result[i] = int(v)
		}
	}
	return result
}

func optionalIntMap(m map[int]int64) any {
	if len(m) == 0 {
		return nil
	}
	result := make(map[string]any)
	for k, v := range m {
		result[fmt.Sprintf("%d", k)] = v
	}
	return goavro.Union("map", result)
}

func optionalBytesMap(m map[int][]byte) any {
	if len(m) == 0 {
		return nil
	}
	result := make(map[string]any)
	for k, v := range m {
		result[fmt.Sprintf("%d", k)] = v
	}
	return goavro.Union("map", result)
}

// SerializeValue serializes a value to bytes for use in bounds.
func SerializeValue(value any, typ Type) ([]byte, error) {
	buf := new(bytes.Buffer)

	switch v := value.(type) {
	case bool:
		if v {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
	case int32:
		binary.Write(buf, binary.LittleEndian, v)
	case int64:
		binary.Write(buf, binary.LittleEndian, v)
	case float32:
		binary.Write(buf, binary.LittleEndian, v)
	case float64:
		binary.Write(buf, binary.LittleEndian, v)
	case string:
		buf.WriteString(v)
	case []byte:
		buf.Write(v)
	default:
		return nil, fmt.Errorf("unsupported type for serialization: %T", value)
	}

	return buf.Bytes(), nil
}

// DeserializeValue deserializes bytes to a value.
func DeserializeValue(data []byte, typ Type) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}

	buf := bytes.NewReader(data)

	switch t := typ.(type) {
	case PrimitiveType:
		switch t.TypeID() {
		case TypeBoolean:
			if data[0] == 0 {
				return false, nil
			}
			return true, nil
		case TypeInt:
			var v int32
			binary.Read(buf, binary.LittleEndian, &v)
			return v, nil
		case TypeLong:
			var v int64
			binary.Read(buf, binary.LittleEndian, &v)
			return v, nil
		case TypeFloat:
			var v float32
			binary.Read(buf, binary.LittleEndian, &v)
			return v, nil
		case TypeDouble:
			var v float64
			binary.Read(buf, binary.LittleEndian, &v)
			return v, nil
		case TypeString:
			return string(data), nil
		case TypeBinary:
			return data, nil
		}
	}

	return data, nil
}

// ReadManifestList reads a manifest list from a reader.
func ReadManifestList(r io.Reader) ([]ManifestFile, error) {
	reader, err := NewManifestListReader(r)
	if err != nil {
		return nil, err
	}
	return reader.Read()
}

// ReadManifest reads a manifest from a reader.
func ReadManifest(r io.Reader) (*Manifest, error) {
	reader, err := NewManifestReader(r)
	if err != nil {
		return nil, err
	}
	return reader.Read()
}
