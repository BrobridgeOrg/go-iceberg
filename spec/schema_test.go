package spec

import (
	"testing"
)

func TestNewSchema(t *testing.T) {
	fields := []NestedField{
		{ID: 1, Name: "id", Type: LongType, Required: true},
		{ID: 2, Name: "name", Type: StringType, Required: true},
		{ID: 3, Name: "email", Type: StringType, Required: false},
	}

	schema := NewSchema(1, fields)

	if schema.SchemaID != 1 {
		t.Errorf("SchemaID = %d, want 1", schema.SchemaID)
	}
	if len(schema.Fields) != 3 {
		t.Errorf("Fields length = %d, want 3", len(schema.Fields))
	}
}

func TestSchemaFields(t *testing.T) {
	fields := []NestedField{
		{ID: 1, Name: "id", Type: LongType, Required: true},
		{ID: 2, Name: "name", Type: StringType, Required: true},
		{ID: 3, Name: "email", Type: StringType, Required: false},
	}
	schema := NewSchema(1, fields)

	// Verify field names
	if schema.Fields[0].Name != "id" {
		t.Errorf("Fields[0].Name = %s, want id", schema.Fields[0].Name)
	}
	if schema.Fields[1].Name != "name" {
		t.Errorf("Fields[1].Name = %s, want name", schema.Fields[1].Name)
	}
	if schema.Fields[2].Name != "email" {
		t.Errorf("Fields[2].Name = %s, want email", schema.Fields[2].Name)
	}

	// Verify field types
	if !schema.Fields[0].Type.Equals(LongType) {
		t.Error("Fields[0].Type should be LongType")
	}
	if !schema.Fields[1].Type.Equals(StringType) {
		t.Error("Fields[1].Type should be StringType")
	}

	// Verify required
	if !schema.Fields[0].Required {
		t.Error("Fields[0] should be required")
	}
	if schema.Fields[2].Required {
		t.Error("Fields[2] should not be required")
	}
}

func TestSchemaStruct(t *testing.T) {
	fields := []NestedField{
		{ID: 1, Name: "id", Type: LongType, Required: true},
		{ID: 2, Name: "name", Type: StringType, Required: true},
	}

	schema := &Schema{
		SchemaID: 1,
		Fields:   fields,
	}

	if schema.SchemaID != 1 {
		t.Errorf("SchemaID = %d, want 1", schema.SchemaID)
	}
	if len(schema.Fields) != 2 {
		t.Errorf("Fields length = %d, want 2", len(schema.Fields))
	}
}

func TestSchemaNestedTypes(t *testing.T) {
	// Schema with nested struct type
	addressType := StructType{
		Fields: []NestedField{
			{ID: 100, Name: "street", Type: StringType, Required: true},
			{ID: 101, Name: "city", Type: StringType, Required: true},
			{ID: 102, Name: "zip", Type: StringType, Required: false},
		},
	}

	fields := []NestedField{
		{ID: 1, Name: "id", Type: LongType, Required: true},
		{ID: 2, Name: "name", Type: StringType, Required: true},
		{ID: 3, Name: "address", Type: addressType, Required: false},
	}

	schema := NewSchema(1, fields)

	if len(schema.Fields) != 3 {
		t.Errorf("Fields length = %d, want 3", len(schema.Fields))
	}

	// Verify nested struct
	addressField := schema.Fields[2]
	if addressField.Name != "address" {
		t.Errorf("Fields[2].Name = %s, want address", addressField.Name)
	}

	st, ok := addressField.Type.(StructType)
	if !ok {
		t.Error("Fields[2].Type should be StructType")
		return
	}

	if len(st.Fields) != 3 {
		t.Errorf("Address struct fields length = %d, want 3", len(st.Fields))
	}
}

func TestSchemaWithListType(t *testing.T) {
	tagsType := ListType{
		ElementID:       100,
		Element:         StringType,
		ElementRequired: true,
	}

	fields := []NestedField{
		{ID: 1, Name: "id", Type: LongType, Required: true},
		{ID: 2, Name: "tags", Type: tagsType, Required: false},
	}

	schema := NewSchema(1, fields)

	tagsField := schema.Fields[1]
	lt, ok := tagsField.Type.(ListType)
	if !ok {
		t.Error("Fields[1].Type should be ListType")
		return
	}

	if !lt.Element.Equals(StringType) {
		t.Error("List element type should be StringType")
	}
}

func TestSchemaWithMapType(t *testing.T) {
	propsType := MapType{
		KeyID:         100,
		Key:           StringType,
		ValueID:       101,
		Value:         StringType,
		ValueRequired: false,
	}

	fields := []NestedField{
		{ID: 1, Name: "id", Type: LongType, Required: true},
		{ID: 2, Name: "properties", Type: propsType, Required: false},
	}

	schema := NewSchema(1, fields)

	propsField := schema.Fields[1]
	mt, ok := propsField.Type.(MapType)
	if !ok {
		t.Error("Fields[1].Type should be MapType")
		return
	}

	if !mt.Key.Equals(StringType) {
		t.Error("Map key type should be StringType")
	}
	if !mt.Value.Equals(StringType) {
		t.Error("Map value type should be StringType")
	}
}
