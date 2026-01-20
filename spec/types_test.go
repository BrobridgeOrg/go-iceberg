package spec

import (
	"testing"
)

func TestPrimitiveTypes(t *testing.T) {
	tests := []struct {
		name     string
		typ      Type
		expected TypeID
	}{
		{"BooleanType", BooleanType, TypeBoolean},
		{"IntType", IntType, TypeInt},
		{"LongType", LongType, TypeLong},
		{"FloatType", FloatType, TypeFloat},
		{"DoubleType", DoubleType, TypeDouble},
		{"StringType", StringType, TypeString},
		{"BinaryType", BinaryType, TypeBinary},
		{"DateType", DateType, TypeDate},
		{"TimeType", TimeType, TypeTime},
		{"TimestampType", TimestampType, TypeTimestamp},
		{"TimestampTzType", TimestampTzType, TypeTimestampTz},
		{"UUIDType", UUIDType, TypeUUID},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.typ.TypeID() != tt.expected {
				t.Errorf("%s.TypeID() = %v, want %v", tt.name, tt.typ.TypeID(), tt.expected)
			}
		})
	}
}

func TestPrimitiveTypeString(t *testing.T) {
	tests := []struct {
		typ      PrimitiveType
		expected string
	}{
		{BooleanType, "boolean"},
		{IntType, "int"},
		{LongType, "long"},
		{FloatType, "float"},
		{DoubleType, "double"},
		{StringType, "string"},
		{BinaryType, "binary"},
		{DateType, "date"},
		{TimeType, "time"},
		{TimestampType, "timestamp"},
		{TimestampTzType, "timestamptz"},
		{UUIDType, "uuid"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if tt.typ.String() != tt.expected {
				t.Errorf("String() = %s, want %s", tt.typ.String(), tt.expected)
			}
		})
	}
}

func TestPrimitiveTypeEquals(t *testing.T) {
	if !BooleanType.Equals(BooleanType) {
		t.Error("BooleanType should equal itself")
	}

	if BooleanType.Equals(IntType) {
		t.Error("BooleanType should not equal IntType")
	}

	if BooleanType.Equals(StringType) {
		t.Error("BooleanType should not equal StringType")
	}
}

func TestDecimalType(t *testing.T) {
	dt := DecimalType{Precision: 10, Scale: 2}

	if dt.Precision != 10 {
		t.Errorf("Precision = %d, want 10", dt.Precision)
	}
	if dt.Scale != 2 {
		t.Errorf("Scale = %d, want 2", dt.Scale)
	}

	if dt.TypeID() != TypeDecimal {
		t.Errorf("TypeID = %v, want TypeDecimal", dt.TypeID())
	}
}

func TestFixedType(t *testing.T) {
	ft := FixedType{Length: 16}

	if ft.Length != 16 {
		t.Errorf("Length = %d, want 16", ft.Length)
	}

	if ft.TypeID() != TypeFixed {
		t.Errorf("TypeID = %v, want TypeFixed", ft.TypeID())
	}
}

func TestListType(t *testing.T) {
	lt := ListType{
		ElementID:       1,
		Element:         StringType,
		ElementRequired: true,
	}

	if lt.ElementID != 1 {
		t.Errorf("ElementID = %d, want 1", lt.ElementID)
	}
	if !lt.Element.Equals(StringType) {
		t.Errorf("Element type mismatch")
	}
	if !lt.ElementRequired {
		t.Error("ElementRequired should be true")
	}
	if lt.TypeID() != TypeList {
		t.Errorf("TypeID = %v, want TypeList", lt.TypeID())
	}
}

func TestMapType(t *testing.T) {
	mt := MapType{
		KeyID:         1,
		Key:           StringType,
		ValueID:       2,
		Value:         LongType,
		ValueRequired: false,
	}

	if mt.KeyID != 1 {
		t.Errorf("KeyID = %d, want 1", mt.KeyID)
	}
	if !mt.Key.Equals(StringType) {
		t.Errorf("Key type mismatch")
	}
	if mt.ValueID != 2 {
		t.Errorf("ValueID = %d, want 2", mt.ValueID)
	}
	if !mt.Value.Equals(LongType) {
		t.Errorf("Value type mismatch")
	}
	if mt.ValueRequired {
		t.Error("ValueRequired should be false")
	}
	if mt.TypeID() != TypeMap {
		t.Errorf("TypeID = %v, want TypeMap", mt.TypeID())
	}
}

func TestStructType(t *testing.T) {
	st := StructType{
		Fields: []NestedField{
			{ID: 1, Name: "id", Type: LongType, Required: true},
			{ID: 2, Name: "name", Type: StringType, Required: false},
		},
	}

	if len(st.Fields) != 2 {
		t.Errorf("Fields length = %d, want 2", len(st.Fields))
	}

	if st.Fields[0].Name != "id" {
		t.Errorf("Fields[0].Name = %s, want id", st.Fields[0].Name)
	}
	if st.Fields[1].Name != "name" {
		t.Errorf("Fields[1].Name = %s, want name", st.Fields[1].Name)
	}

	if st.TypeID() != TypeStruct {
		t.Errorf("TypeID = %v, want TypeStruct", st.TypeID())
	}
}

func TestNestedField(t *testing.T) {
	field := NestedField{
		ID:       1,
		Name:     "test_field",
		Type:     StringType,
		Required: true,
		Doc:      "A test field",
	}

	if field.ID != 1 {
		t.Errorf("ID = %d, want 1", field.ID)
	}
	if field.Name != "test_field" {
		t.Errorf("Name = %s, want test_field", field.Name)
	}
	if !field.Type.Equals(StringType) {
		t.Error("Type should be StringType")
	}
	if !field.Required {
		t.Error("Required should be true")
	}
	if field.Doc != "A test field" {
		t.Errorf("Doc = %s, want 'A test field'", field.Doc)
	}
}
