// Package spec implements the Apache Iceberg table format specification types.
// This package follows the Iceberg spec: https://iceberg.apache.org/spec/
package spec

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// TypeID represents the type identifier for Iceberg primitive types.
type TypeID int

const (
	TypeBoolean TypeID = iota
	TypeInt
	TypeLong
	TypeFloat
	TypeDouble
	TypeDate
	TypeTime
	TypeTimestamp
	TypeTimestampTz
	TypeString
	TypeUUID
	TypeBinary
	TypeFixed
	TypeDecimal
	TypeStruct
	TypeList
	TypeMap
)

// Type represents an Iceberg data type.
type Type interface {
	// TypeID returns the type identifier.
	TypeID() TypeID
	// String returns the string representation of the type.
	String() string
	// Equals checks if two types are equal.
	Equals(other Type) bool
}

// PrimitiveType represents a primitive Iceberg type.
type PrimitiveType struct {
	id TypeID
}

func (t PrimitiveType) TypeID() TypeID { return t.id }
func (t PrimitiveType) Equals(other Type) bool {
	if o, ok := other.(PrimitiveType); ok {
		return t.id == o.id
	}
	return false
}

func (t PrimitiveType) String() string {
	switch t.id {
	case TypeBoolean:
		return "boolean"
	case TypeInt:
		return "int"
	case TypeLong:
		return "long"
	case TypeFloat:
		return "float"
	case TypeDouble:
		return "double"
	case TypeDate:
		return "date"
	case TypeTime:
		return "time"
	case TypeTimestamp:
		return "timestamp"
	case TypeTimestampTz:
		return "timestamptz"
	case TypeString:
		return "string"
	case TypeUUID:
		return "uuid"
	case TypeBinary:
		return "binary"
	default:
		return "unknown"
	}
}

// Primitive type constants
var (
	BooleanType     = PrimitiveType{TypeBoolean}
	IntType         = PrimitiveType{TypeInt}
	LongType        = PrimitiveType{TypeLong}
	FloatType       = PrimitiveType{TypeFloat}
	DoubleType      = PrimitiveType{TypeDouble}
	DateType        = PrimitiveType{TypeDate}
	TimeType        = PrimitiveType{TypeTime}
	TimestampType   = PrimitiveType{TypeTimestamp}
	TimestampTzType = PrimitiveType{TypeTimestampTz}
	StringType      = PrimitiveType{TypeString}
	UUIDType        = PrimitiveType{TypeUUID}
	BinaryType      = PrimitiveType{TypeBinary}
)

// FixedType represents a fixed-length binary type.
type FixedType struct {
	Length int
}

func (t FixedType) TypeID() TypeID { return TypeFixed }
func (t FixedType) String() string { return fmt.Sprintf("fixed[%d]", t.Length) }
func (t FixedType) Equals(other Type) bool {
	if o, ok := other.(FixedType); ok {
		return t.Length == o.Length
	}
	return false
}

// DecimalType represents a decimal type with precision and scale.
type DecimalType struct {
	Precision int
	Scale     int
}

func (t DecimalType) TypeID() TypeID { return TypeDecimal }
func (t DecimalType) String() string { return fmt.Sprintf("decimal(%d, %d)", t.Precision, t.Scale) }
func (t DecimalType) Equals(other Type) bool {
	if o, ok := other.(DecimalType); ok {
		return t.Precision == o.Precision && t.Scale == o.Scale
	}
	return false
}

// NestedField represents a field in a struct, list, or map type.
type NestedField struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Required bool   `json:"required"`
	Type     Type   `json:"type"`
	Doc      string `json:"doc,omitempty"`
}

// StructType represents a struct type with named fields.
type StructType struct {
	Fields []NestedField `json:"fields"`
}

func (t StructType) TypeID() TypeID { return TypeStruct }
func (t StructType) String() string {
	var fields []string
	for _, f := range t.Fields {
		opt := "optional"
		if f.Required {
			opt = "required"
		}
		fields = append(fields, fmt.Sprintf("%d: %s: %s %s", f.ID, f.Name, opt, f.Type.String()))
	}
	return fmt.Sprintf("struct<%s>", strings.Join(fields, ", "))
}
func (t StructType) Equals(other Type) bool {
	o, ok := other.(StructType)
	if !ok || len(t.Fields) != len(o.Fields) {
		return false
	}
	for i := range t.Fields {
		if t.Fields[i].ID != o.Fields[i].ID ||
			t.Fields[i].Name != o.Fields[i].Name ||
			t.Fields[i].Required != o.Fields[i].Required ||
			!t.Fields[i].Type.Equals(o.Fields[i].Type) {
			return false
		}
	}
	return true
}

// Field returns the field with the given ID, or nil if not found.
func (t StructType) Field(id int) *NestedField {
	for i := range t.Fields {
		if t.Fields[i].ID == id {
			return &t.Fields[i]
		}
	}
	return nil
}

// FieldByName returns the field with the given name, or nil if not found.
func (t StructType) FieldByName(name string) *NestedField {
	for i := range t.Fields {
		if t.Fields[i].Name == name {
			return &t.Fields[i]
		}
	}
	return nil
}

// ListType represents a list type.
type ListType struct {
	ElementID       int  `json:"element-id"`
	Element         Type `json:"element"`
	ElementRequired bool `json:"element-required"`
}

func (t ListType) TypeID() TypeID { return TypeList }
func (t ListType) String() string {
	return fmt.Sprintf("list<%s>", t.Element.String())
}
func (t ListType) Equals(other Type) bool {
	if o, ok := other.(ListType); ok {
		return t.ElementID == o.ElementID &&
			t.ElementRequired == o.ElementRequired &&
			t.Element.Equals(o.Element)
	}
	return false
}

// MapType represents a map type.
type MapType struct {
	KeyID         int  `json:"key-id"`
	Key           Type `json:"key"`
	ValueID       int  `json:"value-id"`
	Value         Type `json:"value"`
	ValueRequired bool `json:"value-required"`
}

func (t MapType) TypeID() TypeID { return TypeMap }
func (t MapType) String() string {
	return fmt.Sprintf("map<%s, %s>", t.Key.String(), t.Value.String())
}
func (t MapType) Equals(other Type) bool {
	if o, ok := other.(MapType); ok {
		return t.KeyID == o.KeyID &&
			t.ValueID == o.ValueID &&
			t.ValueRequired == o.ValueRequired &&
			t.Key.Equals(o.Key) &&
			t.Value.Equals(o.Value)
	}
	return false
}

// ParseType parses a type string into a Type.
func ParseType(s string) (Type, error) {
	s = strings.TrimSpace(s)

	// Handle primitive types
	switch s {
	case "boolean":
		return BooleanType, nil
	case "int":
		return IntType, nil
	case "long":
		return LongType, nil
	case "float":
		return FloatType, nil
	case "double":
		return DoubleType, nil
	case "date":
		return DateType, nil
	case "time":
		return TimeType, nil
	case "timestamp":
		return TimestampType, nil
	case "timestamptz":
		return TimestampTzType, nil
	case "string":
		return StringType, nil
	case "uuid":
		return UUIDType, nil
	case "binary":
		return BinaryType, nil
	}

	// Handle parameterized types
	if strings.HasPrefix(s, "fixed[") && strings.HasSuffix(s, "]") {
		length, err := strconv.Atoi(s[6 : len(s)-1])
		if err != nil {
			return nil, fmt.Errorf("invalid fixed type: %s", s)
		}
		return FixedType{Length: length}, nil
	}

	if strings.HasPrefix(s, "decimal(") && strings.HasSuffix(s, ")") {
		parts := strings.Split(s[8:len(s)-1], ",")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid decimal type: %s", s)
		}
		precision, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			return nil, fmt.Errorf("invalid decimal precision: %s", s)
		}
		scale, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil {
			return nil, fmt.Errorf("invalid decimal scale: %s", s)
		}
		return DecimalType{Precision: precision, Scale: scale}, nil
	}

	return nil, fmt.Errorf("unknown type: %s", s)
}

// TypeJSON is used for JSON marshaling/unmarshaling of Type.
type TypeJSON struct {
	Type string `json:"type"`
	// For struct type
	Fields []NestedFieldJSON `json:"fields,omitempty"`
	// For list type
	ElementID       int    `json:"element-id,omitempty"`
	Element         string `json:"element,omitempty"`
	ElementRequired bool   `json:"element-required,omitempty"`
	// For map type
	KeyID         int    `json:"key-id,omitempty"`
	Key           string `json:"key,omitempty"`
	ValueID       int    `json:"value-id,omitempty"`
	Value         string `json:"value,omitempty"`
	ValueRequired bool   `json:"value-required,omitempty"`
}

// NestedFieldJSON is used for JSON marshaling/unmarshaling of NestedField.
type NestedFieldJSON struct {
	ID       int             `json:"id"`
	Name     string          `json:"name"`
	Required bool            `json:"required"`
	Type     json.RawMessage `json:"type"`
	Doc      string          `json:"doc,omitempty"`
}
