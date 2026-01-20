package spec

import (
	"encoding/json"
	"fmt"
)

// Schema represents an Iceberg table schema.
// A schema is a struct type that defines the columns of a table.
type Schema struct {
	SchemaID        int           `json:"schema-id"`
	IdentifierField []int         `json:"identifier-field-ids,omitempty"`
	Fields          []NestedField `json:"fields"`
}

// NewSchema creates a new schema with the given fields.
func NewSchema(schemaID int, fields []NestedField) *Schema {
	return &Schema{
		SchemaID: schemaID,
		Fields:   fields,
	}
}

// NewSchemaWithIdentifiers creates a new schema with identifier fields.
func NewSchemaWithIdentifiers(schemaID int, identifierFieldIDs []int, fields []NestedField) *Schema {
	return &Schema{
		SchemaID:        schemaID,
		IdentifierField: identifierFieldIDs,
		Fields:          fields,
	}
}

// AsStruct returns the schema as a struct type.
func (s *Schema) AsStruct() StructType {
	return StructType{Fields: s.Fields}
}

// Field returns the field with the given ID, or nil if not found.
func (s *Schema) Field(id int) *NestedField {
	return s.AsStruct().Field(id)
}

// FieldByName returns the field with the given name, or nil if not found.
func (s *Schema) FieldByName(name string) *NestedField {
	return s.AsStruct().FieldByName(name)
}

// NumFields returns the number of fields in the schema.
func (s *Schema) NumFields() int {
	return len(s.Fields)
}

// HighestFieldID returns the highest field ID in the schema.
func (s *Schema) HighestFieldID() int {
	highest := 0
	for _, f := range s.Fields {
		if f.ID > highest {
			highest = f.ID
		}
		highest = maxFieldID(f.Type, highest)
	}
	return highest
}

func maxFieldID(t Type, current int) int {
	switch v := t.(type) {
	case StructType:
		for _, f := range v.Fields {
			if f.ID > current {
				current = f.ID
			}
			current = maxFieldID(f.Type, current)
		}
	case ListType:
		if v.ElementID > current {
			current = v.ElementID
		}
		current = maxFieldID(v.Element, current)
	case MapType:
		if v.KeyID > current {
			current = v.KeyID
		}
		if v.ValueID > current {
			current = v.ValueID
		}
		current = maxFieldID(v.Key, current)
		current = maxFieldID(v.Value, current)
	}
	return current
}

// Equals checks if two schemas are equal.
func (s *Schema) Equals(other *Schema) bool {
	if s.SchemaID != other.SchemaID {
		return false
	}
	if len(s.Fields) != len(other.Fields) {
		return false
	}
	for i := range s.Fields {
		if s.Fields[i].ID != other.Fields[i].ID ||
			s.Fields[i].Name != other.Fields[i].Name ||
			s.Fields[i].Required != other.Fields[i].Required ||
			!s.Fields[i].Type.Equals(other.Fields[i].Type) {
			return false
		}
	}
	return true
}

// SchemaJSON is used for JSON marshaling/unmarshaling of Schema.
type SchemaJSON struct {
	SchemaID        int               `json:"schema-id"`
	Type            string            `json:"type"`
	Fields          []NestedFieldJSON `json:"fields"`
	IdentifierField []int             `json:"identifier-field-ids,omitempty"`
}

// MarshalJSON implements json.Marshaler.
func (s *Schema) MarshalJSON() ([]byte, error) {
	fields := make([]NestedFieldJSON, len(s.Fields))
	for i, f := range s.Fields {
		typeBytes, err := marshalType(f.Type)
		if err != nil {
			return nil, err
		}
		fields[i] = NestedFieldJSON{
			ID:       f.ID,
			Name:     f.Name,
			Required: f.Required,
			Type:     typeBytes,
			Doc:      f.Doc,
		}
	}

	return json.Marshal(SchemaJSON{
		SchemaID:        s.SchemaID,
		Type:            "struct",
		Fields:          fields,
		IdentifierField: s.IdentifierField,
	})
}

// UnmarshalJSON implements json.Unmarshaler.
func (s *Schema) UnmarshalJSON(data []byte) error {
	var sj SchemaJSON
	if err := json.Unmarshal(data, &sj); err != nil {
		return err
	}

	s.SchemaID = sj.SchemaID
	s.IdentifierField = sj.IdentifierField
	s.Fields = make([]NestedField, len(sj.Fields))

	for i, f := range sj.Fields {
		t, err := unmarshalType(f.Type)
		if err != nil {
			return fmt.Errorf("failed to unmarshal field %s type: %w", f.Name, err)
		}
		s.Fields[i] = NestedField{
			ID:       f.ID,
			Name:     f.Name,
			Required: f.Required,
			Type:     t,
			Doc:      f.Doc,
		}
	}

	return nil
}

// marshalType marshals a Type to JSON.
func marshalType(t Type) (json.RawMessage, error) {
	switch v := t.(type) {
	case PrimitiveType:
		return json.Marshal(v.String())
	case FixedType:
		return json.Marshal(v.String())
	case DecimalType:
		return json.Marshal(v.String())
	case StructType:
		fields := make([]NestedFieldJSON, len(v.Fields))
		for i, f := range v.Fields {
			typeBytes, err := marshalType(f.Type)
			if err != nil {
				return nil, err
			}
			fields[i] = NestedFieldJSON{
				ID:       f.ID,
				Name:     f.Name,
				Required: f.Required,
				Type:     typeBytes,
				Doc:      f.Doc,
			}
		}
		return json.Marshal(map[string]interface{}{
			"type":   "struct",
			"fields": fields,
		})
	case ListType:
		elementBytes, err := marshalType(v.Element)
		if err != nil {
			return nil, err
		}
		return json.Marshal(map[string]interface{}{
			"type":             "list",
			"element-id":       v.ElementID,
			"element":          json.RawMessage(elementBytes),
			"element-required": v.ElementRequired,
		})
	case MapType:
		keyBytes, err := marshalType(v.Key)
		if err != nil {
			return nil, err
		}
		valueBytes, err := marshalType(v.Value)
		if err != nil {
			return nil, err
		}
		return json.Marshal(map[string]interface{}{
			"type":           "map",
			"key-id":         v.KeyID,
			"key":            json.RawMessage(keyBytes),
			"value-id":       v.ValueID,
			"value":          json.RawMessage(valueBytes),
			"value-required": v.ValueRequired,
		})
	default:
		return nil, fmt.Errorf("unknown type: %T", t)
	}
}

// unmarshalType unmarshals a Type from JSON.
func unmarshalType(data json.RawMessage) (Type, error) {
	// Try to unmarshal as string (primitive type)
	var typeStr string
	if err := json.Unmarshal(data, &typeStr); err == nil {
		return ParseType(typeStr)
	}

	// Try to unmarshal as object (complex type)
	var typeObj map[string]json.RawMessage
	if err := json.Unmarshal(data, &typeObj); err != nil {
		return nil, fmt.Errorf("invalid type JSON: %s", string(data))
	}

	typeField, ok := typeObj["type"]
	if !ok {
		return nil, fmt.Errorf("missing type field")
	}

	var typeName string
	if err := json.Unmarshal(typeField, &typeName); err != nil {
		return nil, fmt.Errorf("invalid type field: %w", err)
	}

	switch typeName {
	case "struct":
		var fieldsJSON []NestedFieldJSON
		if err := json.Unmarshal(typeObj["fields"], &fieldsJSON); err != nil {
			return nil, fmt.Errorf("invalid struct fields: %w", err)
		}
		fields := make([]NestedField, len(fieldsJSON))
		for i, f := range fieldsJSON {
			t, err := unmarshalType(f.Type)
			if err != nil {
				return nil, err
			}
			fields[i] = NestedField{
				ID:       f.ID,
				Name:     f.Name,
				Required: f.Required,
				Type:     t,
				Doc:      f.Doc,
			}
		}
		return StructType{Fields: fields}, nil

	case "list":
		var elementID int
		var elementRequired bool
		if err := json.Unmarshal(typeObj["element-id"], &elementID); err != nil {
			return nil, fmt.Errorf("invalid list element-id: %w", err)
		}
		if req, ok := typeObj["element-required"]; ok {
			if err := json.Unmarshal(req, &elementRequired); err != nil {
				return nil, fmt.Errorf("invalid list element-required: %w", err)
			}
		}
		element, err := unmarshalType(typeObj["element"])
		if err != nil {
			return nil, fmt.Errorf("invalid list element type: %w", err)
		}
		return ListType{
			ElementID:       elementID,
			Element:         element,
			ElementRequired: elementRequired,
		}, nil

	case "map":
		var keyID, valueID int
		var valueRequired bool
		if err := json.Unmarshal(typeObj["key-id"], &keyID); err != nil {
			return nil, fmt.Errorf("invalid map key-id: %w", err)
		}
		if err := json.Unmarshal(typeObj["value-id"], &valueID); err != nil {
			return nil, fmt.Errorf("invalid map value-id: %w", err)
		}
		if req, ok := typeObj["value-required"]; ok {
			if err := json.Unmarshal(req, &valueRequired); err != nil {
				return nil, fmt.Errorf("invalid map value-required: %w", err)
			}
		}
		key, err := unmarshalType(typeObj["key"])
		if err != nil {
			return nil, fmt.Errorf("invalid map key type: %w", err)
		}
		value, err := unmarshalType(typeObj["value"])
		if err != nil {
			return nil, fmt.Errorf("invalid map value type: %w", err)
		}
		return MapType{
			KeyID:         keyID,
			Key:           key,
			ValueID:       valueID,
			Value:         value,
			ValueRequired: valueRequired,
		}, nil

	default:
		// Try parsing as primitive
		return ParseType(typeName)
	}
}
