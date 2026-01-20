package spec

import (
	"encoding/json"
	"fmt"
)

// Transform represents a partition transform function.
type Transform string

const (
	TransformIdentity Transform = "identity"
	TransformYear     Transform = "year"
	TransformMonth    Transform = "month"
	TransformDay      Transform = "day"
	TransformHour     Transform = "hour"
	TransformVoid     Transform = "void"
	// Bucket and truncate transforms have parameters
)

// PartitionField represents a field in a partition spec.
type PartitionField struct {
	SourceID  int       `json:"source-id"`
	FieldID   int       `json:"field-id"`
	Name      string    `json:"name"`
	Transform Transform `json:"transform"`
}

// PartitionSpec represents a partition specification.
type PartitionSpec struct {
	SpecID int              `json:"spec-id"`
	Fields []PartitionField `json:"fields"`
}

// NewPartitionSpec creates a new partition spec with the given fields.
func NewPartitionSpec(specID int, fields []PartitionField) *PartitionSpec {
	return &PartitionSpec{
		SpecID: specID,
		Fields: fields,
	}
}

// UnpartitionedSpec returns an unpartitioned spec.
func UnpartitionedSpec() *PartitionSpec {
	return &PartitionSpec{
		SpecID: 0,
		Fields: []PartitionField{},
	}
}

// IsUnpartitioned returns true if this is an unpartitioned spec.
func (p *PartitionSpec) IsUnpartitioned() bool {
	return len(p.Fields) == 0
}

// NumFields returns the number of partition fields.
func (p *PartitionSpec) NumFields() int {
	return len(p.Fields)
}

// Field returns the partition field at the given index.
func (p *PartitionSpec) Field(idx int) *PartitionField {
	if idx < 0 || idx >= len(p.Fields) {
		return nil
	}
	return &p.Fields[idx]
}

// LastFieldID returns the highest field ID in the partition spec.
func (p *PartitionSpec) LastFieldID() int {
	maxID := 0
	for _, f := range p.Fields {
		if f.FieldID > maxID {
			maxID = f.FieldID
		}
	}
	return maxID
}

// MarshalJSON implements json.Marshaler.
func (p *PartitionSpec) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		SpecID int              `json:"spec-id"`
		Fields []PartitionField `json:"fields"`
	}{
		SpecID: p.SpecID,
		Fields: p.Fields,
	})
}

// BucketTransform creates a bucket transform with the given number of buckets.
func BucketTransform(numBuckets int) Transform {
	return Transform(fmt.Sprintf("bucket[%d]", numBuckets))
}

// TruncateTransform creates a truncate transform with the given width.
func TruncateTransform(width int) Transform {
	return Transform(fmt.Sprintf("truncate[%d]", width))
}

// PartitionSpecBuilder helps build partition specs.
type PartitionSpecBuilder struct {
	specID int
	fields []PartitionField
	nextID int
}

// NewPartitionSpecBuilder creates a new partition spec builder.
func NewPartitionSpecBuilder(specID int) *PartitionSpecBuilder {
	return &PartitionSpecBuilder{
		specID: specID,
		fields: []PartitionField{},
		nextID: 1000, // Start field IDs at 1000 for partition fields
	}
}

// Identity adds an identity partition field.
func (b *PartitionSpecBuilder) Identity(sourceID int, name string) *PartitionSpecBuilder {
	b.fields = append(b.fields, PartitionField{
		SourceID:  sourceID,
		FieldID:   b.nextID,
		Name:      name,
		Transform: TransformIdentity,
	})
	b.nextID++
	return b
}

// Year adds a year partition field.
func (b *PartitionSpecBuilder) Year(sourceID int, name string) *PartitionSpecBuilder {
	b.fields = append(b.fields, PartitionField{
		SourceID:  sourceID,
		FieldID:   b.nextID,
		Name:      name,
		Transform: TransformYear,
	})
	b.nextID++
	return b
}

// Month adds a month partition field.
func (b *PartitionSpecBuilder) Month(sourceID int, name string) *PartitionSpecBuilder {
	b.fields = append(b.fields, PartitionField{
		SourceID:  sourceID,
		FieldID:   b.nextID,
		Name:      name,
		Transform: TransformMonth,
	})
	b.nextID++
	return b
}

// Day adds a day partition field.
func (b *PartitionSpecBuilder) Day(sourceID int, name string) *PartitionSpecBuilder {
	b.fields = append(b.fields, PartitionField{
		SourceID:  sourceID,
		FieldID:   b.nextID,
		Name:      name,
		Transform: TransformDay,
	})
	b.nextID++
	return b
}

// Hour adds an hour partition field.
func (b *PartitionSpecBuilder) Hour(sourceID int, name string) *PartitionSpecBuilder {
	b.fields = append(b.fields, PartitionField{
		SourceID:  sourceID,
		FieldID:   b.nextID,
		Name:      name,
		Transform: TransformHour,
	})
	b.nextID++
	return b
}

// Bucket adds a bucket partition field.
func (b *PartitionSpecBuilder) Bucket(sourceID int, name string, numBuckets int) *PartitionSpecBuilder {
	b.fields = append(b.fields, PartitionField{
		SourceID:  sourceID,
		FieldID:   b.nextID,
		Name:      name,
		Transform: BucketTransform(numBuckets),
	})
	b.nextID++
	return b
}

// Truncate adds a truncate partition field.
func (b *PartitionSpecBuilder) Truncate(sourceID int, name string, width int) *PartitionSpecBuilder {
	b.fields = append(b.fields, PartitionField{
		SourceID:  sourceID,
		FieldID:   b.nextID,
		Name:      name,
		Transform: TruncateTransform(width),
	})
	b.nextID++
	return b
}

// Build returns the partition spec.
func (b *PartitionSpecBuilder) Build() *PartitionSpec {
	return NewPartitionSpec(b.specID, b.fields)
}
