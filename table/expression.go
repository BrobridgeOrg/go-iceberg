package table

import (
	"fmt"
)

// ExprOp represents an expression operator.
type ExprOp int

const (
	OpAnd ExprOp = iota
	OpOr
	OpNot
	OpEq
	OpNotEq
	OpLt
	OpLte
	OpGt
	OpGte
	OpIn
	OpNotIn
	OpIsNull
	OpNotNull
	OpStartsWith
	OpNotStartsWith
)

// String returns the string representation of the operator.
func (op ExprOp) String() string {
	switch op {
	case OpAnd:
		return "AND"
	case OpOr:
		return "OR"
	case OpNot:
		return "NOT"
	case OpEq:
		return "="
	case OpNotEq:
		return "!="
	case OpLt:
		return "<"
	case OpLte:
		return "<="
	case OpGt:
		return ">"
	case OpGte:
		return ">="
	case OpIn:
		return "IN"
	case OpNotIn:
		return "NOT IN"
	case OpIsNull:
		return "IS NULL"
	case OpNotNull:
		return "IS NOT NULL"
	case OpStartsWith:
		return "STARTS WITH"
	case OpNotStartsWith:
		return "NOT STARTS WITH"
	default:
		return "UNKNOWN"
	}
}

// Expression represents a filter expression.
type Expression struct {
	Op       ExprOp
	Column   string
	Value    any
	Values   []any
	Children []*Expression
}

// String returns a string representation of the expression.
func (e *Expression) String() string {
	if e == nil {
		return "nil"
	}

	switch e.Op {
	case OpAnd, OpOr:
		if len(e.Children) == 0 {
			return ""
		}
		result := fmt.Sprintf("(%s", e.Children[0].String())
		for i := 1; i < len(e.Children); i++ {
			result += fmt.Sprintf(" %s %s", e.Op.String(), e.Children[i].String())
		}
		return result + ")"
	case OpNot:
		if len(e.Children) > 0 {
			return fmt.Sprintf("NOT %s", e.Children[0].String())
		}
		return "NOT nil"
	case OpIn:
		return fmt.Sprintf("%s IN %v", e.Column, e.Values)
	case OpNotIn:
		return fmt.Sprintf("%s NOT IN %v", e.Column, e.Values)
	case OpIsNull:
		return fmt.Sprintf("%s IS NULL", e.Column)
	case OpNotNull:
		return fmt.Sprintf("%s IS NOT NULL", e.Column)
	default:
		return fmt.Sprintf("%s %s %v", e.Column, e.Op.String(), e.Value)
	}
}

// ExprBuilder helps build filter expressions.
type ExprBuilder struct {
	column string
}

// Col creates a new expression builder for the given column.
func Col(name string) *ExprBuilder {
	return &ExprBuilder{column: name}
}

// Eq creates an equality expression.
func (b *ExprBuilder) Eq(value any) *Expression {
	return &Expression{
		Op:     OpEq,
		Column: b.column,
		Value:  value,
	}
}

// NotEq creates a not-equal expression.
func (b *ExprBuilder) NotEq(value any) *Expression {
	return &Expression{
		Op:     OpNotEq,
		Column: b.column,
		Value:  value,
	}
}

// Lt creates a less-than expression.
func (b *ExprBuilder) Lt(value any) *Expression {
	return &Expression{
		Op:     OpLt,
		Column: b.column,
		Value:  value,
	}
}

// Lte creates a less-than-or-equal expression.
func (b *ExprBuilder) Lte(value any) *Expression {
	return &Expression{
		Op:     OpLte,
		Column: b.column,
		Value:  value,
	}
}

// Gt creates a greater-than expression.
func (b *ExprBuilder) Gt(value any) *Expression {
	return &Expression{
		Op:     OpGt,
		Column: b.column,
		Value:  value,
	}
}

// Gte creates a greater-than-or-equal expression.
func (b *ExprBuilder) Gte(value any) *Expression {
	return &Expression{
		Op:     OpGte,
		Column: b.column,
		Value:  value,
	}
}

// In creates an IN expression.
func (b *ExprBuilder) In(values ...any) *Expression {
	return &Expression{
		Op:     OpIn,
		Column: b.column,
		Values: values,
	}
}

// NotIn creates a NOT IN expression.
func (b *ExprBuilder) NotIn(values ...any) *Expression {
	return &Expression{
		Op:     OpNotIn,
		Column: b.column,
		Values: values,
	}
}

// IsNull creates an IS NULL expression.
func (b *ExprBuilder) IsNull() *Expression {
	return &Expression{
		Op:     OpIsNull,
		Column: b.column,
	}
}

// IsNotNull creates an IS NOT NULL expression.
func (b *ExprBuilder) IsNotNull() *Expression {
	return &Expression{
		Op:     OpNotNull,
		Column: b.column,
	}
}

// StartsWith creates a STARTS WITH expression.
func (b *ExprBuilder) StartsWith(prefix string) *Expression {
	return &Expression{
		Op:     OpStartsWith,
		Column: b.column,
		Value:  prefix,
	}
}

// NotStartsWith creates a NOT STARTS WITH expression.
func (b *ExprBuilder) NotStartsWith(prefix string) *Expression {
	return &Expression{
		Op:     OpNotStartsWith,
		Column: b.column,
		Value:  prefix,
	}
}

// And combines expressions with AND.
func And(exprs ...*Expression) *Expression {
	return &Expression{
		Op:       OpAnd,
		Children: exprs,
	}
}

// Or combines expressions with OR.
func Or(exprs ...*Expression) *Expression {
	return &Expression{
		Op:       OpOr,
		Children: exprs,
	}
}

// Not negates an expression.
func Not(expr *Expression) *Expression {
	return &Expression{
		Op:       OpNot,
		Children: []*Expression{expr},
	}
}

// Eq is a shorthand for creating an equality expression.
func Eq(column string, value any) *Expression {
	return Col(column).Eq(value)
}

// NotEq is a shorthand for creating a not-equal expression.
func NotEq(column string, value any) *Expression {
	return Col(column).NotEq(value)
}

// Lt is a shorthand for creating a less-than expression.
func Lt(column string, value any) *Expression {
	return Col(column).Lt(value)
}

// Lte is a shorthand for creating a less-than-or-equal expression.
func Lte(column string, value any) *Expression {
	return Col(column).Lte(value)
}

// Gt is a shorthand for creating a greater-than expression.
func Gt(column string, value any) *Expression {
	return Col(column).Gt(value)
}

// Gte is a shorthand for creating a greater-than-or-equal expression.
func Gte(column string, value any) *Expression {
	return Col(column).Gte(value)
}

// In is a shorthand for creating an IN expression.
func In(column string, values ...any) *Expression {
	return Col(column).In(values...)
}

// IsNull is a shorthand for creating an IS NULL expression.
func IsNull(column string) *Expression {
	return Col(column).IsNull()
}

// IsNotNull is a shorthand for creating an IS NOT NULL expression.
func IsNotNull(column string) *Expression {
	return Col(column).IsNotNull()
}

// Between creates a BETWEEN expression (column >= lower AND column <= upper).
func Between(column string, lower, upper any) *Expression {
	return And(
		Col(column).Gte(lower),
		Col(column).Lte(upper),
	)
}

// ExpressionVisitor defines an interface for visiting expressions.
type ExpressionVisitor interface {
	VisitAnd(children []*Expression) any
	VisitOr(children []*Expression) any
	VisitNot(child *Expression) any
	VisitEq(column string, value any) any
	VisitNotEq(column string, value any) any
	VisitLt(column string, value any) any
	VisitLte(column string, value any) any
	VisitGt(column string, value any) any
	VisitGte(column string, value any) any
	VisitIn(column string, values []any) any
	VisitNotIn(column string, values []any) any
	VisitIsNull(column string) any
	VisitIsNotNull(column string) any
}

// Visit dispatches to the appropriate visitor method.
func (e *Expression) Visit(visitor ExpressionVisitor) any {
	switch e.Op {
	case OpAnd:
		return visitor.VisitAnd(e.Children)
	case OpOr:
		return visitor.VisitOr(e.Children)
	case OpNot:
		if len(e.Children) > 0 {
			return visitor.VisitNot(e.Children[0])
		}
		return nil
	case OpEq:
		return visitor.VisitEq(e.Column, e.Value)
	case OpNotEq:
		return visitor.VisitNotEq(e.Column, e.Value)
	case OpLt:
		return visitor.VisitLt(e.Column, e.Value)
	case OpLte:
		return visitor.VisitLte(e.Column, e.Value)
	case OpGt:
		return visitor.VisitGt(e.Column, e.Value)
	case OpGte:
		return visitor.VisitGte(e.Column, e.Value)
	case OpIn:
		return visitor.VisitIn(e.Column, e.Values)
	case OpNotIn:
		return visitor.VisitNotIn(e.Column, e.Values)
	case OpIsNull:
		return visitor.VisitIsNull(e.Column)
	case OpNotNull:
		return visitor.VisitIsNotNull(e.Column)
	default:
		return nil
	}
}

// Clone creates a deep copy of the expression.
func (e *Expression) Clone() *Expression {
	if e == nil {
		return nil
	}

	clone := &Expression{
		Op:     e.Op,
		Column: e.Column,
		Value:  e.Value,
	}

	if e.Values != nil {
		clone.Values = make([]any, len(e.Values))
		copy(clone.Values, e.Values)
	}

	if e.Children != nil {
		clone.Children = make([]*Expression, len(e.Children))
		for i, child := range e.Children {
			clone.Children[i] = child.Clone()
		}
	}

	return clone
}

// Simplify simplifies the expression by removing redundant nodes.
func (e *Expression) Simplify() *Expression {
	if e == nil {
		return nil
	}

	switch e.Op {
	case OpAnd, OpOr:
		if len(e.Children) == 0 {
			return nil
		}
		if len(e.Children) == 1 {
			return e.Children[0].Simplify()
		}
		simplified := make([]*Expression, 0, len(e.Children))
		for _, child := range e.Children {
			s := child.Simplify()
			if s != nil {
				simplified = append(simplified, s)
			}
		}
		if len(simplified) == 0 {
			return nil
		}
		if len(simplified) == 1 {
			return simplified[0]
		}
		return &Expression{
			Op:       e.Op,
			Children: simplified,
		}
	case OpNot:
		if len(e.Children) == 0 {
			return nil
		}
		child := e.Children[0].Simplify()
		if child == nil {
			return nil
		}
		// Double negation
		if child.Op == OpNot && len(child.Children) > 0 {
			return child.Children[0].Simplify()
		}
		return &Expression{
			Op:       OpNot,
			Children: []*Expression{child},
		}
	default:
		return e
	}
}

// GetReferencedColumns returns all columns referenced by the expression.
func (e *Expression) GetReferencedColumns() []string {
	if e == nil {
		return nil
	}

	columns := make(map[string]bool)
	e.collectColumns(columns)

	result := make([]string, 0, len(columns))
	for col := range columns {
		result = append(result, col)
	}
	return result
}

func (e *Expression) collectColumns(columns map[string]bool) {
	if e.Column != "" {
		columns[e.Column] = true
	}
	for _, child := range e.Children {
		child.collectColumns(columns)
	}
}
