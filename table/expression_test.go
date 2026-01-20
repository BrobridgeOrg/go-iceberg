package table

import (
	"testing"
)

func TestExprBuilderEq(t *testing.T) {
	expr := Col("id").Eq(123)

	if expr.Op != OpEq {
		t.Errorf("Op = %v, want OpEq", expr.Op)
	}
	if expr.Column != "id" {
		t.Errorf("Column = %s, want id", expr.Column)
	}
	if expr.Value != 123 {
		t.Errorf("Value = %v, want 123", expr.Value)
	}
}

func TestExprBuilderNotEq(t *testing.T) {
	expr := Col("status").NotEq("deleted")

	if expr.Op != OpNotEq {
		t.Errorf("Op = %v, want OpNotEq", expr.Op)
	}
	if expr.Column != "status" {
		t.Errorf("Column = %s, want status", expr.Column)
	}
	if expr.Value != "deleted" {
		t.Errorf("Value = %v, want deleted", expr.Value)
	}
}

func TestExprBuilderComparisons(t *testing.T) {
	tests := []struct {
		name     string
		expr     *Expression
		expected ExprOp
	}{
		{"Gt", Col("age").Gt(18), OpGt},
		{"Gte", Col("age").Gte(18), OpGte},
		{"Lt", Col("age").Lt(65), OpLt},
		{"Lte", Col("age").Lte(65), OpLte},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expr.Op != tt.expected {
				t.Errorf("Op = %v, want %v", tt.expr.Op, tt.expected)
			}
		})
	}
}

func TestExprBuilderIn(t *testing.T) {
	expr := Col("status").In("active", "pending", "review")

	if expr.Op != OpIn {
		t.Errorf("Op = %v, want OpIn", expr.Op)
	}
	if expr.Column != "status" {
		t.Errorf("Column = %s, want status", expr.Column)
	}
	if len(expr.Values) != 3 {
		t.Errorf("Values length = %d, want 3", len(expr.Values))
	}
}

func TestExprBuilderNotIn(t *testing.T) {
	expr := Col("role").NotIn("banned", "suspended")

	if expr.Op != OpNotIn {
		t.Errorf("Op = %v, want OpNotIn", expr.Op)
	}
	if len(expr.Values) != 2 {
		t.Errorf("Values length = %d, want 2", len(expr.Values))
	}
}

func TestExprBuilderNullChecks(t *testing.T) {
	exprNull := Col("email").IsNull()
	if exprNull.Op != OpIsNull {
		t.Errorf("IsNull Op = %v, want OpIsNull", exprNull.Op)
	}

	exprNotNull := Col("email").IsNotNull()
	if exprNotNull.Op != OpNotNull {
		t.Errorf("IsNotNull Op = %v, want OpNotNull", exprNotNull.Op)
	}
}

func TestExprBuilderStartsWith(t *testing.T) {
	expr := Col("name").StartsWith("John")

	if expr.Op != OpStartsWith {
		t.Errorf("Op = %v, want OpStartsWith", expr.Op)
	}
	if expr.Column != "name" {
		t.Errorf("Column = %s, want name", expr.Column)
	}
	if expr.Value != "John" {
		t.Errorf("Value = %v, want John", expr.Value)
	}
}

func TestAndExpression(t *testing.T) {
	expr := And(
		Col("age").Gte(18),
		Col("status").Eq("active"),
	)

	if expr.Op != OpAnd {
		t.Errorf("Op = %v, want OpAnd", expr.Op)
	}
	if len(expr.Children) != 2 {
		t.Errorf("Children length = %d, want 2", len(expr.Children))
	}
}

func TestOrExpression(t *testing.T) {
	expr := Or(
		Col("role").Eq("admin"),
		Col("role").Eq("moderator"),
	)

	if expr.Op != OpOr {
		t.Errorf("Op = %v, want OpOr", expr.Op)
	}
	if len(expr.Children) != 2 {
		t.Errorf("Children length = %d, want 2", len(expr.Children))
	}
}

func TestNotExpression(t *testing.T) {
	inner := Col("deleted").Eq(true)
	expr := Not(inner)

	if expr.Op != OpNot {
		t.Errorf("Op = %v, want OpNot", expr.Op)
	}
	if len(expr.Children) != 1 {
		t.Errorf("Children length = %d, want 1", len(expr.Children))
	}
}

func TestNestedExpressions(t *testing.T) {
	// ((age >= 18) AND (status = 'active')) OR (role = 'admin')
	expr := Or(
		And(
			Col("age").Gte(18),
			Col("status").Eq("active"),
		),
		Col("role").Eq("admin"),
	)

	if expr.Op != OpOr {
		t.Errorf("Root Op = %v, want OpOr", expr.Op)
	}
	if len(expr.Children) != 2 {
		t.Errorf("Root Children length = %d, want 2", len(expr.Children))
	}

	andExpr := expr.Children[0]
	if andExpr.Op != OpAnd {
		t.Errorf("First child Op = %v, want OpAnd", andExpr.Op)
	}
}

func TestConvenienceFunctions(t *testing.T) {
	// Test Eq helper
	eqExpr := Eq("id", 123)
	if eqExpr.Op != OpEq || eqExpr.Column != "id" {
		t.Error("Eq helper failed")
	}

	// Test NotEq helper
	neqExpr := NotEq("status", "deleted")
	if neqExpr.Op != OpNotEq || neqExpr.Column != "status" {
		t.Error("NotEq helper failed")
	}

	// Test Gt helper
	gtExpr := Gt("age", 18)
	if gtExpr.Op != OpGt || gtExpr.Column != "age" {
		t.Error("Gt helper failed")
	}

	// Test Gte helper
	gteExpr := Gte("age", 18)
	if gteExpr.Op != OpGte || gteExpr.Column != "age" {
		t.Error("Gte helper failed")
	}

	// Test Lt helper
	ltExpr := Lt("age", 65)
	if ltExpr.Op != OpLt || ltExpr.Column != "age" {
		t.Error("Lt helper failed")
	}

	// Test Lte helper
	lteExpr := Lte("age", 65)
	if lteExpr.Op != OpLte || lteExpr.Column != "age" {
		t.Error("Lte helper failed")
	}

	// Test In helper
	inExpr := In("status", "a", "b", "c")
	if inExpr.Op != OpIn || len(inExpr.Values) != 3 {
		t.Error("In helper failed")
	}

	// Test IsNull helper
	nullExpr := IsNull("email")
	if nullExpr.Op != OpIsNull || nullExpr.Column != "email" {
		t.Error("IsNull helper failed")
	}

	// Test IsNotNull helper
	notNullExpr := IsNotNull("email")
	if notNullExpr.Op != OpNotNull || notNullExpr.Column != "email" {
		t.Error("IsNotNull helper failed")
	}
}

func TestBetween(t *testing.T) {
	expr := Between("price", 10.0, 100.0)

	if expr.Op != OpAnd {
		t.Errorf("Between should create AND expression, got %v", expr.Op)
	}
	if len(expr.Children) != 2 {
		t.Errorf("Between should have 2 children, got %d", len(expr.Children))
	}

	// First child: price >= 10.0
	if expr.Children[0].Op != OpGte {
		t.Error("First child should be Gte")
	}
	// Second child: price <= 100.0
	if expr.Children[1].Op != OpLte {
		t.Error("Second child should be Lte")
	}
}

func TestExpressionString(t *testing.T) {
	// Test simple expression
	expr := Col("id").Eq(123)
	str := expr.String()
	if str == "" {
		t.Error("Expression.String() should not be empty")
	}

	// Test AND expression
	andExpr := And(Col("a").Eq(1), Col("b").Eq(2))
	andStr := andExpr.String()
	if andStr == "" {
		t.Error("AND Expression.String() should not be empty")
	}
}
