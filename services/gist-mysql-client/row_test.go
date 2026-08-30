package gistmysqlclient

import (
	"reflect"
	"testing"
)

func TestBuildModelRow_NonPointerZeroValue_IsIncluded(t *testing.T) {
	// This is the documented footgun/convention: a non-pointer field is
	// NEVER omitted, even at its zero value - callers of Update must use
	// a purpose-built partial struct, not their full read model.
	type widget struct {
		ID   int    `db:"id" json:"id"`
		Name string `db:"name" json:"name"`
	}
	row := buildModelRow(reflect.ValueOf(widget{}))
	if v, ok := row["id"]; !ok || v != 0 {
		t.Fatalf("expected id=0 to be present (not omitted), got %v (present=%v)", v, ok)
	}
	if v, ok := row["name"]; !ok || v != "" {
		t.Fatalf(`expected name="" to be present (not omitted), got %v (present=%v)`, v, ok)
	}
}

func TestBuildModelRow_NilPointerField_IsOmitted(t *testing.T) {
	type patch struct {
		Name *string `db:"name" json:"name"`
		Qty  *int    `db:"qty" json:"qty"`
	}
	row := buildModelRow(reflect.ValueOf(patch{}))
	if _, ok := row["name"]; ok {
		t.Fatalf("expected a nil pointer field to be omitted entirely, got %v", row)
	}
	if _, ok := row["qty"]; ok {
		t.Fatalf("expected a nil pointer field to be omitted entirely, got %v", row)
	}
	if len(row) != 0 {
		t.Fatalf("expected an empty row for an all-nil-pointer struct, got %v", row)
	}
}

func TestBuildModelRow_NonNilPointerField_DereferencedAndIncluded(t *testing.T) {
	type patch struct {
		Name *string `db:"name" json:"name"`
	}
	name := "new-name"
	row := buildModelRow(reflect.ValueOf(patch{Name: &name}))
	if row["name"] != "new-name" {
		t.Fatalf("expected the dereferenced value, got %v", row["name"])
	}
}

func TestBuildModelRow_PartialUpdate_OnlySetFieldsPresent(t *testing.T) {
	// The actual real-world use case: a partial-update struct where some
	// fields are populated and others are deliberately left nil.
	type patch struct {
		Name *string `db:"name" json:"name"`
		Qty  *int    `db:"qty" json:"qty"`
	}
	name := "widget"
	row := buildModelRow(reflect.ValueOf(patch{Name: &name, Qty: nil}))
	if len(row) != 1 {
		t.Fatalf("expected exactly one populated field, got %v", row)
	}
	if row["name"] != "widget" {
		t.Fatalf("got %v", row)
	}
}

func TestBuildModelRow_RelationFieldsExcluded(t *testing.T) {
	type order struct {
		ID int `db:"id" json:"id"`
	}
	type item struct {
		ID     int     `db:"id" json:"id"`
		Orders []order `db:"orders" json:"orders" join:"id,item_id"`
	}
	row := buildModelRow(reflect.ValueOf(item{ID: 1, Orders: []order{{ID: 1}}}))
	if _, ok := row["orders"]; ok {
		t.Fatalf("expected the has-many relation field to be excluded from the write row, got %v", row)
	}
	if row["id"] != 1 {
		t.Fatalf("expected the scalar id field to still be included, got %v", row)
	}
}

func TestBuildModelRow_TopLevelNilPointer_ReturnsEmptyRow(t *testing.T) {
	type widget struct {
		Name string `db:"name" json:"name"`
	}
	var w *widget
	row := buildModelRow(reflect.ValueOf(w))
	if len(row) != 0 {
		t.Fatalf("expected an empty row for a nil top-level pointer, got %v", row)
	}
}

func TestBuildModelRow_EmbeddedStruct_Promoted(t *testing.T) {
	type Base struct {
		ID int `db:"id" json:"id"`
	}
	type Widget struct {
		Base
		Name string `db:"name" json:"name"`
	}
	row := buildModelRow(reflect.ValueOf(Widget{ID: 7, Name: "w"}))
	if row["id"] != 7 || row["name"] != "w" {
		t.Fatalf("got %v", row)
	}
}

func TestBuildModelRow_FieldWithoutColumnTag_Excluded(t *testing.T) {
	type widget struct {
		ID       int `db:"id" json:"id"`
		internal int
	}
	row := buildModelRow(reflect.ValueOf(widget{ID: 1, internal: 99}))
	if len(row) != 1 {
		t.Fatalf("expected only the db-tagged field, got %v", row)
	}
}
