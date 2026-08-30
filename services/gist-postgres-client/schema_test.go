package gistpostgresclient

import (
	"reflect"
	"testing"
)

type testOrder struct {
	ID       int `source:"public.orders" db:"id" json:"id"`
	ItemID   int `db:"item_id" json:"item_id"`
	Quantity int `db:"quantity" json:"quantity"`
}

type testItem struct {
	ID     int         `source:"public.items" db:"id" json:"id"`
	Name   string      `db:"name" json:"name"`
	Orders []testOrder `db:"orders" json:"orders" join:"id,item_id"`
}

func TestBuildModelSchema_ScalarFields(t *testing.T) {
	schema := buildModelSchema(reflect.TypeFor[testOrder]())
	if len(schema.Sources) != 2 || schema.Sources[0] != "public" || schema.Sources[1] != "orders" {
		t.Fatalf("unexpected sources: %v", schema.Sources)
	}
	if len(schema.Fields) != 3 {
		t.Fatalf("expected 3 fields, got %d: %+v", len(schema.Fields), schema.Fields)
	}
	names := map[string]bool{}
	for _, f := range schema.Fields {
		names[f.Name] = true
		if f.IsStruct {
			t.Fatalf("did not expect any struct field on a plain scalar struct, got %+v", f)
		}
	}
	for _, want := range []string{"id", "item_id", "quantity"} {
		if !names[want] {
			t.Fatalf("expected field %q, got %v", want, names)
		}
	}
}

func TestBuildModelSchema_HasManySlice_ReportsRawStructure(t *testing.T) {
	schema := buildModelSchema(reflect.TypeFor[testItem]())
	var found bool
	for _, f := range schema.Fields {
		if !f.IsStruct {
			continue
		}
		found = true
		if f.JsonName != "orders" || !f.IsSlice {
			t.Fatalf("unexpected struct field: %+v", f)
		}
		if f.JoinTag != "id,item_id" {
			t.Fatalf("expected the raw join tag to be forwarded unparsed, got %q", f.JoinTag)
		}
		if f.Nested == nil || len(f.Nested.Fields) != 3 {
			t.Fatalf("expected the nested schema to have testOrder's 3 fields, got %+v", f.Nested)
		}
	}
	if !found {
		t.Fatal("expected to find a struct field for Orders")
	}
}

func TestBuildModelSchema_PointerToStruct(t *testing.T) {
	schema := buildModelSchema(reflect.TypeFor[*testOrder]())
	if len(schema.Sources) != 2 {
		t.Fatalf("expected a pointer-to-struct type to be dereferenced, got sources %v", schema.Sources)
	}
}

func TestBuildModelSchema_MalformedJoinTag_ForwardedUnvalidated(t *testing.T) {
	// The client no longer parses or validates the join tag - that
	// interpretation (including rejecting a malformed one) now happens
	// server-side. buildModelSchema must not panic here; it just reports
	// whatever raw string the tag holds.
	type badRelation struct {
		ID     int         `source:"public.items" db:"id" json:"id"`
		Orders []testOrder `db:"orders" json:"orders" join:"id"` // missing the second segment
	}
	schema := buildModelSchema(reflect.TypeFor[badRelation]())
	var found bool
	for _, f := range schema.Fields {
		if f.Name == "orders" {
			found = true
			if f.JoinTag != "id" {
				t.Fatalf("expected the malformed join tag to be forwarded as-is, got %q", f.JoinTag)
			}
		}
	}
	if !found {
		t.Fatal("expected to find the orders field despite its malformed join tag")
	}
}

func TestBuildModelSchema_EmbeddedStruct_FieldsPromoted(t *testing.T) {
	type Base struct {
		ID int `source:"public.widgets" db:"id" json:"id"`
	}
	type Widget struct {
		Base
		Name string `db:"name" json:"name"`
	}
	schema := buildModelSchema(reflect.TypeFor[Widget]())
	if schema.Sources[1] != "widgets" {
		t.Fatalf("expected the embedded struct's source tag to set Sources, got %v", schema.Sources)
	}
	if len(schema.Fields) != 2 {
		t.Fatalf("expected both the embedded and outer fields, got %+v", schema.Fields)
	}
}

func TestBuildModelSchema_BoolField_SetsIsBool(t *testing.T) {
	// Identical to gist-mysql's schema_test.go - the wire hint that lets
	// the server coerce a driver-returned smallint-as-boolean into a real
	// JSON boolean before sending the row back.
	type flags struct {
		ID         int   `source:"public.flags" db:"id" json:"id"`
		Active     bool  `db:"active" json:"active"`
		IsImported *bool `db:"is_imported" json:"is_imported,omitempty"`
		Count      int   `db:"count" json:"count"`
	}
	schema := buildModelSchema(reflect.TypeFor[flags]())

	byName := map[string]bool{}
	for _, f := range schema.Fields {
		byName[f.Name] = f.IsBool
	}
	if !byName["active"] {
		t.Error("expected active (bool) to have IsBool=true")
	}
	if !byName["is_imported"] {
		t.Error("expected is_imported (*bool) to have IsBool=true")
	}
	if byName["id"] || byName["count"] {
		t.Error("expected non-bool fields to have IsBool=false")
	}
}

func TestBuildModelSchema_BelongsToStruct_ReportsRawStructure(t *testing.T) {
	type parent struct {
		ID   int    `source:"public.items" db:"id" json:"id"`
		Name string `db:"name" json:"name"`
	}
	type child struct {
		ID     int    `source:"public.orders" db:"id" json:"id"`
		Item   parent `db:"item" json:"item" join:"item_id,id"`
		ItemID int    `db:"item_id" json:"item_id"`
	}
	schema := buildModelSchema(reflect.TypeFor[child]())
	var found bool
	for _, f := range schema.Fields {
		if f.Name != "item" {
			continue
		}
		found = true
		if !f.IsStruct || f.IsSlice {
			t.Fatalf("expected a non-slice struct field, got %+v", f)
		}
		if f.JoinTag != "item_id,id" {
			t.Fatalf("expected the raw join tag to be forwarded unparsed, got %q", f.JoinTag)
		}
	}
	if !found {
		t.Fatal("expected to find the Item struct field")
	}
}
