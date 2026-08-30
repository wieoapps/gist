package gistmysqlclient

import (
	"encoding/json"
	"testing"
)

func TestCondition_ToWire_Leaf(t *testing.T) {
	c := NewCondition("id", Equal, 5)
	wire := c.(condition).toWire()
	leaf := wire.GetLeaf()
	if leaf == nil {
		t.Fatal("expected a leaf condition")
	}
	if leaf.GetField() != "id" || leaf.GetOperator() != "eq" {
		t.Fatalf("unexpected leaf: %+v", leaf)
	}
	var v float64
	if err := json.Unmarshal(leaf.GetValueJson(), &v); err != nil {
		t.Fatalf("could not decode value_json: %v", err)
	}
	if v != 5 {
		t.Fatalf("expected value 5, got %v", v)
	}
}

func TestNewBetweenCondition_EncodesTwoElementArray(t *testing.T) {
	c := NewBetweenCondition("age", 18, 65)
	wire := c.(condition).toWire().GetLeaf()
	if wire.GetOperator() != "between" {
		t.Fatalf("expected operator 'between', got %q", wire.GetOperator())
	}
	var bounds [2]any
	if err := json.Unmarshal(wire.GetValueJson(), &bounds); err != nil {
		t.Fatalf("could not decode bounds: %v", err)
	}
	if bounds[0] != float64(18) || bounds[1] != float64(65) {
		t.Fatalf("expected [18, 65], got %v", bounds)
	}
}

func TestNewNotBetweenCondition(t *testing.T) {
	c := NewNotBetweenCondition("age", 1, 2)
	wire := c.(condition).toWire().GetLeaf()
	if wire.GetOperator() != "not_between" {
		t.Fatalf("expected operator 'not_between', got %q", wire.GetOperator())
	}
}

func TestAndConditions_ToWire(t *testing.T) {
	c := AndConditions(NewCondition("a", Equal, 1), NewCondition("b", Equal, 2))
	wire := c.(conditionGroup).toWire()
	if wire.GetAndGroup() == nil {
		t.Fatal("expected an AndGroup")
	}
	if len(wire.GetAndGroup().GetConditions()) != 2 {
		t.Fatalf("expected 2 child conditions, got %d", len(wire.GetAndGroup().GetConditions()))
	}
}

func TestOrConditions_ToWire(t *testing.T) {
	c := OrConditions(NewCondition("a", Equal, 1), NewCondition("b", Equal, 2))
	wire := c.(conditionGroup).toWire()
	if wire.GetOrGroup() == nil {
		t.Fatal("expected an OrGroup")
	}
}

func TestToWireConditions_PreservesOrder(t *testing.T) {
	cs := []Conditioner{NewCondition("a", Equal, 1), NewCondition("b", Equal, 2)}
	wire := toWireConditions(cs)
	if len(wire) != 2 {
		t.Fatalf("expected 2 wire conditions, got %d", len(wire))
	}
	if wire[0].GetLeaf().GetField() != "a" || wire[1].GetLeaf().GetField() != "b" {
		t.Fatalf("expected order preserved, got %+v", wire)
	}
}

func TestToWireRelationConditions_EmptyMapIsNil(t *testing.T) {
	if got := toWireRelationConditions(nil); got != nil {
		t.Fatalf("expected nil for an empty/nil map, got %v", got)
	}
	if got := toWireRelationConditions(map[string][]Conditioner{}); got != nil {
		t.Fatalf("expected nil for an empty map, got %v", got)
	}
}

func TestToWireRelationConditions_PerRelation(t *testing.T) {
	m := map[string][]Conditioner{
		"orders": {NewCondition("quantity", GreaterThan, 0)},
	}
	got := toWireRelationConditions(m)
	if len(got) != 1 || got["orders"] == nil {
		t.Fatalf("expected a wire ConditionGroup keyed by relation name, got %v", got)
	}
	if len(got["orders"].GetConditions()) != 1 {
		t.Fatalf("expected 1 condition under 'orders', got %d", len(got["orders"].GetConditions()))
	}
}
