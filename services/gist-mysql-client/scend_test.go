package gistmysqlclient

import "testing"

func TestAsc_ToWire(t *testing.T) {
	wire := Asc([]string{"name"}).toWire()
	if wire.GetDescending() {
		t.Fatal("Asc must not set Descending")
	}
	if len(wire.GetColumns()) != 1 || wire.GetColumns()[0] != "name" {
		t.Fatalf("unexpected columns: %v", wire.GetColumns())
	}
}

func TestDesc_ToWire(t *testing.T) {
	wire := Desc([]string{"created_at"}).toWire()
	if !wire.GetDescending() {
		t.Fatal("Desc must set Descending")
	}
}

func TestToWireSorts_PreservesOrder(t *testing.T) {
	sorts := []Scend{Asc([]string{"a"}), Desc([]string{"b"})}
	wire := toWireSorts(sorts)
	if len(wire) != 2 {
		t.Fatalf("expected 2, got %d", len(wire))
	}
	if wire[0].GetDescending() || !wire[1].GetDescending() {
		t.Fatalf("expected [asc, desc] order preserved, got %+v", wire)
	}
}
