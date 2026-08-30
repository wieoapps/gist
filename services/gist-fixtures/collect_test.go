package gistfixtures

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"google.golang.org/grpc"

	"github.com/wieoapps/gist"
	"github.com/wieoapps/gist-proto"
	"github.com/wieoapps/gist/internal/rpcconn"
)

type fixtureOrder struct {
	ID       int `source:"db.orders" db:"id" json:"id"`
	ItemID   int `db:"item_id" json:"item_id"`
	Quantity int `db:"quantity" json:"quantity"`
}

type fixtureItem struct {
	ID     int            `source:"db.items" db:"id" json:"id"`
	Name   string         `db:"name" json:"name"`
	Price  float64        `db:"price" json:"price"`
	Active bool           `db:"active" json:"active"`
	Orders []fixtureOrder `db:"orders" json:"orders" join:"id,item_id"`
}

func TestCollect_ScalarFieldsAndKinds(t *testing.T) {
	out := tableRows{}
	kinds := columnKinds{}
	collect(out, kinds, reflect.ValueOf(fixtureOrder{ID: 1, ItemID: 2, Quantity: 3}))

	if len(out["orders"]) != 1 {
		t.Fatalf("expected 1 row in table \"orders\", got %+v", out)
	}
	row := out["orders"][0]
	if row["id"] != 1 || row["item_id"] != 2 || row["quantity"] != 3 {
		t.Fatalf("unexpected row: %+v", row)
	}
	if kinds["orders"]["id"] != "int" || kinds["orders"]["quantity"] != "int" {
		t.Fatalf("expected int kinds for id/quantity, got %+v", kinds["orders"])
	}
}

func TestCollect_RecursesIntoJoinedSlice(t *testing.T) {
	out := tableRows{}
	kinds := columnKinds{}
	item := fixtureItem{
		ID: 1, Name: "widget", Price: 9.99, Active: true,
		Orders: []fixtureOrder{{ID: 10, ItemID: 1, Quantity: 2}, {ID: 11, ItemID: 1, Quantity: 5}},
	}
	collect(out, kinds, reflect.ValueOf(item))

	if len(out["items"]) != 1 {
		t.Fatalf("expected 1 row in table \"items\", got %+v", out)
	}
	if len(out["orders"]) != 2 {
		t.Fatalf("expected the joined Orders slice to recurse into its own table, got %+v", out["orders"])
	}
	itemRow := out["items"][0]
	if itemRow["name"] != "widget" || itemRow["price"] != 9.99 || itemRow["active"] != true {
		t.Fatalf("unexpected item row: %+v", itemRow)
	}
	if kinds["items"]["price"] != "float" || kinds["items"]["active"] != "bool" || kinds["items"]["name"] != "string" {
		t.Fatalf("unexpected item kinds: %+v", kinds["items"])
	}
	if kinds["orders"]["id"] != "int" {
		t.Fatalf("expected the joined table's own kinds to also be collected, got %+v", kinds["orders"])
	}
}

func TestCollect_SliceOfStructs_EachCollected(t *testing.T) {
	out := tableRows{}
	kinds := columnKinds{}
	orders := []fixtureOrder{{ID: 1, ItemID: 1, Quantity: 1}, {ID: 2, ItemID: 1, Quantity: 2}}
	collect(out, kinds, reflect.ValueOf(orders))
	if len(out["orders"]) != 2 {
		t.Fatalf("expected 2 rows from a top-level slice, got %+v", out["orders"])
	}
}

func TestCollect_NilPointer_NoOp(t *testing.T) {
	out := tableRows{}
	kinds := columnKinds{}
	var p *fixtureOrder
	collect(out, kinds, reflect.ValueOf(p))
	if len(out) != 0 {
		t.Fatalf("expected a nil pointer to collect nothing, got %+v", out)
	}
}

func TestCollect_NonStruct_NoOp(t *testing.T) {
	out := tableRows{}
	kinds := columnKinds{}
	collect(out, kinds, reflect.ValueOf(42))
	if len(out) != 0 {
		t.Fatalf("expected a non-struct value to collect nothing, got %+v", out)
	}
}

func TestCollect_MissingSourceTag_Panics(t *testing.T) {
	type noSource struct {
		ID int `db:"id" json:"id"`
	}
	defer func() {
		if recover() == nil {
			t.Fatal("expected collect to panic when a struct has no `source` tag")
		}
	}()
	collect(tableRows{}, columnKinds{}, reflect.ValueOf(noSource{ID: 1}))
}

func TestCollect_UnexportedAndUntaggedFields_Skipped(t *testing.T) {
	type withExtras struct {
		ID       int `source:"db.things" db:"id" json:"id"`
		untagged string
		NoDbTag  string
	}
	out := tableRows{}
	kinds := columnKinds{}
	collect(out, kinds, reflect.ValueOf(withExtras{ID: 1, untagged: "x", NoDbTag: "y"}))
	row := out["things"][0]
	if len(row) != 1 {
		t.Fatalf("expected only the db-tagged field to be collected, got %+v", row)
	}
}

// --- fieldKind / fieldValue ---

func TestFieldKind_Classifications(t *testing.T) {
	cases := []struct {
		v    any
		want string
	}{
		{int(0), "int"},
		{int64(0), "int"},
		{uint(0), "int"},
		{float64(0), "float"},
		{float32(0), "float"},
		{true, "bool"},
		{"x", "string"},
		{map[string]any{}, "string"},
		{struct{}{}, "other"},
	}
	for _, c := range cases {
		got := fieldKind(reflect.ValueOf(c.v))
		if got != c.want {
			t.Errorf("fieldKind(%T) = %q, want %q", c.v, got, c.want)
		}
	}
}

func TestFieldKind_NilPointer_IsOther(t *testing.T) {
	var p *int
	if got := fieldKind(reflect.ValueOf(p)); got != "other" {
		t.Errorf("expected a nil pointer to classify as \"other\", got %q", got)
	}
}

func TestFieldKind_NonNilPointer_DereferencedKind(t *testing.T) {
	n := 5
	if got := fieldKind(reflect.ValueOf(&n)); got != "int" {
		t.Errorf("expected a non-nil *int to classify as \"int\", got %q", got)
	}
}

func TestFieldValue_MapEncodedAsJSONString(t *testing.T) {
	got := fieldValue(reflect.ValueOf(map[string]any{"a": 1}))
	s, ok := got.(string)
	if !ok {
		t.Fatalf("expected a map field to be JSON-encoded to a string, got %T", got)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(s), &decoded); err != nil {
		t.Fatalf("could not decode fieldValue's output as JSON: %v", err)
	}
	if decoded["a"] != float64(1) {
		t.Fatalf("unexpected decoded map: %+v", decoded)
	}
}

func TestFieldValue_NilPointer_ReturnsNil(t *testing.T) {
	var p *int
	if got := fieldValue(reflect.ValueOf(p)); got != nil {
		t.Errorf("expected nil for a nil pointer field, got %v", got)
	}
}

func TestFieldValue_Scalar_ReturnsAsIs(t *testing.T) {
	if got := fieldValue(reflect.ValueOf(42)); got != 42 {
		t.Errorf("expected 42, got %v", got)
	}
}

// --- GenerateFixtures: fake BootstrapServiceClient, no live gist-server. ---

type fakeAdminClient struct {
	req *gistproto.GenerateFixturesRequest
	err error
}

func (f *fakeAdminClient) Register(context.Context, *gistproto.RegisterRequest, ...grpc.CallOption) (*gistproto.RegisterResponse, error) {
	return &gistproto.RegisterResponse{}, nil
}

func (f *fakeAdminClient) GenerateFixtures(_ context.Context, in *gistproto.GenerateFixturesRequest, _ ...grpc.CallOption) (*gistproto.GenerateFixturesResponse, error) {
	f.req = in
	if f.err != nil {
		return nil, f.err
	}
	return &gistproto.GenerateFixturesResponse{}, nil
}

func TestGenerateFixtures_SendsEncodedRowsAndKinds(t *testing.T) {
	fake := &fakeAdminClient{}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{Admin: fake})

	opt := GenerateFixtures("fixtures-1", fixtureOrder{ID: 1, ItemID: 2, Quantity: 3})
	if err := opt(server); err != nil {
		t.Fatalf("GenerateFixtures option failed: %v", err)
	}

	if fake.req == nil {
		t.Fatal("expected GenerateFixtures to be called on the Admin client")
	}
	if fake.req.GetServiceId() != "fixtures-1" {
		t.Errorf("expected ServiceId %q, got %q", "fixtures-1", fake.req.GetServiceId())
	}

	var decoded tableRows
	if err := json.Unmarshal(fake.req.GetRowsJson(), &decoded); err != nil {
		t.Fatalf("could not decode RowsJson: %v", err)
	}
	if len(decoded["orders"]) != 1 {
		t.Fatalf("expected 1 encoded row for table \"orders\", got %+v", decoded)
	}

	kinds := fake.req.GetColumnKinds()
	if kinds["orders"] == nil || kinds["orders"].GetKinds()["quantity"] != "int" {
		t.Fatalf("expected wire ColumnKinds to include orders.quantity=int, got %+v", kinds)
	}
}

func TestGenerateFixtures_MultipleModelValues_MergedByTable(t *testing.T) {
	fake := &fakeAdminClient{}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{Admin: fake})

	opt := GenerateFixtures("fixtures-1",
		fixtureOrder{ID: 1, ItemID: 1, Quantity: 1},
		fixtureOrder{ID: 2, ItemID: 1, Quantity: 2},
	)
	if err := opt(server); err != nil {
		t.Fatalf("GenerateFixtures option failed: %v", err)
	}

	var decoded tableRows
	if err := json.Unmarshal(fake.req.GetRowsJson(), &decoded); err != nil {
		t.Fatalf("could not decode RowsJson: %v", err)
	}
	if len(decoded["orders"]) != 2 {
		t.Fatalf("expected both rows merged under table \"orders\", got %+v", decoded["orders"])
	}
}

func TestGenerateFixtures_PropagatesServerError(t *testing.T) {
	fake := &fakeAdminClient{err: context.DeadlineExceeded}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{Admin: fake})

	opt := GenerateFixtures("fixtures-1", fixtureOrder{ID: 1, ItemID: 1, Quantity: 1})
	if err := opt(server); err == nil {
		t.Fatal("expected GenerateFixtures to propagate the server error")
	}
}
