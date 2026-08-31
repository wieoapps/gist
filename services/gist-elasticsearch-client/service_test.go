package gistelasticsearchclient

import (
	"context"
	"encoding/json"
	"testing"

	"google.golang.org/grpc"

	"github.com/wieoapps/gist"
	"github.com/wieoapps/gist-proto"
	"github.com/wieoapps/gist/internal/rpcconn"
)

// fakeESClient records the last request of each kind and returns a
// scripted response - exercises the real client-side JSON encoding/
// decoding with no live Elasticsearch cluster or gist-server needed.
type fakeESClient struct {
	lastCreate *gistproto.CreateItemRequest
	createResp *gistproto.CreateItemResponse
	createErr  error
	lastIndex  *gistproto.IndexItemRequest
	indexResp  *gistproto.IndexItemResponse
	indexErr   error
	lastGet    *gistproto.GetItemRequest
	getResp    *gistproto.GetItemResponse
	getErr     error
	lastUpdate *gistproto.UpdateItemRequest
	updateResp *gistproto.UpdateItemResponse
	updateErr  error
	lastDelete *gistproto.DeleteItemRequest
	deleteResp *gistproto.DeleteItemResponse
	deleteErr  error
	lastBulk   *gistproto.BulkIndexRequest
	bulkResp   *gistproto.BulkIndexResponse
	bulkErr    error
	lastSearch *gistproto.SearchRequest
	searchResp *gistproto.SearchResponse
	searchErr  error
}

func (f *fakeESClient) CreateItem(_ context.Context, in *gistproto.CreateItemRequest, _ ...grpc.CallOption) (*gistproto.CreateItemResponse, error) {
	f.lastCreate = in
	if f.createErr != nil {
		return nil, f.createErr
	}
	return f.createResp, nil
}

func (f *fakeESClient) IndexItem(_ context.Context, in *gistproto.IndexItemRequest, _ ...grpc.CallOption) (*gistproto.IndexItemResponse, error) {
	f.lastIndex = in
	if f.indexErr != nil {
		return nil, f.indexErr
	}
	return f.indexResp, nil
}

func (f *fakeESClient) GetItem(_ context.Context, in *gistproto.GetItemRequest, _ ...grpc.CallOption) (*gistproto.GetItemResponse, error) {
	f.lastGet = in
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.getResp, nil
}

func (f *fakeESClient) UpdateItem(_ context.Context, in *gistproto.UpdateItemRequest, _ ...grpc.CallOption) (*gistproto.UpdateItemResponse, error) {
	f.lastUpdate = in
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	return f.updateResp, nil
}

func (f *fakeESClient) DeleteItem(_ context.Context, in *gistproto.DeleteItemRequest, _ ...grpc.CallOption) (*gistproto.DeleteItemResponse, error) {
	f.lastDelete = in
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	return f.deleteResp, nil
}

func (f *fakeESClient) BulkIndex(_ context.Context, in *gistproto.BulkIndexRequest, _ ...grpc.CallOption) (*gistproto.BulkIndexResponse, error) {
	f.lastBulk = in
	if f.bulkErr != nil {
		return nil, f.bulkErr
	}
	return f.bulkResp, nil
}

func (f *fakeESClient) Search(_ context.Context, in *gistproto.SearchRequest, _ ...grpc.CallOption) (*gistproto.SearchResponse, error) {
	f.lastSearch = in
	if f.searchErr != nil {
		return nil, f.searchErr
	}
	return f.searchResp, nil
}

type sampleItem struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func newTestService(fake *fakeESClient) *Service {
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{Elasticsearch: fake})
	return NewService(server, "es1")
}

func TestCreateItem_EncodesItemAndSendsRequest(t *testing.T) {
	fake := &fakeESClient{createResp: &gistproto.CreateItemResponse{Created: true, Id: "generated-1"}}
	svc := newTestService(fake)

	created, id, err := svc.CreateItem(context.Background(), "my-index", "", sampleItem{Name: "widget", Age: 3})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !created {
		t.Error("expected created=true")
	}
	if id != "generated-1" {
		t.Errorf("id = %q, want generated-1", id)
	}

	if fake.lastCreate.GetServiceId() != "es1" {
		t.Fatalf("expected service id es1, got %q", fake.lastCreate.GetServiceId())
	}
	if fake.lastCreate.GetIndex() != "my-index" {
		t.Fatalf("expected index 'my-index', got %q", fake.lastCreate.GetIndex())
	}
	if fake.lastCreate.GetId() != "" {
		t.Errorf("expected no id sent for an auto-generated create, got %q", fake.lastCreate.GetId())
	}

	var decoded sampleItem
	if err := json.Unmarshal(fake.lastCreate.GetItemJson(), &decoded); err != nil {
		t.Fatalf("could not decode sent item_json: %v", err)
	}
	if decoded != (sampleItem{Name: "widget", Age: 3}) {
		t.Fatalf("expected item to round-trip through JSON encoding, got %+v", decoded)
	}
}

func TestCreateItem_ExplicitIDIsSent(t *testing.T) {
	fake := &fakeESClient{createResp: &gistproto.CreateItemResponse{Created: true, Id: "w-1"}}
	svc := newTestService(fake)

	created, id, err := svc.CreateItem(context.Background(), "my-index", "w-1", sampleItem{Name: "widget"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !created || id != "w-1" {
		t.Errorf("created=%v id=%q, want created=true id=w-1", created, id)
	}
	if fake.lastCreate.GetId() != "w-1" {
		t.Errorf("expected id w-1 to be sent, got %q", fake.lastCreate.GetId())
	}
}

func TestCreateItem_ConflictReturnsFalseNoError(t *testing.T) {
	fake := &fakeESClient{createResp: &gistproto.CreateItemResponse{Created: false}}
	svc := newTestService(fake)

	created, id, err := svc.CreateItem(context.Background(), "my-index", "w-1", sampleItem{Name: "widget"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if created || id != "" {
		t.Errorf("created=%v id=%q, want created=false id=\"\"", created, id)
	}
}

func TestCreateItem_WireErrorBecomesError(t *testing.T) {
	fake := &fakeESClient{createResp: &gistproto.CreateItemResponse{ErrorCode: "internal", ErrorMessage: "cluster unavailable"}}
	svc := newTestService(fake)

	_, _, err := svc.CreateItem(context.Background(), "my-index", "", sampleItem{Name: "x"})
	if err == nil {
		t.Fatal("expected an error when the wire response carries an error_code")
	}
}

func TestCreateItem_TransportErrorPropagates(t *testing.T) {
	fake := &fakeESClient{createErr: context.DeadlineExceeded}
	svc := newTestService(fake)

	_, _, err := svc.CreateItem(context.Background(), "my-index", "", sampleItem{Name: "x"})
	if err == nil {
		t.Fatal("expected an error when the RPC call itself fails")
	}
}

func TestCreateItem_UnmarshalableItemReturnsError(t *testing.T) {
	fake := &fakeESClient{createResp: &gistproto.CreateItemResponse{}}
	svc := newTestService(fake)

	// A function value cannot be JSON-marshaled - this should fail before
	// ever calling the RPC.
	_, _, err := svc.CreateItem(context.Background(), "my-index", "", func() {})
	if err == nil {
		t.Fatal("expected an error encoding an unmarshalable item")
	}
	if fake.lastCreate != nil {
		t.Fatal("expected the RPC to never be called when encoding fails")
	}
}

func TestIndexItem_EncodesItemAndSendsRequestWithID(t *testing.T) {
	fake := &fakeESClient{indexResp: &gistproto.IndexItemResponse{}}
	svc := newTestService(fake)

	err := svc.IndexItem(context.Background(), "my-index", "w-1", sampleItem{Name: "widget", Age: 3})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if fake.lastIndex.GetServiceId() != "es1" {
		t.Fatalf("expected service id es1, got %q", fake.lastIndex.GetServiceId())
	}
	if fake.lastIndex.GetIndex() != "my-index" {
		t.Fatalf("expected index 'my-index', got %q", fake.lastIndex.GetIndex())
	}
	if fake.lastIndex.GetId() != "w-1" {
		t.Errorf("expected id w-1 to be sent, got %q", fake.lastIndex.GetId())
	}

	var decoded sampleItem
	if err := json.Unmarshal(fake.lastIndex.GetItemJson(), &decoded); err != nil {
		t.Fatalf("could not decode sent item_json: %v", err)
	}
	if decoded != (sampleItem{Name: "widget", Age: 3}) {
		t.Fatalf("expected item to round-trip through JSON encoding, got %+v", decoded)
	}
}

func TestIndexItem_WireErrorBecomesError(t *testing.T) {
	fake := &fakeESClient{indexResp: &gistproto.IndexItemResponse{ErrorCode: "internal", ErrorMessage: "cluster unavailable"}}
	svc := newTestService(fake)

	err := svc.IndexItem(context.Background(), "my-index", "w-1", sampleItem{Name: "x"})
	if err == nil {
		t.Fatal("expected an error when the wire response carries an error_code")
	}
}

func TestIndexItem_TransportErrorPropagates(t *testing.T) {
	fake := &fakeESClient{indexErr: context.DeadlineExceeded}
	svc := newTestService(fake)

	err := svc.IndexItem(context.Background(), "my-index", "w-1", sampleItem{Name: "x"})
	if err == nil {
		t.Fatal("expected an error when the RPC call itself fails")
	}
}

func TestIndexItem_UnmarshalableItemReturnsError(t *testing.T) {
	fake := &fakeESClient{indexResp: &gistproto.IndexItemResponse{}}
	svc := newTestService(fake)

	// A function value cannot be JSON-marshaled - this should fail before
	// ever calling the RPC.
	err := svc.IndexItem(context.Background(), "my-index", "w-1", func() {})
	if err == nil {
		t.Fatal("expected an error encoding an unmarshalable item")
	}
	if fake.lastIndex != nil {
		t.Fatal("expected the RPC to never be called when encoding fails")
	}
}

func TestGetItem_FoundDecodesIntoOut(t *testing.T) {
	fake := &fakeESClient{getResp: &gistproto.GetItemResponse{Found: true, ItemJson: []byte(`{"name":"widget","age":3}`)}}
	svc := newTestService(fake)

	var out sampleItem
	found, err := svc.GetItem(context.Background(), "my-index", "w-1", &out)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found {
		t.Fatal("expected found=true")
	}
	if out != (sampleItem{Name: "widget", Age: 3}) {
		t.Errorf("out = %+v, want decoded item", out)
	}
	if fake.lastGet.GetIndex() != "my-index" || fake.lastGet.GetId() != "w-1" {
		t.Errorf("unexpected request: %+v", fake.lastGet)
	}
}

func TestGetItem_NotFoundLeavesOutUntouched(t *testing.T) {
	fake := &fakeESClient{getResp: &gistproto.GetItemResponse{Found: false}}
	svc := newTestService(fake)

	out := sampleItem{Name: "unchanged"}
	found, err := svc.GetItem(context.Background(), "my-index", "missing", &out)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Error("expected found=false")
	}
	if out.Name != "unchanged" {
		t.Errorf("expected out to be left untouched, got %+v", out)
	}
}

func TestGetItem_WireErrorBecomesError(t *testing.T) {
	fake := &fakeESClient{getResp: &gistproto.GetItemResponse{ErrorCode: "internal", ErrorMessage: "boom"}}
	svc := newTestService(fake)

	var out sampleItem
	if _, err := svc.GetItem(context.Background(), "my-index", "w-1", &out); err == nil {
		t.Fatal("expected an error when the wire response carries an error_code")
	}
}

func TestUpdateItem_EncodesDocAndSendsRequest(t *testing.T) {
	fake := &fakeESClient{updateResp: &gistproto.UpdateItemResponse{Found: true}}
	svc := newTestService(fake)

	found, err := svc.UpdateItem(context.Background(), "my-index", "w-1", map[string]any{"price": 12.5})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found {
		t.Error("expected found=true")
	}
	var decoded map[string]any
	if err := json.Unmarshal(fake.lastUpdate.GetItemJson(), &decoded); err != nil {
		t.Fatalf("could not decode sent doc: %v", err)
	}
	if decoded["price"] != 12.5 {
		t.Errorf("decoded = %v, want price=12.5", decoded)
	}
}

func TestUpdateItem_NotFound(t *testing.T) {
	fake := &fakeESClient{updateResp: &gistproto.UpdateItemResponse{Found: false}}
	svc := newTestService(fake)

	found, err := svc.UpdateItem(context.Background(), "my-index", "missing", map[string]any{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Error("expected found=false")
	}
}

func TestDeleteItem_SendsRequestAndReportsFound(t *testing.T) {
	fake := &fakeESClient{deleteResp: &gistproto.DeleteItemResponse{Found: true}}
	svc := newTestService(fake)

	found, err := svc.DeleteItem(context.Background(), "my-index", "w-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found {
		t.Error("expected found=true")
	}
	if fake.lastDelete.GetIndex() != "my-index" || fake.lastDelete.GetId() != "w-1" {
		t.Errorf("unexpected request: %+v", fake.lastDelete)
	}
}

func TestDeleteItem_WireErrorBecomesError(t *testing.T) {
	fake := &fakeESClient{deleteResp: &gistproto.DeleteItemResponse{ErrorCode: "internal", ErrorMessage: "boom"}}
	svc := newTestService(fake)

	if _, err := svc.DeleteItem(context.Background(), "my-index", "w-1"); err == nil {
		t.Fatal("expected an error when the wire response carries an error_code")
	}
}

func TestBulkIndex_EncodesItemsAndDecodesResults(t *testing.T) {
	fake := &fakeESClient{bulkResp: &gistproto.BulkIndexResponse{
		HasErrors: true,
		Results: []*gistproto.BulkIndexResult{
			{Id: "w-1", Status: 201},
			{Id: "w-2", Status: 400, Error: "mapper_parsing_exception"},
		},
	}}
	svc := newTestService(fake)

	hasErrors, results, err := svc.BulkIndex(context.Background(), "my-index", []BulkItem{
		{ID: "w-1", Item: sampleItem{Name: "a"}},
		{Item: sampleItem{Name: "b"}}, // no id - auto-generated
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hasErrors {
		t.Error("expected hasErrors=true")
	}
	if len(results) != 2 || results[0].ID != "w-1" || results[0].Status != 201 {
		t.Errorf("results[0] = %+v", results)
	}
	if results[1].ID != "w-2" || results[1].Error == "" {
		t.Errorf("results[1] = %+v", results[1])
	}

	if len(fake.lastBulk.GetItems()) != 2 {
		t.Fatalf("expected 2 items sent, got %d", len(fake.lastBulk.GetItems()))
	}
	if fake.lastBulk.GetItems()[0].GetId() != "w-1" {
		t.Errorf("first item id = %q, want w-1", fake.lastBulk.GetItems()[0].GetId())
	}
	if fake.lastBulk.GetItems()[1].GetId() != "" {
		t.Errorf("second item id = %q, want empty (auto-generated)", fake.lastBulk.GetItems()[1].GetId())
	}
	var decoded sampleItem
	if err := json.Unmarshal(fake.lastBulk.GetItems()[0].GetItemJson(), &decoded); err != nil {
		t.Fatalf("could not decode first item: %v", err)
	}
	if decoded.Name != "a" {
		t.Errorf("first item = %+v, want Name=a", decoded)
	}
}

func TestBulkIndex_UnmarshalableItemReturnsErrorBeforeCallingRPC(t *testing.T) {
	fake := &fakeESClient{bulkResp: &gistproto.BulkIndexResponse{}}
	svc := newTestService(fake)

	_, _, err := svc.BulkIndex(context.Background(), "my-index", []BulkItem{{Item: func() {}}})
	if err == nil {
		t.Fatal("expected an error encoding an unmarshalable item")
	}
	if fake.lastBulk != nil {
		t.Fatal("expected the RPC to never be called when encoding fails")
	}
}

func TestBulkIndex_WireErrorBecomesError(t *testing.T) {
	fake := &fakeESClient{bulkResp: &gistproto.BulkIndexResponse{ErrorCode: "internal", ErrorMessage: "boom"}}
	svc := newTestService(fake)

	_, _, err := svc.BulkIndex(context.Background(), "my-index", []BulkItem{{Item: sampleItem{}}})
	if err == nil {
		t.Fatal("expected an error when the wire response carries an error_code")
	}
}

func TestSearch_EncodesQueryAndDecodesHits(t *testing.T) {
	responseJSON := []byte(`{
		"hits": {
			"total": {"value": 2},
			"hits": [
				{"_id": "w-1", "_score": 1.5, "_source": {"name": "sprocket"}},
				{"_id": "w-2", "_score": 0.8, "_source": {"name": "widget"}}
			]
		}
	}`)
	fake := &fakeESClient{searchResp: &gistproto.SearchResponse{ResponseJson: responseJSON}}
	svc := newTestService(fake)

	query := map[string]any{"query": map[string]any{"match": map[string]any{"name": "sprocket"}}}
	result, err := svc.Search(context.Background(), "my-index", query)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Total != 2 {
		t.Errorf("Total = %d, want 2", result.Total)
	}
	if len(result.Hits) != 2 {
		t.Fatalf("expected 2 hits, got %d", len(result.Hits))
	}
	if result.Hits[0].ID != "w-1" || result.Hits[0].Score != 1.5 {
		t.Errorf("Hits[0] = %+v", result.Hits[0])
	}
	var source sampleItem
	if err := json.Unmarshal(result.Hits[0].Source, &source); err != nil {
		t.Fatalf("could not decode Source: %v", err)
	}
	if source.Name != "sprocket" {
		t.Errorf("Source = %+v, want Name=sprocket", source)
	}

	var sentQuery map[string]any
	if err := json.Unmarshal(fake.lastSearch.GetQueryJson(), &sentQuery); err != nil {
		t.Fatalf("could not decode sent query: %v", err)
	}
	if fake.lastSearch.GetIndex() != "my-index" {
		t.Errorf("index = %q, want my-index", fake.lastSearch.GetIndex())
	}
}

func TestSearch_WireErrorBecomesError(t *testing.T) {
	fake := &fakeESClient{searchResp: &gistproto.SearchResponse{ErrorCode: "internal", ErrorMessage: "boom"}}
	svc := newTestService(fake)

	if _, err := svc.Search(context.Background(), "my-index", map[string]any{}); err == nil {
		t.Fatal("expected an error when the wire response carries an error_code")
	}
}

func TestSearch_TransportErrorPropagates(t *testing.T) {
	fake := &fakeESClient{searchErr: context.DeadlineExceeded}
	svc := newTestService(fake)

	if _, err := svc.Search(context.Background(), "my-index", map[string]any{}); err == nil {
		t.Fatal("expected an error when the RPC call itself fails")
	}
}
