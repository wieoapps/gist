package gistelasticsearchclient

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/wieoapps/gist"
	"github.com/wieoapps/gist/internal/rpcconn"
	"github.com/wieoapps/gist/proto"
)

type Service struct {
	server    *gist.Server
	serviceID string
}

func NewService(server *gist.Server, serviceID string) *Service {
	return &Service{server: server, serviceID: serviceID}
}

func init() {
	gist.RegisterServiceType(NewService)
}

// CreateItem inserts item into index - never overwrites. With id == "",
// Elasticsearch assigns a new id (returned as the second result; this
// can never fail to create, so created is always true on success). With
// id set, it creates the document only if nothing is already there at
// that id - created reports which case this was; a "conflict" isn't an
// error, the same convention GetItem/DeleteItem use for "not found".
// See IndexItem for create-or-replace at a known id instead.
func (s *Service) CreateItem(ctx context.Context, index, id string, item any) (created bool, actualID string, err error) {
	itemJSON, err := json.Marshal(item)
	if err != nil {
		return false, "", fmt.Errorf("gistelasticsearchclient: could not encode item: %w", err)
	}

	resp, err := rpcconn.MustFor(s.server).Elasticsearch.CreateItem(ctx, &proto.CreateItemRequest{
		ServiceId: s.serviceID,
		Index:     index,
		Id:        id,
		ItemJson:  itemJSON,
	})
	if err != nil {
		return false, "", fmt.Errorf("gistelasticsearchclient: create item: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return false, "", fmt.Errorf("gistelasticsearchclient: create item: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return resp.GetCreated(), resp.GetId(), nil
}

// IndexItem creates or replaces the document at id - a full
// create-or-replace, unlike CreateItem's insert-only semantics. id is
// required: replacing a document whose id you don't already know isn't
// a meaningful operation - use CreateItem (with id == "") if you want
// Elasticsearch to assign one.
func (s *Service) IndexItem(ctx context.Context, index, id string, item any) error {
	itemJSON, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("gistelasticsearchclient: could not encode item: %w", err)
	}

	resp, err := rpcconn.MustFor(s.server).Elasticsearch.IndexItem(ctx, &proto.IndexItemRequest{
		ServiceId: s.serviceID,
		Index:     index,
		Id:        id,
		ItemJson:  itemJSON,
	})
	if err != nil {
		return fmt.Errorf("gistelasticsearchclient: index item: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return fmt.Errorf("gistelasticsearchclient: index item: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return nil
}

// GetItem retrieves the document at id, decoding its stored source into
// out (a pointer, same convention json.Unmarshal uses). found reports
// whether the document existed - out is left untouched when it didn't.
func (s *Service) GetItem(ctx context.Context, index, id string, out any) (found bool, err error) {
	resp, err := rpcconn.MustFor(s.server).Elasticsearch.GetItem(ctx, &proto.GetItemRequest{
		ServiceId: s.serviceID,
		Index:     index,
		Id:        id,
	})
	if err != nil {
		return false, fmt.Errorf("gistelasticsearchclient: get item: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return false, fmt.Errorf("gistelasticsearchclient: get item: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	if !resp.GetFound() {
		return false, nil
	}
	if err := json.Unmarshal(resp.GetItemJson(), out); err != nil {
		return false, fmt.Errorf("gistelasticsearchclient: could not decode item: %w", err)
	}
	return true, nil
}

// UpdateItem merges doc's fields into the existing document at id - a
// partial update, not a full replace (see IndexItem for that). found
// reports whether a document existed to update.
func (s *Service) UpdateItem(ctx context.Context, index, id string, doc any) (found bool, err error) {
	docJSON, err := json.Marshal(doc)
	if err != nil {
		return false, fmt.Errorf("gistelasticsearchclient: could not encode update: %w", err)
	}

	resp, err := rpcconn.MustFor(s.server).Elasticsearch.UpdateItem(ctx, &proto.UpdateItemRequest{
		ServiceId: s.serviceID,
		Index:     index,
		Id:        id,
		ItemJson:  docJSON,
	})
	if err != nil {
		return false, fmt.Errorf("gistelasticsearchclient: update item: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return false, fmt.Errorf("gistelasticsearchclient: update item: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return resp.GetFound(), nil
}

// DeleteItem deletes the document at id. found reports whether a
// document existed to delete.
func (s *Service) DeleteItem(ctx context.Context, index, id string) (found bool, err error) {
	resp, err := rpcconn.MustFor(s.server).Elasticsearch.DeleteItem(ctx, &proto.DeleteItemRequest{
		ServiceId: s.serviceID,
		Index:     index,
		Id:        id,
	})
	if err != nil {
		return false, fmt.Errorf("gistelasticsearchclient: delete item: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return false, fmt.Errorf("gistelasticsearchclient: delete item: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return resp.GetFound(), nil
}

// BulkItem is one document for BulkIndex - ID empty auto-generates an
// id, same as IndexItem.
type BulkItem struct {
	ID   string
	Item any
}

// BulkResult is one BulkItem's own outcome, in request order - the bulk
// API can partially fail (e.g. one document rejected by a strict
// mapping) even when the overall request succeeds, so check each
// result's Error, not just BulkIndex's hasErrors return.
type BulkResult struct {
	ID     string
	Status int
	Error  string
}

// BulkIndex indexes every item into index in one round trip. hasErrors
// mirrors Elasticsearch's own top-level "errors" flag.
func (s *Service) BulkIndex(ctx context.Context, index string, items []BulkItem) (hasErrors bool, results []BulkResult, err error) {
	wireItems := make([]*proto.BulkIndexItem, len(items))
	for i, item := range items {
		itemJSON, encErr := json.Marshal(item.Item)
		if encErr != nil {
			return false, nil, fmt.Errorf("gistelasticsearchclient: could not encode bulk item %d: %w", i, encErr)
		}
		wireItems[i] = &proto.BulkIndexItem{Id: item.ID, ItemJson: itemJSON}
	}

	resp, err := rpcconn.MustFor(s.server).Elasticsearch.BulkIndex(ctx, &proto.BulkIndexRequest{
		ServiceId: s.serviceID,
		Index:     index,
		Items:     wireItems,
	})
	if err != nil {
		return false, nil, fmt.Errorf("gistelasticsearchclient: bulk index: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return false, nil, fmt.Errorf("gistelasticsearchclient: bulk index: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}

	results = make([]BulkResult, len(resp.GetResults()))
	for i, r := range resp.GetResults() {
		results[i] = BulkResult{ID: r.GetId(), Status: int(r.GetStatus()), Error: r.GetError()}
	}
	return resp.GetHasErrors(), results, nil
}

// SearchHit is one matched document.
type SearchHit struct {
	ID     string
	Score  float64
	Source json.RawMessage
}

// SearchResult is Search's decoded response - Total is the number of
// matching documents (which can exceed len(Hits) - see size/from in the
// query), Hits the page actually returned.
type SearchResult struct {
	Total int64
	Hits  []SearchHit
}

// Search runs query - a full Elasticsearch search request body, e.g.
// map[string]any{"query": map[string]any{"match": map[string]any{"name": "sprocket"}}, "size": 10} -
// against index.
func (s *Service) Search(ctx context.Context, index string, query any) (*SearchResult, error) {
	queryJSON, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("gistelasticsearchclient: could not encode query: %w", err)
	}

	resp, err := rpcconn.MustFor(s.server).Elasticsearch.Search(ctx, &proto.SearchRequest{
		ServiceId: s.serviceID,
		Index:     index,
		QueryJson: queryJSON,
	})
	if err != nil {
		return nil, fmt.Errorf("gistelasticsearchclient: search: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return nil, fmt.Errorf("gistelasticsearchclient: search: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}

	var wire struct {
		Hits struct {
			Total struct {
				Value int64 `json:"value"`
			} `json:"total"`
			Hits []struct {
				ID     string          `json:"_id"`
				Score  float64         `json:"_score"`
				Source json.RawMessage `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(resp.GetResponseJson(), &wire); err != nil {
		return nil, fmt.Errorf("gistelasticsearchclient: could not decode search response: %w", err)
	}

	result := &SearchResult{Total: wire.Hits.Total.Value, Hits: make([]SearchHit, len(wire.Hits.Hits))}
	for i, h := range wire.Hits.Hits {
		result.Hits[i] = SearchHit{ID: h.ID, Score: h.Score, Source: h.Source}
	}
	return result, nil
}
