package gistpostgresclient

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/wieoapps/gist"
	gistproto "github.com/wieoapps/gist/proto"
	"github.com/wieoapps/gist/internal/rpcconn"
)

type widgetModel struct {
	ID   int    `source:"public.widgets" db:"id" json:"id"`
	Name string `db:"name" json:"name"`
}

// newFakeTransaction builds a *Transaction bound to a fresh fakePGClient,
// exactly the way NewService(server, id).NewTransaction(ctx) would in real
// use - going through the package's own public API (rather than a direct
// struct literal, which couldn't set Transaction's unexported fields from
// this _test.go file anyway). NewTransaction itself sends no RPC (see
// service.go's begin) - tests that care about the Begin a transaction's
// first op sends, or about Commit/Rollback actually reaching the wire,
// perform that first op themselves; see TestFind_FirstOp_* and the
// TestTransaction_Commit_/Rollback_ tests below.
func newFakeTransaction(t *testing.T, fake *fakePGClient) *Transaction {
	t.Helper()
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{PG: fake})
	svc := NewService(server, "test-service")
	tr, err := svc.NewTransaction(context.Background())
	if err != nil {
		t.Fatalf("NewTransaction: %v", err)
	}
	return tr
}

func TestNewTransaction_SendsNoRPCUntilFirstOp(t *testing.T) {
	fake := &fakePGClient{}
	newFakeTransaction(t, fake)
	if fake.beginReq != nil || fake.repoReq != nil {
		t.Fatal("expected NewTransaction alone to send no RPC")
	}
}

func TestFind_FirstOp_SendsBeginWithServiceID(t *testing.T) {
	fake := &fakePGClient{}
	tr := newFakeTransaction(t, fake)

	if _, err := Find[widgetModel](tr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	begin := fake.repoReq.GetBegin()
	if begin == nil {
		t.Fatal("expected the first op on a fresh transaction to carry Begin")
	}
	if begin.GetServiceId() != "test-service" {
		t.Fatalf("expected service_id 'test-service', got %q", begin.GetServiceId())
	}
	if begin.GetReadOnly() {
		t.Fatal("NewTransaction (not NewReadTransaction) must send read_only=false")
	}
	if tr.handle != "fake-handle" {
		t.Fatalf("expected the transaction to capture the handle the server opened, got %q", tr.handle)
	}
}

func TestFind_FirstOp_ReadTransactionSetsReadOnly(t *testing.T) {
	fake := &fakePGClient{}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{PG: fake})
	svc := NewService(server, "svc")
	tr, err := svc.NewReadTransaction(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := Find[widgetModel](tr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !fake.repoReq.GetBegin().GetReadOnly() {
		t.Fatal("expected read_only=true for NewReadTransaction")
	}
}

func TestFind_SecondOp_ReusesHandleWithoutBeginAgain(t *testing.T) {
	fake := &fakePGClient{}
	tr := newFakeTransaction(t, fake)

	if _, err := Find[widgetModel](tr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	handle := tr.handle
	if handle == "" {
		t.Fatal("expected the first op to have captured a handle")
	}

	if _, err := Find[widgetModel](tr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.repoReq.GetBegin() != nil {
		t.Fatal("expected the second op on the same transaction to not send Begin again")
	}
	if fake.repoReq.GetTransactionHandle() != handle {
		t.Fatalf("expected the second op to reuse the captured handle, got %q", fake.repoReq.GetTransactionHandle())
	}
}

func TestTransaction_Commit_NoOpWhenNeverUsed(t *testing.T) {
	fake := &fakePGClient{}
	tr := newFakeTransaction(t, fake)
	if err := tr.Commit(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.commitReq != nil {
		t.Fatal("expected Commit on a transaction that never ran an op to send no RPC")
	}
}

func TestTransaction_Rollback_NoOpWhenNeverUsed(t *testing.T) {
	fake := &fakePGClient{}
	tr := newFakeTransaction(t, fake)
	if err := tr.Rollback(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.rollbackReq != nil {
		t.Fatal("expected Rollback on a transaction that never ran an op to send no RPC")
	}
}

func TestTransaction_Commit_SendsHandle(t *testing.T) {
	fake := &fakePGClient{}
	tr := newFakeTransaction(t, fake)
	if _, err := Find[widgetModel](tr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := tr.Commit(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.commitReq.GetTransactionHandle() != tr.handle {
		t.Fatalf("expected Commit to send the transaction's own handle, got %q", fake.commitReq.GetTransactionHandle())
	}
}

func TestTransaction_Commit_ServerErrorCode_SurfacesAsError(t *testing.T) {
	fake := &fakePGClient{}
	tr := newFakeTransaction(t, fake)
	if _, err := Find[widgetModel](tr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fake.commitResp = &gistproto.Ack{ErrorCode: "internal", ErrorMessage: "boom"}

	if err := tr.Commit(); err == nil {
		t.Fatal("expected an error when the server returns a non-empty error_code")
	}
}

func TestTransaction_Rollback(t *testing.T) {
	fake := &fakePGClient{}
	tr := newFakeTransaction(t, fake)
	if _, err := Find[widgetModel](tr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := tr.Rollback(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.rollbackReq.GetTransactionHandle() != tr.handle {
		t.Fatalf("expected Rollback to send the transaction's handle, got %q", fake.rollbackReq.GetTransactionHandle())
	}
}

func TestFind_SendsCorrectOpSchemaAndConditions(t *testing.T) {
	fake := &fakePGClient{}
	tr := newFakeTransaction(t, fake)

	rowJSON, _ := json.Marshal(map[string]any{"id": 1, "name": "widget"})
	fake.repoResp = &gistproto.RepoResponse{RowsJson: [][]byte{rowJSON}}

	results, err := Find[widgetModel](tr, WithConditions(NewCondition("id", Equal, 1)), WithLimit(10))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if fake.repoReq.GetOp() != gistproto.RepoOp_FIND {
		t.Fatalf("expected RepoOp_FIND, got %v", fake.repoReq.GetOp())
	}
	if fake.repoReq.GetBegin() == nil {
		t.Fatal("expected the first op on a fresh transaction to carry Begin")
	}
	if len(fake.repoReq.GetSchema().GetSources()) != 2 || fake.repoReq.GetSchema().GetSources()[1] != "widgets" {
		t.Fatalf("expected the schema reflected from widgetModel's source tag, got %v", fake.repoReq.GetSchema().GetSources())
	}
	if fake.repoReq.GetOptions().GetLimit() != 10 {
		t.Fatalf("expected limit=10 forwarded, got %d", fake.repoReq.GetOptions().GetLimit())
	}
	if len(fake.repoReq.GetOptions().GetConditions()) != 1 {
		t.Fatalf("expected 1 condition forwarded, got %d", len(fake.repoReq.GetOptions().GetConditions()))
	}

	if len(results) != 1 || results[0].ID != 1 || results[0].Name != "widget" {
		t.Fatalf("expected the scripted row decoded into a widgetModel, got %+v", results)
	}
}

func TestFind_ServerError_PropagatesWithContext(t *testing.T) {
	fake := &fakePGClient{repoResp: &gistproto.RepoResponse{ErrorCode: "internal", ErrorMessage: "db exploded"}}
	tr := newFakeTransaction(t, fake)
	_, err := Find[widgetModel](tr)
	if err == nil {
		t.Fatal("expected an error when the server response carries an error_code")
	}
}

func TestFindOne_NoRows_ReturnsNilNotError(t *testing.T) {
	fake := &fakePGClient{repoResp: &gistproto.RepoResponse{}}
	tr := newFakeTransaction(t, fake)
	result, err := FindOne[widgetModel](tr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil for no matching rows, got %+v", result)
	}
	if fake.repoReq.GetOp() != gistproto.RepoOp_FIND_ONE {
		t.Fatalf("expected RepoOp_FIND_ONE, got %v", fake.repoReq.GetOp())
	}
}

func TestCount_ReturnsScriptedCount(t *testing.T) {
	fake := &fakePGClient{repoResp: &gistproto.RepoResponse{Count: 42}}
	tr := newFakeTransaction(t, fake)
	n, err := Count[widgetModel](tr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 42 {
		t.Fatalf("expected 42, got %d", n)
	}
	if fake.repoReq.GetOp() != gistproto.RepoOp_COUNT {
		t.Fatalf("expected RepoOp_COUNT, got %v", fake.repoReq.GetOp())
	}
}

func TestExists_ReturnsScriptedBool(t *testing.T) {
	fake := &fakePGClient{repoResp: &gistproto.RepoResponse{Exists: true}}
	tr := newFakeTransaction(t, fake)
	exists, err := Exists[widgetModel](tr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !exists {
		t.Fatal("expected true")
	}
	if fake.repoReq.GetOp() != gistproto.RepoOp_EXISTS {
		t.Fatalf("expected RepoOp_EXISTS, got %v", fake.repoReq.GetOp())
	}
}

func TestSave_EncodesEveryRowAndSumsAffected(t *testing.T) {
	fake := &fakePGClient{repoResp: &gistproto.RepoResponse{Affected: 2}}
	tr := newFakeTransaction(t, fake)

	n, err := Save(tr, widgetModel{ID: 1, Name: "a"}, widgetModel{ID: 2, Name: "b"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected affected=2, got %d", n)
	}
	if fake.repoReq.GetOp() != gistproto.RepoOp_SAVE {
		t.Fatalf("expected RepoOp_SAVE, got %v", fake.repoReq.GetOp())
	}
	if len(fake.repoReq.GetRows()) != 2 {
		t.Fatalf("expected 2 rows sent, got %d", len(fake.repoReq.GetRows()))
	}
	var row0 map[string]any
	_ = json.Unmarshal(fake.repoReq.GetRows()[0].GetRowJson(), &row0)
	if row0["name"] != "a" {
		t.Fatalf("expected the first row's fields encoded, got %v", row0)
	}
}

func TestSave_NoModels_ReturnsZeroWithoutCallingServer(t *testing.T) {
	fake := &fakePGClient{}
	tr := newFakeTransaction(t, fake)
	n, err := Save(tr)
	if err != nil || n != 0 {
		t.Fatalf("expected (0, nil) for no models, got (%d, %v)", n, err)
	}
	if fake.repoReq != nil {
		t.Fatal("expected no Repo call at all when there are no models to save")
	}
}

func TestSaveWithReturning_DecodesReturnedRows(t *testing.T) {
	rowJSON, _ := json.Marshal(map[string]any{"id": 9, "name": "created"})
	fake := &fakePGClient{repoResp: &gistproto.RepoResponse{RowsJson: [][]byte{rowJSON}, Affected: 1}}
	tr := newFakeTransaction(t, fake)

	// The insert payload's type (a bare struct with no "id") is
	// deliberately independent of the return type (widgetModel) - the
	// exact decoupling fix documented in write.go.
	type widgetInsert struct {
		Name string `db:"name" json:"name"`
	}
	results, affected, err := SaveWithReturning[widgetModel](tr, widgetInsert{Name: "created"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if affected != 1 {
		t.Fatalf("expected affected=1, got %d", affected)
	}
	if len(results) != 1 || results[0].ID != 9 || results[0].Name != "created" {
		t.Fatalf("expected the returned row decoded as widgetModel, got %+v", results)
	}
	if fake.repoReq.GetOp() != gistproto.RepoOp_SAVE_WITH_RETURNING {
		t.Fatalf("expected RepoOp_SAVE_WITH_RETURNING, got %v", fake.repoReq.GetOp())
	}
	// Schema must be reflected from the RETURN type (widgetModel, with
	// its full "public.widgets" source), not the insert payload type
	// (widgetInsert, which has no source tag at all).
	if len(fake.repoReq.GetSchema().GetSources()) != 2 {
		t.Fatalf("expected the schema reflected from widgetModel, got sources %v", fake.repoReq.GetSchema().GetSources())
	}
}

func TestUpdate_SendsPartialRowAndConditions(t *testing.T) {
	fake := &fakePGClient{repoResp: &gistproto.RepoResponse{Affected: 3}}
	tr := newFakeTransaction(t, fake)

	type nameUpdate struct {
		Name string `source:"public.widgets" db:"name" json:"name"`
	}
	n, err := Update(tr, nameUpdate{Name: "renamed"}, WithConditions(NewCondition("id", Equal, 1)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 3 {
		t.Fatalf("expected affected=3, got %d", n)
	}
	if fake.repoReq.GetOp() != gistproto.RepoOp_UPDATE {
		t.Fatalf("expected RepoOp_UPDATE, got %v", fake.repoReq.GetOp())
	}
	if len(fake.repoReq.GetRows()) != 1 {
		t.Fatalf("expected exactly one row (the new values), got %d", len(fake.repoReq.GetRows()))
	}
	if len(fake.repoReq.GetOptions().GetConditions()) != 1 {
		t.Fatalf("expected the WHERE conditions forwarded, got %d", len(fake.repoReq.GetOptions().GetConditions()))
	}
}

func TestUpdateWithReturning_SchemaFromReturnTypeNotPayload(t *testing.T) {
	rowJSON, _ := json.Marshal(map[string]any{"id": 1, "name": "renamed"})
	fake := &fakePGClient{repoResp: &gistproto.RepoResponse{RowsJson: [][]byte{rowJSON}, Affected: 1}}
	tr := newFakeTransaction(t, fake)

	type nameUpdate struct {
		Name string `db:"name" json:"name"`
	}
	results, affected, err := UpdateWithReturning[widgetModel](tr, nameUpdate{Name: "renamed"}, WithConditions(NewCondition("id", Equal, 1)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if affected != 1 || len(results) != 1 || results[0].Name != "renamed" {
		t.Fatalf("got results=%+v affected=%d", results, affected)
	}
	if fake.repoReq.GetOp() != gistproto.RepoOp_UPDATE_WITH_RETURNING {
		t.Fatalf("expected RepoOp_UPDATE_WITH_RETURNING, got %v", fake.repoReq.GetOp())
	}
}

func TestDelete_SendsConditionsAndLimit(t *testing.T) {
	fake := &fakePGClient{repoResp: &gistproto.RepoResponse{Affected: 1}}
	tr := newFakeTransaction(t, fake)

	n, err := Delete[widgetModel](tr, WithConditions(NewCondition("id", Equal, 1)), WithLimit(1))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected affected=1, got %d", n)
	}
	if fake.repoReq.GetOp() != gistproto.RepoOp_DELETE {
		t.Fatalf("expected RepoOp_DELETE, got %v", fake.repoReq.GetOp())
	}
	if fake.repoReq.GetOptions().GetLimit() != 1 {
		t.Fatalf("expected limit=1 forwarded, got %d", fake.repoReq.GetOptions().GetLimit())
	}
}

func TestDeleteWithReturning_DecodesRowsBeforeDeletionConceptually(t *testing.T) {
	rowJSON, _ := json.Marshal(map[string]any{"id": 5, "name": "gone-soon"})
	fake := &fakePGClient{repoResp: &gistproto.RepoResponse{RowsJson: [][]byte{rowJSON}, Affected: 1}}
	tr := newFakeTransaction(t, fake)

	results, affected, err := DeleteWithReturning[widgetModel](tr, WithConditions(NewCondition("id", Equal, 5)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if affected != 1 || len(results) != 1 || results[0].Name != "gone-soon" {
		t.Fatalf("got results=%+v affected=%d", results, affected)
	}
	if fake.repoReq.GetOp() != gistproto.RepoOp_DELETE_WITH_RETURNING {
		t.Fatalf("expected RepoOp_DELETE_WITH_RETURNING, got %v", fake.repoReq.GetOp())
	}
}

func TestFind_WithLockAndRelationConditions_ForwardedToOptions(t *testing.T) {
	fake := &fakePGClient{}
	tr := newFakeTransaction(t, fake)

	_, err := Find[widgetModel](tr, WithLock(), WithRelationConditions("orders", NewCondition("qty", GreaterThan, 0)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !fake.repoReq.GetOptions().GetLock() {
		t.Fatal("expected lock=true forwarded to the wire request")
	}
	if len(fake.repoReq.GetOptions().GetRelationConditions()) != 1 {
		t.Fatalf("expected relation_conditions forwarded, got %v", fake.repoReq.GetOptions().GetRelationConditions())
	}
}
