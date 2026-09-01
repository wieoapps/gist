package gistpostgresclient

import (
	"context"
	"errors"
	"testing"

	"github.com/wieoapps/gist"
	gistproto "github.com/wieoapps/gist/proto"
	"github.com/wieoapps/gist/internal/rpcconn"
)

func TestInTransaction_CommitsOnSuccess(t *testing.T) {
	fake := &fakePGClient{}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{PG: fake})
	svc := NewService(server, "svc")

	err := svc.InTransaction(context.Background(), func(tr *Transaction) error {
		_, err := Find[widgetModel](tr)
		return err
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.commitReq == nil {
		t.Fatal("expected InTransaction to commit when fn returns nil")
	}
	if fake.rollbackReq != nil {
		t.Fatal("expected no rollback when fn returns nil")
	}
}

func TestInTransaction_RollsBackOnError_AndReturnsOriginalError(t *testing.T) {
	fake := &fakePGClient{}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{PG: fake})
	svc := NewService(server, "svc")
	wantErr := errors.New("business logic said no")

	err := svc.InTransaction(context.Background(), func(tr *Transaction) error {
		if _, err := Find[widgetModel](tr); err != nil {
			return err
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected the original error back, got %v", err)
	}
	if fake.rollbackReq == nil {
		t.Fatal("expected InTransaction to roll back when fn returns an error")
	}
	if fake.commitReq != nil {
		t.Fatal("expected no commit when fn returns an error")
	}
}

func TestInTransaction_NoOpOnEmptyClosure_SendsNoRPCAtAll(t *testing.T) {
	fake := &fakePGClient{}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{PG: fake})
	svc := NewService(server, "svc")

	if err := svc.InTransaction(context.Background(), func(tr *Transaction) error { return nil }); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.repoReq != nil || fake.commitReq != nil || fake.rollbackReq != nil {
		t.Fatal("expected a closure that runs no op to send no RPC at all - Commit is a local no-op on an unused Transaction")
	}
}

func TestInReadTransaction_SetsReadOnly(t *testing.T) {
	fake := &fakePGClient{}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{PG: fake})
	svc := NewService(server, "svc")

	err := svc.InReadTransaction(context.Background(), func(tr *Transaction) error {
		_, err := Find[widgetModel](tr)
		return err
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !fake.repoReq.GetBegin().GetReadOnly() {
		t.Fatal("expected read_only=true for InReadTransaction")
	}
}

func TestInNamedTransaction_ForwardsName(t *testing.T) {
	fake := &fakePGClient{}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{PG: fake})
	svc := NewService(server, "svc")

	err := svc.InNamedTransaction(context.Background(), "my-tx", func(tr *Transaction) error {
		_, err := Find[widgetModel](tr)
		return err
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.repoReq.GetBegin().GetName() != "my-tx" {
		t.Fatalf("expected name forwarded, got %q", fake.repoReq.GetBegin().GetName())
	}
}

func TestCloseAfter_FirstOp_FoldsBeginAndEndIntoOneCall(t *testing.T) {
	fake := &fakePGClient{}
	tr := newFakeTransaction(t, fake)

	tr.CloseAfter(Rollback)
	if _, err := Find[widgetModel](tr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if fake.repoReq.GetBegin() == nil {
		t.Fatal("expected the first op to still carry Begin")
	}
	if fake.repoReq.GetEnd().GetAction() != gistproto.EndAction_END_ACTION_ROLLBACK {
		t.Fatalf("expected the same call to carry End(ROLLBACK), got %v", fake.repoReq.GetEnd())
	}
	if tr.handle != "" {
		t.Fatalf("expected the transaction to stay closed (no handle) after a CloseAfter'd call, got %q", tr.handle)
	}
	if err := tr.Commit(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.commitReq != nil {
		t.Fatal("expected Commit after a CloseAfter'd op to send no RPC - the transaction is already closed")
	}
}

func TestCloseAfter_LaterOp_ClearsHandleAndSuppressesTrailingClose(t *testing.T) {
	fake := &fakePGClient{}
	tr := newFakeTransaction(t, fake)

	// First op opens the transaction normally (no CloseAfter yet).
	if _, err := Find[widgetModel](tr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	openHandle := tr.handle
	if openHandle == "" {
		t.Fatal("expected the first op to have opened a real handle")
	}

	// Second op is the last one - fold the close into it.
	tr.CloseAfter(CommitIfOK)
	if _, err := Update(tr, widgetModel{Name: "renamed"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(fake.repoReqs) != 2 {
		t.Fatalf("expected exactly 2 Repo calls, got %d", len(fake.repoReqs))
	}
	secondReq := fake.repoReqs[1]
	if secondReq.GetTransactionHandle() != openHandle {
		t.Fatalf("expected the second call to reuse the open handle, got %q", secondReq.GetTransactionHandle())
	}
	if secondReq.GetEnd() == nil {
		t.Fatal("expected the second call to carry End")
	}
	if tr.handle != "" {
		t.Fatalf("expected the transaction to be marked closed after the CloseAfter'd call, got %q", tr.handle)
	}

	if err := tr.Commit(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.commitReq != nil || fake.rollbackReq != nil {
		t.Fatal("expected no trailing Commit/Rollback RPC after a CloseAfter'd op already closed the transaction")
	}
}

func TestInTransaction_FindThenCloseAfterUpdate_SendsExactlyTwoRepoCallsNoTrailingClose(t *testing.T) {
	fake := &fakePGClient{}
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{PG: fake})
	svc := NewService(server, "svc")

	err := svc.InTransaction(context.Background(), func(tr *Transaction) error {
		if _, err := Find[widgetModel](tr); err != nil {
			return err
		}
		tr.CloseAfter(CommitIfOK) // this Update is the last op - fold the close into it
		_, err := Update(tr, widgetModel{Name: "renamed"})
		return err
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(fake.repoReqs) != 2 {
		t.Fatalf("expected exactly 2 Repo calls (Find, Update-with-end), got %d", len(fake.repoReqs))
	}
	if fake.repoReqs[0].GetEnd() != nil {
		t.Fatal("expected the first call (Find) to not carry End")
	}
	if fake.repoReqs[1].GetEnd() == nil {
		t.Fatal("expected the second call (Update) to carry End")
	}
	// InTransaction's own trailing Commit/Rollback must not fire an RPC -
	// the transaction was already closed by the CloseAfter'd Update.
	if fake.commitReq != nil || fake.rollbackReq != nil {
		t.Fatal("expected InTransaction's trailing close to be a local no-op, not a third RPC")
	}
}
