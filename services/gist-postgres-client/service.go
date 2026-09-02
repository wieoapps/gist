package gistpostgresclient

import (
	"context"
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

func (s *Service) NewTransaction(ctx context.Context) (*Transaction, error) {
	return s.begin(ctx, false, "")
}

func (s *Service) NewReadTransaction(ctx context.Context) (*Transaction, error) {
	return s.begin(ctx, true, "")
}

func (s *Service) NewNamedTransaction(ctx context.Context, name string) (*Transaction, error) {
	return s.begin(ctx, false, name)
}

func (s *Service) NewNamedReadTransaction(ctx context.Context, name string) (*Transaction, error) {
	return s.begin(ctx, true, name)
}

// begin never calls BeginTransaction itself - it just records what a
// BeginTransaction call would need. The actual RPC is folded into the
// Transaction's first real op (see repoRequest/captureHandle below), saving
// a full round trip for the common case of one op per transaction. A
// Transaction that never performs an op never opens one server-side at
// all, so ctx is unused here but kept for signature compatibility.
func (s *Service) begin(_ context.Context, readOnly bool, name string) (*Transaction, error) {
	return &Transaction{server: s.server, serviceID: s.serviceID, readOnly: readOnly, name: name}, nil
}

// oneShot builds a Transaction whose single op folds its close action into
// that same op's RepoRequest (via end) instead of a separate Commit/Rollback
// RPC - the basis for this package's *AutoClose functions in repo.go/
// write.go. Unlike begin, this Transaction is never returned to the caller,
// so there's no way to reuse or double-close it once its one op has run.
func (s *Service) oneShot(readOnly bool, action proto.EndAction) *Transaction {
	return &Transaction{server: s.server, serviceID: s.serviceID, readOnly: readOnly, end: &proto.EndTransaction{Action: action}}
}

// InTransaction, InReadTransaction, InNamedTransaction and
// InNamedReadTransaction are the callback form of
// NewTransaction/NewReadTransaction/NewNamedTransaction/
// NewNamedReadTransaction: fn runs against a fresh Transaction that's
// committed if fn returns nil and rolled back otherwise, so a caller can't
// forget to close what it opens or leak it on an early return. Prefer
// these over the New*Transaction family for any transaction with more than
// one op - for exactly one op, prefer this package's *AutoClose functions
// instead, which additionally fold the close into that one op's own RPC.
func (s *Service) InTransaction(ctx context.Context, fn func(tr *Transaction) error) error {
	return s.inTransaction(ctx, false, "", fn)
}

func (s *Service) InReadTransaction(ctx context.Context, fn func(tr *Transaction) error) error {
	return s.inTransaction(ctx, true, "", fn)
}

func (s *Service) InNamedTransaction(ctx context.Context, name string, fn func(tr *Transaction) error) error {
	return s.inTransaction(ctx, false, name, fn)
}

func (s *Service) InNamedReadTransaction(ctx context.Context, name string, fn func(tr *Transaction) error) error {
	return s.inTransaction(ctx, true, name, fn)
}

// inTransaction is InTransaction et al.'s shared implementation. If fn
// already closed tr itself (via CloseAfter on its last op), the trailing
// Commit/Rollback below is already a safe local no-op - Commit/Rollback
// both short-circuit on handle == "", which repoRequest guarantees once an
// End-carrying call has gone out - so no extra bookkeeping is needed here
// to detect that case.
func (s *Service) inTransaction(ctx context.Context, readOnly bool, name string, fn func(tr *Transaction) error) error {
	tr, err := s.begin(ctx, readOnly, name)
	if err != nil {
		return err
	}
	if err := fn(tr); err != nil {
		_ = tr.Rollback()
		return err
	}
	return tr.Commit()
}

// EndAction is the SDK-level counterpart of gist/proto's EndAction enum -
// callers never need to import gist/proto directly to use CloseAfter.
type EndAction string

const (
	// Rollback discards the transaction regardless of its last op's outcome
	// - the natural choice for a transaction with nothing to persist.
	Rollback EndAction = "rollback"
	// Commit persists the transaction regardless of its last op's outcome -
	// the caller's own responsibility to have checked that op's error.
	Commit EndAction = "commit"
	// CommitIfOK commits if the last op succeeded and rolls back otherwise,
	// decided server-side in the same call - the natural choice for a
	// transaction whose last op is a write.
	CommitIfOK EndAction = "commit_if_ok"
)

func (a EndAction) toWire() proto.EndAction {
	switch a {
	case Commit:
		return proto.EndAction_END_ACTION_COMMIT
	case CommitIfOK:
		return proto.EndAction_END_ACTION_COMMIT_IF_OK
	default:
		return proto.EndAction_END_ACTION_ROLLBACK
	}
}

type Transaction struct {
	server    *gist.Server
	serviceID string
	readOnly  bool
	name      string
	handle    string
	end       *proto.EndTransaction // set by Service.oneShot, or by CloseAfter
	endErr    error                 // set by captureHandle if end's action failed server-side
}

// CloseAfter marks t to fold a Commit/Rollback into its very next Repo call
// - whichever op the caller calls next - instead of paying for a separate
// round trip afterward. For a caller inside InTransaction (or using the
// Transaction API directly) that knows in advance which op is its last one,
// e.g. right before the Update in a Find-then-Update sequence. Safe to
// call before a Transaction's very first op too, folding Begin+that op+the
// close into one call - the same mechanism this package's *AutoClose
// functions use internally via Service.oneShot.
func (t *Transaction) CloseAfter(action EndAction) {
	t.end = &proto.EndTransaction{Action: action.toWire()}
}

// repoRequest builds a RepoRequest carrying either t's already-open handle,
// or - if this Transaction hasn't opened one yet - the begin parameters for
// the server to open one as part of this same call. Every Find/Save/Count/
// etc. in repo.go and write.go starts its request this way. end rides
// along unconditionally - it's nil for every Transaction except the
// ephemeral ones built by oneShot, or a Transaction that had CloseAfter
// called on it. Once end is consumed, t.handle is cleared too: this call
// closes the transaction server-side either way (success or failure -
// endTx on the server runs unconditionally whenever a request carries
// end), so there's nothing left this Transaction should ever send again.
func (t *Transaction) repoRequest() *proto.RepoRequest {
	var req *proto.RepoRequest
	if t.handle != "" {
		req = &proto.RepoRequest{TransactionHandle: t.handle}
	} else {
		req = &proto.RepoRequest{
			Begin: &proto.BeginTransactionRequest{ServiceId: t.serviceID, ReadOnly: t.readOnly, Name: t.name},
		}
	}
	if t.end != nil {
		req.End = t.end
		t.end = nil
		t.handle = ""
	}
	return req
}

// captureHandle records a handle the server opened for this call (only
// present when repoRequest sent Begin and no End - the server never returns
// a handle for a transaction it also closed this same call) and records any
// failure of a folded-in end action into t.endErr, for the *AutoClose
// wrappers to check once their underlying op has returned. Call it right
// after checking the RPC's transport error and before checking resp's own
// ErrorCode - even a failed op can have opened a transaction that's now
// genuinely live server-side and still needs an explicit Commit/Rollback.
func (t *Transaction) captureHandle(resp *proto.RepoResponse) {
	if t.handle == "" {
		t.handle = resp.GetTransactionHandle()
	}
	if resp.GetEndErrorCode() != "" {
		t.endErr = fmt.Errorf("%s: %s", resp.GetEndErrorCode(), resp.GetEndErrorMessage())
	}
}

// pg is how every function in this package reaches the raw PostgresServiceClient -
// gistsdk.Server never exposes it directly (see rpcconn's package doc).
func (t *Transaction) pg() proto.PostgresServiceClient {
	return rpcconn.MustFor(t.server).PG
}

func (t *Transaction) Commit() error {
	if t.handle == "" {
		return nil
	}
	ack, err := t.pg().Commit(context.Background(), &proto.TransactionHandle{TransactionHandle: t.handle})
	if err != nil {
		return fmt.Errorf("gistpostgres: commit: %w", err)
	}
	if ack.GetErrorCode() != "" {
		return fmt.Errorf("gistpostgres: commit: %s: %s", ack.GetErrorCode(), ack.GetErrorMessage())
	}
	return nil
}

func (t *Transaction) Rollback() error {
	if t.handle == "" {
		return nil
	}
	ack, err := t.pg().Rollback(context.Background(), &proto.TransactionHandle{TransactionHandle: t.handle})
	if err != nil {
		return fmt.Errorf("gistpostgres: rollback: %w", err)
	}
	if ack.GetErrorCode() != "" {
		return fmt.Errorf("gistpostgres: rollback: %s: %s", ack.GetErrorCode(), ack.GetErrorMessage())
	}
	return nil
}
