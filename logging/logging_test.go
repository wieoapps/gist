package logging

import (
	"context"
	"encoding/json"
	"testing"

	"google.golang.org/grpc"

	"github.com/wieoapps/gist/internal/rpcconn"
	"github.com/wieoapps/gist/proto"
)

// fakeLoggingClient records the last Log RPC it received, so tests can
// assert exactly what Debug/Info/Warn/Error/Panic (both the standalone
// package-level ones and a Logger built via NewLogger) actually send -
// without a real gist-server to talk to.
type fakeLoggingClient struct {
	req *proto.LogRequest
	err error
}

func (f *fakeLoggingClient) Log(_ context.Context, req *proto.LogRequest, _ ...grpc.CallOption) (*proto.LogAck, error) {
	f.req = req
	if f.err != nil {
		return nil, f.err
	}
	return &proto.LogAck{}, nil
}

func decodedFields(t *testing.T, req *proto.LogRequest) map[string]any {
	t.Helper()
	if len(req.GetFieldsJson()) == 0 {
		return nil
	}
	var fields map[string]any
	if err := json.Unmarshal(req.GetFieldsJson(), &fields); err != nil {
		t.Fatalf("could not decode fields_json: %v", err)
	}
	return fields
}

func TestPackageLevelInfo_SendsLevelMsgAndFields(t *testing.T) {
	fake := &fakeLoggingClient{}
	owner := &struct{}{}
	rpcconn.Register(owner, &rpcconn.Clients{Logging: fake})
	original := defaultOwner
	SetOwner(owner)
	defer SetOwner(original)

	Info("order approved", map[string]any{"order_id": "abc"})

	if fake.req == nil {
		t.Fatal("expected the Log RPC to be called")
	}
	if fake.req.GetLevel() != "info" || fake.req.GetMsg() != "order approved" {
		t.Fatalf("unexpected request: level=%q msg=%q", fake.req.GetLevel(), fake.req.GetMsg())
	}
	if got := decodedFields(t, fake.req); got["order_id"] != "abc" {
		t.Errorf("expected fields to carry order_id=abc, got %v", got)
	}
}

func TestPackageLevelDebugWarnError_SendCorrectLevel(t *testing.T) {
	fake := &fakeLoggingClient{}
	owner := &struct{}{}
	rpcconn.Register(owner, &rpcconn.Clients{Logging: fake})
	original := defaultOwner
	SetOwner(owner)
	defer SetOwner(original)

	Debug("d", nil)
	if fake.req.GetLevel() != "debug" {
		t.Errorf("expected level debug, got %q", fake.req.GetLevel())
	}
	Warn("w", nil)
	if fake.req.GetLevel() != "warn" {
		t.Errorf("expected level warn, got %q", fake.req.GetLevel())
	}
	Error("e", nil)
	if fake.req.GetLevel() != "error" {
		t.Errorf("expected level error, got %q", fake.req.GetLevel())
	}
}

func TestPanic_SendsPanicLevelAndPanicsLocally(t *testing.T) {
	fake := &fakeLoggingClient{}
	owner := &struct{}{}
	rpcconn.Register(owner, &rpcconn.Clients{Logging: fake})
	original := defaultOwner
	SetOwner(owner)
	defer SetOwner(original)

	defer func() {
		if r := recover(); r != "boom" {
			t.Errorf("expected panic(\"boom\"), got %v", r)
		}
		if fake.req == nil || fake.req.GetLevel() != "panic" {
			t.Errorf("expected the Log RPC to have been sent with level panic before panicking, got %v", fake.req)
		}
	}()
	Panic("boom", nil)
}

// TestNewLogger_SendsThroughItsOwnOwner proves a Logger built via
// NewLogger (what Server.Logger is populated with, and therefore what
// sg.Logger ends up calling) sends through the same RPC path as the
// package-level functions, independently of whatever defaultOwner is
// set to.
func TestNewLogger_SendsThroughItsOwnOwner(t *testing.T) {
	fake := &fakeLoggingClient{}
	owner := &struct{}{}
	rpcconn.Register(owner, &rpcconn.Clients{Logging: fake})

	l := NewLogger(owner)
	l.Info("from sg.Logger", map[string]any{"k": "v"})

	if fake.req == nil || fake.req.GetLevel() != "info" || fake.req.GetMsg() != "from sg.Logger" {
		t.Fatalf("unexpected request: %+v", fake.req)
	}
}

// TestSend_NoOwnerRegistered_DoesNotPanic proves a logging call made
// with no owner set (or an owner whose clients were never registered -
// e.g. before gistsdk.Start's dialAdmin has completed) falls back
// silently instead of panicking the caller's own process.
func TestSend_NoOwnerRegistered_DoesNotPanic(t *testing.T) {
	original := defaultOwner
	SetOwner(nil)
	defer SetOwner(original)

	Info("no owner yet", map[string]any{"k": "v"})

	unregisteredOwner := &struct{}{}
	SetOwner(unregisteredOwner)
	Info("owner never registered", nil)
}

// TestSend_RPCError_DoesNotPanic proves a transport-level RPC failure
// also falls back silently.
func TestSend_RPCError_DoesNotPanic(t *testing.T) {
	fake := &fakeLoggingClient{err: context.DeadlineExceeded}
	owner := &struct{}{}
	rpcconn.Register(owner, &rpcconn.Clients{Logging: fake})
	original := defaultOwner
	SetOwner(owner)
	defer SetOwner(original)

	Info("this RPC will fail", map[string]any{"k": "v"})
}
