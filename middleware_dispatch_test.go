package gist

import (
	"context"
	"errors"
	"testing"

	"github.com/wieoapps/gist/proto"
)

// TestInvokeMiddleware_NotRegistered_ReturnsNotFoundCode mirrors Invoke's
// own "endpoint not registered" behavior for a middleware name nobody
// ever called RegisterMiddlewareDispatch for.
func TestInvokeMiddleware_NotRegistered_ReturnsNotFoundCode(t *testing.T) {
	s := &Server{}
	cs := &callbackServer{app: s}

	resp, err := cs.InvokeMiddleware(context.Background(), &proto.InvokeMiddlewareRequest{Name: "unregistered"})
	if err != nil {
		t.Fatalf("InvokeMiddleware returned a transport error: %v", err)
	}
	if resp.GetErrorCode() != notFoundCode {
		t.Errorf("ErrorCode = %d, want notFoundCode (%d)", resp.GetErrorCode(), notFoundCode)
	}
	if resp.GetErrorMessage() == "" {
		t.Error("expected a non-empty error message naming the missing middleware")
	}
}

// TestInvokeMiddleware_Blocked_PassesThroughResponse proves a middleware
// that decides to block the request has its whole decision (status/
// headers/body) forwarded verbatim - this is what publicapi.Server.
// serve() writes back instead of ever reaching the endpoint's handler.
func TestInvokeMiddleware_Blocked_PassesThroughResponse(t *testing.T) {
	s := &Server{}
	s.RegisterMiddlewareDispatch("auth-check", func(ctx context.Context, req *proto.InvokeMiddlewareRequest) (*proto.InvokeMiddlewareResponse, error) {
		return &proto.InvokeMiddlewareResponse{
			Blocked:    true,
			StatusCode: 401,
			Headers:    map[string]*proto.HeaderValues{"WWW-Authenticate": {Values: []string{"Bearer"}}},
			Body:       []byte(`{"error":"missing token"}`),
		}, nil
	})

	cs := &callbackServer{app: s}
	resp, err := cs.InvokeMiddleware(context.Background(), &proto.InvokeMiddlewareRequest{Name: "auth-check", EndpointId: "get-thing"})
	if err != nil {
		t.Fatalf("InvokeMiddleware returned a transport error: %v", err)
	}
	if !resp.GetBlocked() {
		t.Fatal("expected Blocked to be true")
	}
	if resp.GetStatusCode() != 401 {
		t.Errorf("StatusCode = %d, want 401", resp.GetStatusCode())
	}
	if got := resp.GetHeaders()["WWW-Authenticate"].GetValues(); len(got) != 1 || got[0] != "Bearer" {
		t.Errorf("Headers[WWW-Authenticate] = %v, want [Bearer]", got)
	}
	if string(resp.GetBody()) != `{"error":"missing token"}` {
		t.Errorf("Body = %q, want the middleware's own body", resp.GetBody())
	}
}

// TestInvokeMiddleware_NotBlocked_ProceedsNormally proves the pass-through
// path: Blocked=false regardless of whatever else the middleware set.
func TestInvokeMiddleware_NotBlocked_ProceedsNormally(t *testing.T) {
	s := &Server{}
	s.RegisterMiddlewareDispatch("logger", func(ctx context.Context, req *proto.InvokeMiddlewareRequest) (*proto.InvokeMiddlewareResponse, error) {
		return &proto.InvokeMiddlewareResponse{Blocked: false}, nil
	})

	cs := &callbackServer{app: s}
	resp, err := cs.InvokeMiddleware(context.Background(), &proto.InvokeMiddlewareRequest{Name: "logger"})
	if err != nil {
		t.Fatalf("InvokeMiddleware returned a transport error: %v", err)
	}
	if resp.GetBlocked() {
		t.Error("expected Blocked to be false")
	}
}

// TestInvokeMiddleware_PlainError_FallsBackToInternalMessage_LogsTraceID
// mirrors Invoke's own undeclared-error-message behavior: a plain error
// (not a CodeError) never has its raw text forwarded, and gets a trace
// ID logged alongside the real error instead - middleware has no
// declared-error-code ceremony to opt out of this with (unlike
// EndpointHandler's ExpectedError), so this is the only path for a
// plain error, not one of several.
func TestInvokeMiddleware_PlainError_FallsBackToInternalMessage_LogsTraceID(t *testing.T) {
	rawDetail := "dial tcp: connection refused"
	fake := &fakeInvokeLogger{}
	s := &Server{Logger: fake}
	s.RegisterMiddlewareDispatch("rate-limit", func(ctx context.Context, req *proto.InvokeMiddlewareRequest) (*proto.InvokeMiddlewareResponse, error) {
		return nil, errors.New(rawDetail)
	})

	cs := &callbackServer{app: s}
	resp, err := cs.InvokeMiddleware(context.Background(), &proto.InvokeMiddlewareRequest{Name: "rate-limit"})
	if err != nil {
		t.Fatalf("InvokeMiddleware returned a transport error: %v", err)
	}
	if resp.GetErrorMessage() != "internal error" {
		t.Errorf("ErrorMessage = %q, want the generic fallback, not the raw error", resp.GetErrorMessage())
	}
	if resp.GetErrorCode() != internalCode {
		t.Errorf("ErrorCode = %d, want internalCode (%d)", resp.GetErrorCode(), internalCode)
	}
	traceID := resp.GetErrorTraceId()
	if traceID == "" {
		t.Fatal("expected a non-empty ErrorTraceId when the real error was swallowed")
	}
	if len(fake.errorCalls) != 1 {
		t.Fatalf("expected exactly one Logger.Error call, got %d", len(fake.errorCalls))
	}
	if fake.errorCalls[0].fields["trace_id"] != traceID {
		t.Errorf("expected the logged trace_id (%v) to match the response's ErrorTraceId (%q)", fake.errorCalls[0].fields["trace_id"], traceID)
	}
}

// TestInvokeMiddleware_CodeError_ForwardsOwnMessage proves the inverse:
// a CodeError's own message was deliberately authored as public, so it's
// forwarded verbatim along with its own status code.
func TestInvokeMiddleware_CodeError_ForwardsOwnMessage(t *testing.T) {
	s := &Server{}
	s.RegisterMiddlewareDispatch("auth-check", func(ctx context.Context, req *proto.InvokeMiddlewareRequest) (*proto.InvokeMiddlewareResponse, error) {
		return nil, testCodeError{msg: "token expired"}
	})

	cs := &callbackServer{app: s}
	resp, err := cs.InvokeMiddleware(context.Background(), &proto.InvokeMiddlewareRequest{Name: "auth-check"})
	if err != nil {
		t.Fatalf("InvokeMiddleware returned a transport error: %v", err)
	}
	if resp.GetErrorMessage() != "token expired" {
		t.Errorf("ErrorMessage = %q, want the CodeError's own message forwarded verbatim", resp.GetErrorMessage())
	}
	if resp.GetErrorCode() != internalCode {
		t.Errorf("ErrorCode = %d, want the CodeError's own StatusCode() (%d)", resp.GetErrorCode(), internalCode)
	}
}

// TestInvokeMiddleware_BubbleUpErrors_ForwardsRawErrorText proves the
// same opt-in escape hatch Invoke has applies here too - reusing the
// same Server.bubbleUpRawErrors flag, not a separate one.
func TestInvokeMiddleware_BubbleUpErrors_ForwardsRawErrorText(t *testing.T) {
	rawDetail := "dial tcp: connection refused"
	s := &Server{bubbleUpRawErrors: true}
	s.RegisterMiddlewareDispatch("rate-limit", func(ctx context.Context, req *proto.InvokeMiddlewareRequest) (*proto.InvokeMiddlewareResponse, error) {
		return nil, errors.New(rawDetail)
	})

	cs := &callbackServer{app: s}
	resp, err := cs.InvokeMiddleware(context.Background(), &proto.InvokeMiddlewareRequest{Name: "rate-limit"})
	if err != nil {
		t.Fatalf("InvokeMiddleware returned a transport error: %v", err)
	}
	if resp.GetErrorMessage() != rawDetail {
		t.Errorf("ErrorMessage = %q, want the raw error text %q (BubbleUpErrors was set)", resp.GetErrorMessage(), rawDetail)
	}
	if resp.GetErrorTraceId() != "" {
		t.Errorf("expected no ErrorTraceId when BubbleUpErrors already forwards the raw error, got %q", resp.GetErrorTraceId())
	}
}
