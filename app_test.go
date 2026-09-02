package gist

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wieoapps/gist/proto"
)

// TestReadBubbleUpErrors_TrueWhenSet proves config.json's top-level
// "bubble-up-errors" field is honored by the same read-a-top-level-field
// pattern readLoggerType/readGistBinaryPath already use.
func TestReadBubbleUpErrors_TrueWhenSet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"bubble-up-errors": true}`), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := readBubbleUpErrors(path)
	if err != nil {
		t.Fatalf("readBubbleUpErrors: %v", err)
	}
	if !got {
		t.Error("expected true when config.json sets \"bubble-up-errors\": true")
	}
}

// TestReadBubbleUpErrors_FalseWhenAbsent proves the safe default: a
// config.json with no "bubble-up-errors" key at all must not be
// misread as opting in.
func TestReadBubbleUpErrors_FalseWhenAbsent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := readBubbleUpErrors(path)
	if err != nil {
		t.Fatalf("readBubbleUpErrors: %v", err)
	}
	if got {
		t.Error("expected false when config.json omits \"bubble-up-errors\"")
	}
}

// TestInvoke_UndeclaredMessageError_UsesDeclaredDefaultNotRawError proves
// the information-disclosure fix: a plain error (not a CodeError, so its
// text was never deliberately authored as public) must never have its raw
// Error() text forwarded to the caller, even when its fallback code
// (internalCode) is declared - only the endpoint's own curated default
// message for that code is safe to send.
func TestInvoke_UndeclaredMessageError_UsesDeclaredDefaultNotRawError(t *testing.T) {
	rawDriverDetail := "pq: duplicate key value violates unique constraint \"users_ssn_key\" (table users, column ssn)"
	s := &Server{}
	s.RegisterDispatch("ep", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return nil, errors.New(rawDriverDetail)
	}, map[int32]string{internalCode: "internal server error"})

	cs := &callbackServer{app: s}
	resp, err := cs.Invoke(context.Background(), &proto.InvokeRequest{EndpointId: "ep"})
	if err != nil {
		t.Fatalf("Invoke returned a transport error: %v", err)
	}

	if strings.Contains(resp.GetErrorMessage(), rawDriverDetail) {
		t.Fatalf("ErrorMessage leaked the raw underlying error text: %q", resp.GetErrorMessage())
	}
	if resp.GetErrorMessage() != "internal server error" {
		t.Errorf("ErrorMessage = %q, want the endpoint's declared default %q", resp.GetErrorMessage(), "internal server error")
	}
	if resp.GetErrorCode() != internalCode {
		t.Errorf("ErrorCode = %d, want %d", resp.GetErrorCode(), internalCode)
	}
}

// fakeInvokeLogger records every Error call it received - lets tests
// assert exactly what fields Invoke's swallowed-error path logs,
// without depending on gist-sdk/logging's real RPC-forwarding Logger.
type fakeInvokeLogger struct {
	errorCalls []fakeLogCall
}

type fakeLogCall struct {
	msg    string
	fields map[string]any
}

func (f *fakeInvokeLogger) Debug(msg string, fields map[string]any) {}
func (f *fakeInvokeLogger) Info(msg string, fields map[string]any)  {}
func (f *fakeInvokeLogger) Warn(msg string, fields map[string]any)  {}
func (f *fakeInvokeLogger) Error(msg string, fields map[string]any) {
	f.errorCalls = append(f.errorCalls, fakeLogCall{msg: msg, fields: fields})
}
func (f *fakeInvokeLogger) Panic(msg string, fields map[string]any) {}

// TestInvoke_UndeclaredMessageError_AttachesTraceIDAndLogsIt proves the
// error-reference-ID feature: when the real error is swallowed (no
// CodeError, no BubbleUpErrors), the response carries a non-empty
// ErrorTraceId, and the exact same value was logged alongside the real
// error - so searching a log for it finds the real error behind an
// otherwise-uninformative public message.
func TestInvoke_UndeclaredMessageError_AttachesTraceIDAndLogsIt(t *testing.T) {
	rawDriverDetail := "pq: duplicate key value violates unique constraint \"users_ssn_key\""
	fake := &fakeInvokeLogger{}
	s := &Server{Logger: fake}
	s.RegisterDispatch("ep", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return nil, errors.New(rawDriverDetail)
	}, map[int32]string{internalCode: "internal server error"})

	cs := &callbackServer{app: s}
	resp, err := cs.Invoke(context.Background(), &proto.InvokeRequest{EndpointId: "ep"})
	if err != nil {
		t.Fatalf("Invoke returned a transport error: %v", err)
	}

	traceID := resp.GetErrorTraceId()
	if traceID == "" {
		t.Fatal("expected a non-empty ErrorTraceId when the real error was swallowed")
	}
	if resp.GetErrorMessage() != "internal server error" {
		t.Errorf("ErrorMessage = %q, want the endpoint's declared default", resp.GetErrorMessage())
	}

	if len(fake.errorCalls) != 1 {
		t.Fatalf("expected exactly one Logger.Error call, got %d", len(fake.errorCalls))
	}
	call := fake.errorCalls[0]
	if call.fields["trace_id"] != traceID {
		t.Errorf("expected the logged trace_id (%v) to match the response's ErrorTraceId (%q)", call.fields["trace_id"], traceID)
	}
	if call.fields["error"] == nil {
		t.Error("expected the real error to still be logged as a field")
	}
}

// TestInvoke_UndeclaredMessageError_NoLogger_NoTraceID proves the
// defensive nil-Logger case doesn't fabricate a trace ID that points to
// nothing - if there's no log call to correlate with, there's no point
// handing the customer an ID that will never match anything.
func TestInvoke_UndeclaredMessageError_NoLogger_NoTraceID(t *testing.T) {
	s := &Server{}
	s.RegisterDispatch("ep", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return nil, errors.New("boom")
	}, map[int32]string{internalCode: "internal server error"})

	cs := &callbackServer{app: s}
	resp, err := cs.Invoke(context.Background(), &proto.InvokeRequest{EndpointId: "ep"})
	if err != nil {
		t.Fatalf("Invoke returned a transport error: %v", err)
	}
	if resp.GetErrorTraceId() != "" {
		t.Errorf("expected no ErrorTraceId with no Logger to log it through, got %q", resp.GetErrorTraceId())
	}
}

// TestInvoke_CodeErrorMessage_NoTraceID and
// TestInvoke_BubbleUpErrors_NoTraceID prove a trace ID is only ever
// attached for the swallowed-message case - both other paths already
// forward a genuinely informative message, so there's nothing a trace
// ID would add.
func TestInvoke_CodeErrorMessage_NoTraceID(t *testing.T) {
	fake := &fakeInvokeLogger{}
	s := &Server{Logger: fake}
	s.RegisterDispatch("ep", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return nil, testCodeError{msg: "no account with that email"}
	}, map[int32]string{internalCode: "internal server error"})

	cs := &callbackServer{app: s}
	resp, err := cs.Invoke(context.Background(), &proto.InvokeRequest{EndpointId: "ep"})
	if err != nil {
		t.Fatalf("Invoke returned a transport error: %v", err)
	}
	if resp.GetErrorTraceId() != "" {
		t.Errorf("expected no ErrorTraceId for a CodeError's own message, got %q", resp.GetErrorTraceId())
	}
	if len(fake.errorCalls) != 0 {
		t.Errorf("expected no Logger.Error call for a CodeError's own message, got %d", len(fake.errorCalls))
	}
}

func TestInvoke_BubbleUpErrors_NoTraceID(t *testing.T) {
	fake := &fakeInvokeLogger{}
	s := &Server{Logger: fake, bubbleUpRawErrors: true}
	s.RegisterDispatch("ep", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return nil, errors.New("pq: duplicate key value violates unique constraint")
	}, map[int32]string{internalCode: "internal server error"})

	cs := &callbackServer{app: s}
	resp, err := cs.Invoke(context.Background(), &proto.InvokeRequest{EndpointId: "ep"})
	if err != nil {
		t.Fatalf("Invoke returned a transport error: %v", err)
	}
	if resp.GetErrorTraceId() != "" {
		t.Errorf("expected no ErrorTraceId when BubbleUpErrors already forwards the raw error, got %q", resp.GetErrorTraceId())
	}
}

// TestInvoke_BubbleUpErrors_ForwardsRawErrorText proves the opt-in escape
// hatch: an app that applied BubbleUpErrors gets the pre-fix behavior back
// - a plain error's own text is forwarded verbatim for a declared code,
// same as a CodeError would be.
func TestInvoke_BubbleUpErrors_ForwardsRawErrorText(t *testing.T) {
	rawDriverDetail := "pq: duplicate key value violates unique constraint \"users_ssn_key\""
	s := &Server{bubbleUpRawErrors: true}
	s.RegisterDispatch("ep", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return nil, errors.New(rawDriverDetail)
	}, map[int32]string{internalCode: "internal server error"})

	cs := &callbackServer{app: s}
	resp, err := cs.Invoke(context.Background(), &proto.InvokeRequest{EndpointId: "ep"})
	if err != nil {
		t.Fatalf("Invoke returned a transport error: %v", err)
	}

	if resp.GetErrorMessage() != rawDriverDetail {
		t.Errorf("ErrorMessage = %q, want the raw error text %q (BubbleUpErrors was set)", resp.GetErrorMessage(), rawDriverDetail)
	}
}

// TestBubbleUpErrors_Option_SetsFlag proves the Option itself (the thing a
// customer actually calls, e.g. gistsdk.NewApp(ctx, gistsdk.BubbleUpErrors()))
// does what its doc comment says.
func TestBubbleUpErrors_Option_SetsFlag(t *testing.T) {
	s := &Server{}
	if err := BubbleUpErrors()(s); err != nil {
		t.Fatalf("BubbleUpErrors() returned an error: %v", err)
	}
	if !s.bubbleUpRawErrors {
		t.Error("expected bubbleUpRawErrors to be true after applying BubbleUpErrors()")
	}
}

type testCodeError struct{ msg string }

func (e testCodeError) Error() string     { return e.msg }
func (e testCodeError) StatusCode() int32 { return internalCode }

// TestInvoke_CodeErrorMessage_IsForwarded proves the inverse: a CodeError's
// own message IS meant to be public (the customer explicitly constructed
// it, e.g. via ExpectedError.WithMessage), so it must still be forwarded
// verbatim - the fix only changes behavior for errors that never opted in.
func TestInvoke_CodeErrorMessage_IsForwarded(t *testing.T) {
	s := &Server{}
	s.RegisterDispatch("ep", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return nil, testCodeError{msg: "no account with that email"}
	}, map[int32]string{internalCode: "internal server error"})

	cs := &callbackServer{app: s}
	resp, err := cs.Invoke(context.Background(), &proto.InvokeRequest{EndpointId: "ep"})
	if err != nil {
		t.Fatalf("Invoke returned a transport error: %v", err)
	}

	if resp.GetErrorMessage() != "no account with that email" {
		t.Errorf("ErrorMessage = %q, want the CodeError's own message forwarded verbatim", resp.GetErrorMessage())
	}
}

// TestInvoke_UndeclaredCode_Panics locks in the existing fail-loud safety
// net (unrelated to the message-forwarding fix above, but sharing the same
// declared-error lookup this change touched): a code the endpoint never
// declared must still panic rather than silently reach the caller.
func TestInvoke_UndeclaredCode_Panics(t *testing.T) {
	s := &Server{}
	s.RegisterDispatch("ep", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return nil, errors.New("boom")
	}, map[int32]string{5: "not found"}) // internalCode (13) not declared

	cs := &callbackServer{app: s}
	defer func() {
		if recover() == nil {
			t.Fatal("expected Invoke to panic on an undeclared error code")
		}
	}()
	_, _ = cs.Invoke(context.Background(), &proto.InvokeRequest{EndpointId: "ep"})
}
