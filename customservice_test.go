package gist

import (
	"context"
	"testing"

	gistproto "github.com/wieoapps/gist/proto"
)

type fakeCustomServiceConfig struct {
	IntervalSeconds int `json:"interval-seconds"`
}

// fakeCustomService records every lifecycle call it received, in order,
// so tests can assert Build/PostBuild/Start/Stop actually run in the
// right sequence with the right data - without any real background
// work.
type fakeCustomService struct {
	cfg fakeCustomServiceConfig

	calls          []string
	postBuildErr   error
	startErr       error
	stopErr        error
	readyWasCalled bool
}

func (s *fakeCustomService) PostBuild() error {
	s.calls = append(s.calls, "PostBuild")
	return s.postBuildErr
}

func (s *fakeCustomService) Start(_ context.Context, ready func()) error {
	s.calls = append(s.calls, "Start")
	ready()
	s.readyWasCalled = true
	return s.startErr
}

func (s *fakeCustomService) Stop(_ context.Context) error {
	s.calls = append(s.calls, "Stop")
	return s.stopErr
}

func TestStartCustomService_BuildsPostBuildsAndStarts_InOrder(t *testing.T) {
	var built *fakeCustomService
	s := &Server{}
	err := RegisterCustomService("demo-counter", func(cfg fakeCustomServiceConfig) (*fakeCustomService, error) {
		built = &fakeCustomService{cfg: cfg}
		return built, nil
	})(s)
	if err != nil {
		t.Fatalf("RegisterCustomService option failed: %v", err)
	}

	cs := &callbackServer{app: s}
	resp, err := cs.StartCustomService(context.Background(), &gistproto.StartCustomServiceRequest{
		Kind:       "demo-counter",
		Id:         "primary",
		ConfigJson: []byte(`{"interval-seconds": 5}`),
	})
	if err != nil {
		t.Fatalf("StartCustomService returned a transport error: %v", err)
	}
	if resp.GetErrorCode() != "" {
		t.Fatalf("unexpected error: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}

	if built == nil {
		t.Fatal("expected the build func to have been called")
	}
	if built.cfg.IntervalSeconds != 5 {
		t.Errorf("expected config to decode interval-seconds=5, got %d", built.cfg.IntervalSeconds)
	}
	want := []string{"PostBuild", "Start"}
	if len(built.calls) != len(want) || built.calls[0] != want[0] || built.calls[1] != want[1] {
		t.Errorf("expected calls %v, got %v", want, built.calls)
	}
	if !built.readyWasCalled {
		t.Error("expected ready() to have been called during Start")
	}
}

func TestStartCustomService_UnregisteredKind_ReturnsNotRegistered(t *testing.T) {
	s := &Server{}
	cs := &callbackServer{app: s}
	resp, err := cs.StartCustomService(context.Background(), &gistproto.StartCustomServiceRequest{Kind: "unknown", Id: "x"})
	if err != nil {
		t.Fatalf("StartCustomService returned a transport error: %v", err)
	}
	if resp.GetErrorCode() != notRegisteredCode {
		t.Errorf("expected error code %q, got %q", notRegisteredCode, resp.GetErrorCode())
	}
}

func TestStartCustomService_MalformedConfig_ReturnsInternalError(t *testing.T) {
	s := &Server{}
	err := RegisterCustomService("demo-counter", func(cfg fakeCustomServiceConfig) (*fakeCustomService, error) {
		return &fakeCustomService{cfg: cfg}, nil
	})(s)
	if err != nil {
		t.Fatalf("RegisterCustomService option failed: %v", err)
	}

	cs := &callbackServer{app: s}
	resp, err := cs.StartCustomService(context.Background(), &gistproto.StartCustomServiceRequest{
		Kind: "demo-counter", Id: "primary", ConfigJson: []byte(`not json`),
	})
	if err != nil {
		t.Fatalf("StartCustomService returned a transport error: %v", err)
	}
	if resp.GetErrorCode() != "internal" {
		t.Errorf("expected error code %q for malformed config, got %q", "internal", resp.GetErrorCode())
	}
}

func TestStartCustomService_StartError_ReturnsInternalError_NotTracked(t *testing.T) {
	s := &Server{}
	wantErr := "boom"
	err := RegisterCustomService("demo-counter", func(cfg fakeCustomServiceConfig) (*fakeCustomService, error) {
		return &fakeCustomService{cfg: cfg, startErr: errString(wantErr)}, nil
	})(s)
	if err != nil {
		t.Fatalf("RegisterCustomService option failed: %v", err)
	}

	cs := &callbackServer{app: s}
	resp, err := cs.StartCustomService(context.Background(), &gistproto.StartCustomServiceRequest{Kind: "demo-counter", Id: "primary"})
	if err != nil {
		t.Fatalf("StartCustomService returned a transport error: %v", err)
	}
	if resp.GetErrorCode() != "internal" || resp.GetErrorMessage() != wantErr {
		t.Errorf("expected internal error %q, got %s: %s", wantErr, resp.GetErrorCode(), resp.GetErrorMessage())
	}

	// A failed Start must not be trackable by Stop - nothing to stop.
	stopResp, err := cs.StopCustomService(context.Background(), &gistproto.StopCustomServiceRequest{Kind: "demo-counter", Id: "primary"})
	if err != nil {
		t.Fatalf("StopCustomService returned a transport error: %v", err)
	}
	if stopResp.GetErrorCode() != "" {
		t.Errorf("expected StopCustomService to be a silent no-op for an instance that never started, got %s", stopResp.GetErrorCode())
	}
}

func TestStopCustomService_StopsTheRightInstance_ByID(t *testing.T) {
	instances := map[string]*fakeCustomService{}
	s := &Server{}
	err := RegisterCustomService("demo-counter", func(cfg fakeCustomServiceConfig) (*fakeCustomService, error) {
		svc := &fakeCustomService{cfg: cfg}
		return svc, nil
	})(s)
	if err != nil {
		t.Fatalf("RegisterCustomService option failed: %v", err)
	}

	cs := &callbackServer{app: s}
	for _, id := range []string{"a", "b"} {
		resp, err := cs.StartCustomService(context.Background(), &gistproto.StartCustomServiceRequest{Kind: "demo-counter", Id: id})
		if err != nil || resp.GetErrorCode() != "" {
			t.Fatalf("StartCustomService(%q) failed: err=%v resp=%v", id, err, resp)
		}
	}

	k := s.customServices["demo-counter"]
	k.mu.Lock()
	instances["a"] = k.instances["a"].(*fakeCustomService)
	instances["b"] = k.instances["b"].(*fakeCustomService)
	k.mu.Unlock()

	stopResp, err := cs.StopCustomService(context.Background(), &gistproto.StopCustomServiceRequest{Kind: "demo-counter", Id: "a"})
	if err != nil || stopResp.GetErrorCode() != "" {
		t.Fatalf("StopCustomService(a) failed: err=%v resp=%v", err, stopResp)
	}

	if len(instances["a"].calls) == 0 || instances["a"].calls[len(instances["a"].calls)-1] != "Stop" {
		t.Errorf("expected instance %q to have been stopped, calls=%v", "a", instances["a"].calls)
	}
	for _, c := range instances["b"].calls {
		if c == "Stop" {
			t.Errorf("expected instance %q to be untouched by stopping %q, but it was stopped too", "b", "a")
		}
	}

	k.mu.Lock()
	_, stillTracked := k.instances["a"]
	k.mu.Unlock()
	if stillTracked {
		t.Error("expected instance \"a\" to be removed from tracking after Stop")
	}
}

func TestStopCustomService_UnknownIDOrKind_IsASilentNoOp(t *testing.T) {
	s := &Server{}
	err := RegisterCustomService("demo-counter", func(cfg fakeCustomServiceConfig) (*fakeCustomService, error) {
		return &fakeCustomService{cfg: cfg}, nil
	})(s)
	if err != nil {
		t.Fatalf("RegisterCustomService option failed: %v", err)
	}
	cs := &callbackServer{app: s}

	resp, err := cs.StopCustomService(context.Background(), &gistproto.StopCustomServiceRequest{Kind: "demo-counter", Id: "never-started"})
	if err != nil || resp.GetErrorCode() != "" {
		t.Fatalf("expected a silent no-op for an unknown id, got err=%v resp=%v", err, resp)
	}

	resp, err = cs.StopCustomService(context.Background(), &gistproto.StopCustomServiceRequest{Kind: "unknown-kind", Id: "x"})
	if err != nil || resp.GetErrorCode() != "" {
		t.Fatalf("expected a silent no-op for an unknown kind, got err=%v resp=%v", err, resp)
	}
}

type errString string

func (e errString) Error() string { return string(e) }
