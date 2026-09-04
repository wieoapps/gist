package giststatemachine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"google.golang.org/grpc"

	"github.com/wieoapps/gist"
	"github.com/wieoapps/gist/internal/rpcconn"
	"github.com/wieoapps/gist/proto"
)

type fakeStateMachineClient struct {
	req  *proto.TransitionRequest
	resp *proto.TransitionResponse
	err  error

	graphReq  *proto.GraphRequest
	graphResp *proto.GraphResponse
	graphErr  error

	multiLaneReq  *proto.MultiLaneGraphRequest
	multiLaneResp *proto.MultiLaneGraphResponse
	multiLaneErr  error
}

func (f *fakeStateMachineClient) Transition(_ context.Context, in *proto.TransitionRequest, _ ...grpc.CallOption) (*proto.TransitionResponse, error) {
	f.req = in
	if f.err != nil {
		return nil, f.err
	}
	if f.resp != nil {
		return f.resp, nil
	}
	return &proto.TransitionResponse{NewState: "done"}, nil
}

func (f *fakeStateMachineClient) Graph(_ context.Context, in *proto.GraphRequest, _ ...grpc.CallOption) (*proto.GraphResponse, error) {
	f.graphReq = in
	if f.graphErr != nil {
		return nil, f.graphErr
	}
	if f.graphResp != nil {
		return f.graphResp, nil
	}
	return &proto.GraphResponse{Dot: "digraph {}"}, nil
}

func (f *fakeStateMachineClient) MultiLaneGraph(_ context.Context, in *proto.MultiLaneGraphRequest, _ ...grpc.CallOption) (*proto.MultiLaneGraphResponse, error) {
	f.multiLaneReq = in
	if f.multiLaneErr != nil {
		return nil, f.multiLaneErr
	}
	if f.multiLaneResp != nil {
		return f.multiLaneResp, nil
	}
	return &proto.MultiLaneGraphResponse{Svg: "<svg></svg>"}, nil
}

type testStatable struct {
	State string `json:"state"`
	Name  string `json:"name"`
}

func (s *testStatable) GetState() string      { return s.State }
func (s *testStatable) SetState(state string) { s.State = state }

// testServicesGroup stands in for a customer's servicesGroup type - a
// plain empty struct is enough since gistsdk.BuildServiceGroup only
// needs a struct kind to reflect over, and none of these tests exercise
// any tagged fields on it.
type testServicesGroup struct{}

// newTestService also runs opts (RegisterTriggerFunc results wrapped in
// an Attach call under "svc-1") against the fake server before returning
// the Service - so callers only need to declare their trigger(s), not
// separately attach them.
func newTestService(t *testing.T, fake *fakeStateMachineClient, handlers ...*TriggerHandler[testServicesGroup, *testStatable]) *Service {
	t.Helper()
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{StateMachine: fake})
	if err := Attach("svc-1", handlers...)(server); err != nil {
		t.Fatalf("Attach failed: %v", err)
	}
	return NewService(server, "svc-1")
}

func noopAction(testServicesGroup, context.Context, *testStatable) error { return nil }

func TestTransition_OnActionOnly_SendsCurrentState_AppliesNewState(t *testing.T) {
	fake := &fakeStateMachineClient{}
	approve := RegisterTriggerFunc("approve", noopAction)
	svc := newTestService(t, fake, approve)

	statable := &testStatable{State: "pending", Name: "widget"}
	if err := Transition(context.Background(), svc, "approve", statable); err != nil {
		t.Fatalf("Transition failed: %v", err)
	}

	if fake.req.GetServiceId() != "svc-1" || fake.req.GetTrigger() != "approve" || fake.req.GetCurrentState() != "pending" {
		t.Fatalf("unexpected request: %+v", fake.req)
	}
	if statable.State != "done" {
		t.Fatalf("expected the new state to be applied back onto statable, got %q", statable.State)
	}
}

func TestTransition_UnregisteredTrigger_Errors_RPCNeverAttempted(t *testing.T) {
	fake := &fakeStateMachineClient{}
	approve := RegisterTriggerFunc("approve", noopAction)
	svc := newTestService(t, fake, approve)

	statable := &testStatable{State: "pending"}
	if err := Transition(context.Background(), svc, "reject", statable); err == nil {
		t.Fatal("expected an error for a trigger that was never attached")
	}
	if fake.req != nil {
		t.Fatal("expected the RPC to never be attempted for an unregistered trigger")
	}
}

func TestAttach_NilOnAction_ErrorsAtAttachTime(t *testing.T) {
	server := &gist.Server{}
	broken := &TriggerHandler[testServicesGroup, *testStatable]{trigger: "approve"} // no OnAction
	if err := Attach("svc-1", broken)(server); err == nil {
		t.Fatal("expected Attach to reject a trigger with a nil OnAction")
	}
}

func TestTransition_TransportError_Propagates_OnActionNeverRuns(t *testing.T) {
	fake := &fakeStateMachineClient{err: context.DeadlineExceeded}
	ran := false
	approve := RegisterTriggerFunc("approve", func(testServicesGroup, context.Context, *testStatable) error { ran = true; return nil })
	svc := newTestService(t, fake, approve)

	statable := &testStatable{State: "pending"}
	if err := Transition(context.Background(), svc, "approve", statable); err == nil {
		t.Fatal("expected the transport error to propagate")
	}
	if statable.State != "pending" {
		t.Fatalf("expected state to remain unchanged on error, got %q", statable.State)
	}
	if ran {
		t.Fatal("expected OnAction to never run when the RPC fails")
	}
}

func TestTransition_ServerErrorCode_SurfacesAsError_StateUnchanged(t *testing.T) {
	fake := &fakeStateMachineClient{resp: &proto.TransitionResponse{ErrorCode: "invalid_transition", ErrorMessage: "cannot approve from pending"}}
	approve := RegisterTriggerFunc("approve", noopAction)
	svc := newTestService(t, fake, approve)

	statable := &testStatable{State: "pending"}
	err := Transition(context.Background(), svc, "approve", statable)
	if err == nil {
		t.Fatal("expected an error when the response carries an ErrorCode")
	}
	if statable.State != "pending" {
		t.Fatalf("expected state to remain unchanged when the server reports an error, got %q", statable.State)
	}
}

func TestTransition_PhaseOrder_EnterThenActionThenExit(t *testing.T) {
	fake := &fakeStateMachineClient{}

	var calls []string
	var enterState, actionState, exitState string
	approve := RegisterTriggerFunc("approve",
		func(_ testServicesGroup, _ context.Context, s *testStatable) error {
			calls = append(calls, "action")
			actionState = s.State
			return nil
		},
		OnEnter(
			func(_ testServicesGroup, _ context.Context, s *testStatable) error {
				calls = append(calls, "enter1")
				enterState = s.State
				return nil
			},
			func(testServicesGroup, context.Context, *testStatable) error {
				calls = append(calls, "enter2")
				return nil
			},
		),
		OnExit(
			func(testServicesGroup, context.Context, *testStatable) error {
				calls = append(calls, "exit1")
				return nil
			},
			func(_ testServicesGroup, _ context.Context, s *testStatable) error {
				calls = append(calls, "exit2")
				exitState = s.State
				return nil
			},
		),
	)
	svc := newTestService(t, fake, approve)

	statable := &testStatable{State: "pending"}
	if err := Transition(context.Background(), svc, "approve", statable); err != nil {
		t.Fatalf("Transition failed: %v", err)
	}

	want := []string{"enter1", "enter2", "action", "exit1", "exit2"}
	if len(calls) != len(want) {
		t.Fatalf("expected call order %v, got %v", want, calls)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("expected call order %v, got %v", want, calls)
		}
	}
	if enterState != "pending" {
		t.Fatalf("expected OnEnter to see the pre-transition state, got %q", enterState)
	}
	if actionState != "done" || exitState != "done" {
		t.Fatalf("expected OnAction/OnExit to see the already-applied new state, got action=%q exit=%q", actionState, exitState)
	}
}

func TestTransition_OnEnterError_AbortsBeforeRPC_LaterPhasesNeverRun(t *testing.T) {
	fake := &fakeStateMachineClient{}

	wantErr := errors.New("simulated guard failure")
	actionRan, exitRan := false, false
	approve := RegisterTriggerFunc("approve",
		func(testServicesGroup, context.Context, *testStatable) error { actionRan = true; return nil },
		OnEnter(func(testServicesGroup, context.Context, *testStatable) error { return wantErr }),
		OnExit(func(testServicesGroup, context.Context, *testStatable) error { exitRan = true; return nil }),
	)
	svc := newTestService(t, fake, approve)

	statable := &testStatable{State: "pending"}
	if err := Transition(context.Background(), svc, "approve", statable); !errors.Is(err, wantErr) {
		t.Fatalf("expected OnEnter's own error to propagate, got %v", err)
	}
	if statable.State != "pending" {
		t.Fatalf("expected state to remain unchanged when OnEnter fails, got %q", statable.State)
	}
	if fake.req != nil {
		t.Fatal("expected the RPC to never be attempted when OnEnter fails")
	}
	if actionRan || exitRan {
		t.Fatal("expected OnAction/OnExit to never run when OnEnter fails")
	}
}

func TestTransition_OnActionError_TransitionAlreadyApplied_OnExitNeverRuns(t *testing.T) {
	fake := &fakeStateMachineClient{}

	wantErr := errors.New("simulated action failure")
	exitRan := false
	approve := RegisterTriggerFunc("approve",
		func(testServicesGroup, context.Context, *testStatable) error { return wantErr },
		OnExit(func(testServicesGroup, context.Context, *testStatable) error { exitRan = true; return nil }),
	)
	svc := newTestService(t, fake, approve)

	statable := &testStatable{State: "pending"}
	if err := Transition(context.Background(), svc, "approve", statable); !errors.Is(err, wantErr) {
		t.Fatalf("expected OnAction's error to propagate, got %v", err)
	}
	if statable.State != "done" {
		t.Fatalf("expected the already-applied new state to stick even though OnAction failed, got %q", statable.State)
	}
	if exitRan {
		t.Fatal("expected OnExit to never run when OnAction fails")
	}
}

func TestTransition_OnExitError_Propagates_StateAlreadyApplied(t *testing.T) {
	fake := &fakeStateMachineClient{}

	wantErr := errors.New("simulated exit failure")
	approve := RegisterTriggerFunc("approve", noopAction,
		OnExit(func(testServicesGroup, context.Context, *testStatable) error { return wantErr }),
	)
	svc := newTestService(t, fake, approve)

	statable := &testStatable{State: "pending"}
	if err := Transition(context.Background(), svc, "approve", statable); !errors.Is(err, wantErr) {
		t.Fatalf("expected OnExit's error to propagate, got %v", err)
	}
	if statable.State != "done" {
		t.Fatalf("expected the already-applied new state to stick even though OnExit failed, got %q", statable.State)
	}
}

func TestTransition_PhasesReceiveTheServicesGroup(t *testing.T) {
	fake := &fakeStateMachineClient{}

	var sawSG bool
	approve := RegisterTriggerFunc("approve", func(sg testServicesGroup, _ context.Context, _ *testStatable) error {
		sawSG = true
		_ = sg // the same value on every call - just proving it's reachable, not asserting identity
		return nil
	})
	svc := newTestService(t, fake, approve)

	statable := &testStatable{State: "pending"}
	if err := Transition(context.Background(), svc, "approve", statable); err != nil {
		t.Fatalf("Transition failed: %v", err)
	}
	if !sawSG {
		t.Fatal("expected OnAction to receive the servicesGroup built by Attach")
	}
}

// TestStatable_EmbeddedInCustomerType_GetSetWorkWithoutHandWrittenMethods
// proves the whole point of Statable: a customer's own type just nests
// it and GetState/SetState are simply there, no methods to write.
func TestStatable_EmbeddedInCustomerType_GetSetWorkWithoutHandWrittenMethods(t *testing.T) {
	type customerType struct {
		Statable
		Name string
	}

	var s Statabler = &customerType{Name: "widget"}
	if got := s.GetState(); got != "" {
		t.Errorf("zero-value GetState() = %q, want empty", got)
	}
	s.SetState("draft")
	if got := s.GetState(); got != "draft" {
		t.Errorf("GetState() after SetState(\"draft\") = %q, want \"draft\"", got)
	}
}

// TestStatable_ConcurrentGetSet_NoRace proves GetState/SetState are safe
// to call from multiple goroutines at once - e.g. an HTTP handler
// reading current status while a background job transitions the same
// object. Run with `go test -race`: before Statable's mutex was added,
// this reliably tripped the race detector.
func TestStatable_ConcurrentGetSet_NoRace(t *testing.T) {
	var s Statable
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			s.SetState(fmt.Sprintf("state-%d", i))
		}(i)
		go func() {
			defer wg.Done()
			_ = s.GetState()
		}()
	}
	wg.Wait()
}

func TestGraph_SendsServiceID_ReturnsDot(t *testing.T) {
	fake := &fakeStateMachineClient{graphResp: &proto.GraphResponse{Dot: "digraph { draft -> submitted }"}}
	svc := newTestService(t, fake)

	dot, err := svc.Graph(context.Background())
	if err != nil {
		t.Fatalf("Graph failed: %v", err)
	}
	if fake.graphReq.GetServiceId() != "svc-1" {
		t.Fatalf("unexpected request: %+v", fake.graphReq)
	}
	if dot != "digraph { draft -> submitted }" {
		t.Fatalf("unexpected dot output: %q", dot)
	}
}

func TestGraph_TransportError_Propagates(t *testing.T) {
	fake := &fakeStateMachineClient{graphErr: context.DeadlineExceeded}
	svc := newTestService(t, fake)

	if _, err := svc.Graph(context.Background()); err == nil {
		t.Fatal("expected the transport error to propagate")
	}
}

func TestGraph_ServerErrorCode_SurfacesAsError(t *testing.T) {
	fake := &fakeStateMachineClient{graphResp: &proto.GraphResponse{ErrorCode: "invalid_argument", ErrorMessage: "unknown service svc-1"}}
	svc := newTestService(t, fake)

	if _, err := svc.Graph(context.Background()); err == nil {
		t.Fatal("expected an error when the response carries an ErrorCode")
	}
}

func TestGraphSVG_SendsServiceID_ReturnsSVG(t *testing.T) {
	fake := &fakeStateMachineClient{graphResp: &proto.GraphResponse{Svg: "<svg>...</svg>"}}
	svc := newTestService(t, fake)

	svg, err := svc.GraphSVG(context.Background())
	if err != nil {
		t.Fatalf("GraphSVG failed: %v", err)
	}
	if fake.graphReq.GetServiceId() != "svc-1" {
		t.Fatalf("unexpected request: %+v", fake.graphReq)
	}
	if svg != "<svg>...</svg>" {
		t.Fatalf("unexpected svg output: %q", svg)
	}
}

func TestGraphSVG_TransportError_Propagates(t *testing.T) {
	fake := &fakeStateMachineClient{graphErr: context.DeadlineExceeded}
	svc := newTestService(t, fake)

	if _, err := svc.GraphSVG(context.Background()); err == nil {
		t.Fatal("expected the transport error to propagate")
	}
}

func TestGraph_SendsEveryAttachedTriggerAsImplemented(t *testing.T) {
	fake := &fakeStateMachineClient{}
	approve := RegisterTriggerFunc("approve", noopAction)
	reject := RegisterTriggerFunc("reject", noopAction)
	svc := newTestService(t, fake, approve, reject)

	if _, err := svc.Graph(context.Background()); err != nil {
		t.Fatalf("Graph failed: %v", err)
	}

	got := fake.graphReq.GetImplementedTriggers()
	want := map[string]bool{"approve": true, "reject": true}
	if len(got) != len(want) {
		t.Fatalf("expected %d implemented triggers, got %v", len(want), got)
	}
	for _, trigger := range got {
		if !want[trigger] {
			t.Errorf("unexpected trigger %q in implemented_triggers: %v", trigger, got)
		}
	}
}

func TestGraph_NoTriggersAttached_SendsEmptyImplementedList(t *testing.T) {
	fake := &fakeStateMachineClient{}
	svc := newTestService(t, fake)

	if _, err := svc.Graph(context.Background()); err != nil {
		t.Fatalf("Graph failed: %v", err)
	}
	if len(fake.graphReq.GetImplementedTriggers()) != 0 {
		t.Fatalf("expected no implemented triggers when nothing was Attached, got %v", fake.graphReq.GetImplementedTriggers())
	}
}

func TestMultiLaneGraphSVG_SendsEveryRequestedServiceID_ReturnsSVG(t *testing.T) {
	fake := &fakeStateMachineClient{multiLaneResp: &proto.MultiLaneGraphResponse{Svg: "<svg>multi</svg>"}}
	svc := newTestService(t, fake)

	svg, err := svc.MultiLaneGraphSVG(context.Background(), []string{"svc-1", "svc-2", "svc-3"})
	if err != nil {
		t.Fatalf("MultiLaneGraphSVG failed: %v", err)
	}
	if got := fake.multiLaneReq.GetServiceIds(); len(got) != 3 || got[0] != "svc-1" || got[1] != "svc-2" || got[2] != "svc-3" {
		t.Fatalf("expected service_ids [svc-1 svc-2 svc-3], got %v", got)
	}
	if svg != "<svg>multi</svg>" {
		t.Fatalf("unexpected svg output: %q", svg)
	}
}

func TestMultiLaneGraphSVG_OnlySendsImplementedTriggersForItsOwnServiceID(t *testing.T) {
	// svc's own id is "svc-1" (see newTestService) - only that id's
	// RegisteredStateMachineTriggers has anything to report; the other
	// requested ids get an empty (or absent) entry, exactly as if a
	// completely different process had attached their triggers.
	fake := &fakeStateMachineClient{}
	approve := RegisterTriggerFunc("approve", noopAction)
	svc := newTestService(t, fake, approve)

	if _, err := svc.MultiLaneGraphSVG(context.Background(), []string{"svc-1", "svc-2"}); err != nil {
		t.Fatalf("MultiLaneGraphSVG failed: %v", err)
	}

	got := fake.multiLaneReq.GetImplementedTriggers()
	if triggers := got["svc-1"].GetTriggers(); len(triggers) != 1 || triggers[0] != "approve" {
		t.Fatalf("expected svc-1's implemented triggers to be [approve], got %v", triggers)
	}
	if triggers := got["svc-2"].GetTriggers(); len(triggers) != 0 {
		t.Fatalf("expected svc-2 to have no implemented triggers, got %v", triggers)
	}
}

func TestMultiLaneGraphSVG_TransportError_Propagates(t *testing.T) {
	fake := &fakeStateMachineClient{multiLaneErr: context.DeadlineExceeded}
	svc := newTestService(t, fake)

	if _, err := svc.MultiLaneGraphSVG(context.Background(), []string{"svc-1"}); err == nil {
		t.Fatal("expected the transport error to propagate")
	}
}

func TestMultiLaneGraphSVG_ServerErrorCode_SurfacesAsError(t *testing.T) {
	fake := &fakeStateMachineClient{multiLaneResp: &proto.MultiLaneGraphResponse{ErrorCode: "invalid_argument", ErrorMessage: "unknown service svc-9"}}
	svc := newTestService(t, fake)

	if _, err := svc.MultiLaneGraphSVG(context.Background(), []string{"svc-9"}); err == nil {
		t.Fatal("expected an error when the response carries an ErrorCode")
	}
}
