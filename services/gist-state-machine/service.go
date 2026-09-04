// Package giststatemachine is the customer-facing half of the port of
// plugins/services/gist_statemachine. gist-server owns the actual
// transition graph and enforces it - attempting a trigger is nothing
// more than "is this move permitted from statable's current state, and
// if so what's the new state."
//
// A trigger's OnAction/OnEnter/OnExit phases are registered once, at
// startup, against a state machine's serviceID and the trigger's own
// name - the same Attach(serviceID, handlers...) shape
// gistapiserver.Attach uses for API endpoints, including the sg
// servicesGroup every phase receives as its first argument, built once
// by Attach (gistsdk.BuildServiceGroup) and reused across every
// Transition call, exactly like Attach builds one sg per API service
// and every request's handler reuses it:
//
//	var Submit = giststatemachine.RegisterTriggerFunc("submit",
//		func(sg services.ApiServiceGroup, ctx context.Context, order *model.Order) error {
//			return chargePayment(sg, order) // OnAction - required, and where servicesGroup/M are inferred from
//		},
//		giststatemachine.OnEnter(checkAmount),
//		giststatemachine.OnExit(sendConfirmation),
//	)
//
//	func AttachStateMachineTriggers() gistsdk.Option {
//		return giststatemachine.Attach("demo-order", Submit) // servicesGroup and M both inferred from Submit
//	}
//
// Nothing above ever names the concrete servicesGroup or statable types
// explicitly - RegisterTriggerFunc infers both from onAction's plain
// function literal, the same way slices.SortFunc infers its element
// type from a plain comparison func, and Attach in turn infers both
// from the already-concrete handlers it's given. Firing the trigger
// anywhere else needs only its name - no reference to Submit itself:
//
//	giststatemachine.Transition(ctx, sg.StateMachine, "submit", order)
//
// which runs, in order: every OnEnter function (a guard/preparation
// phase - an error here aborts before the transition is even attempted,
// statable's state is left untouched); then the actual transition
// attempt against gist-server's graph (implicit - nothing to call);
// then OnAction (the trigger's own business logic, now that the move
// has actually succeeded and statable's state reflects it); then every
// OnExit function in order. Everything else - fetching statable,
// persisting it, any side effect unrelated to this specific trigger -
// is still the customer's own code, written around the Transition call.
package giststatemachine

import (
	"context"
	"fmt"
	"sync"

	"github.com/wieoapps/gist"
	"github.com/wieoapps/gist/internal/rpcconn"
	"github.com/wieoapps/gist/proto"
)

type Statabler interface {
	GetState() string
	SetState(state string)
}

// Statable can be embedded in a customer's own state-holding type to get
// GetState/SetState for free, satisfying Statabler without writing
// either method by hand:
//
//	type OrderStatable struct {
//		giststatemachine.Statable
//		OrderID string
//	}
//
// The same "nest it and it's just there" ergonomics as the original
// monolith's own Statable - including its RWMutex-guarded GetState/
// SetState: a customer's own object can outlive a single Transition
// call and be read from other goroutines while one is in flight (e.g.
// an HTTP handler reading current status while a background job
// transitions the same object), so concurrent access has to be safe by
// construction, not just usually fine in practice.
type Statable struct {
	mu    sync.RWMutex
	state string
}

func (s *Statable) GetState() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

func (s *Statable) SetState(state string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = state
}

// TransitionFn is a single OnEnter/OnAction/OnExit phase for a trigger -
// receives the same servicesGroup every API endpoint handler does
// (built once by Attach, reused across every call), plus the live,
// concretely-typed statable object.
type TransitionFn[servicesGroup any, M Statabler] func(sg servicesGroup, ctx context.Context, statable M) error

// TriggerOption adds an OnEnter or OnExit phase to a trigger built by
// RegisterTriggerFunc - see OnEnter/OnExit.
type TriggerOption[servicesGroup any, M Statabler] func(*TriggerHandler[servicesGroup, M])

// OnEnter adds fns to a trigger's guard/preparation phase, run in the
// order given (appended to any already added by an earlier OnEnter
// option), before the transition is attempted - an error here aborts
// it, statable's state is left untouched. Pass to RegisterTriggerFunc.
func OnEnter[servicesGroup any, M Statabler](fns ...TransitionFn[servicesGroup, M]) TriggerOption[servicesGroup, M] {
	return func(h *TriggerHandler[servicesGroup, M]) { h.onEnter = append(h.onEnter, fns...) }
}

// OnExit adds fns to a trigger's side-effect phase, run in the order
// given (appended to any already added by an earlier OnExit option),
// after OnAction succeeds. Pass to RegisterTriggerFunc.
func OnExit[servicesGroup any, M Statabler](fns ...TransitionFn[servicesGroup, M]) TriggerOption[servicesGroup, M] {
	return func(h *TriggerHandler[servicesGroup, M]) { h.onExit = append(h.onExit, fns...) }
}

// TriggerHandler is one trigger's name and its OnEnter/OnAction/OnExit
// phases, built by RegisterTriggerFunc and not yet attached to any
// particular gist-state-machine service - pass several (sharing the
// same servicesGroup and M) to Attach to register them all against one
// service id at once.
type TriggerHandler[servicesGroup any, M Statabler] struct {
	trigger  string
	onEnter  []TransitionFn[servicesGroup, M]
	onAction TransitionFn[servicesGroup, M]
	onExit   []TransitionFn[servicesGroup, M]
}

// RegisterTriggerFunc builds one trigger: its name, its required
// OnAction (the trigger's own business logic - the reason it exists),
// and any OnEnter/OnExit phases via the OnEnter/OnExit options.
// servicesGroup and M are both inferred from onAction alone - never
// write out either type anywhere.
func RegisterTriggerFunc[servicesGroup any, M Statabler](trigger string, onAction TransitionFn[servicesGroup, M], opts ...TriggerOption[servicesGroup, M]) *TriggerHandler[servicesGroup, M] {
	h := &TriggerHandler[servicesGroup, M]{trigger: trigger, onAction: onAction}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// Attach registers every one of handlers against serviceID - the id is
// named once here, not once per trigger. sg is built once
// (gistsdk.BuildServiceGroup), exactly like gistapiserver.Attach builds
// one sg per API service, and reused across every Transition call for
// any of these triggers. Firing any of them elsewhere needs only
// Transition(ctx, svc, trigger, statable); nothing else needs a
// reference to what's passed here.
func Attach[servicesGroup any, M Statabler](serviceID string, handlers ...*TriggerHandler[servicesGroup, M]) gist.Option {
	return func(server *gist.Server) error {
		sg, err := gist.BuildServiceGroup[servicesGroup](server)
		if err != nil {
			return err
		}
		for _, h := range handlers {
			if h.onAction == nil {
				return fmt.Errorf("gist-state-machine: service %q: trigger %q: OnAction is required", serviceID, h.trigger)
			}
			server.RegisterStateMachineTrigger(serviceID, h.trigger, gist.StateMachineTrigger{
				OnEnter:  eraseFns(sg, h.onEnter),
				OnAction: eraseFn(sg, h.onAction),
				OnExit:   eraseFns(sg, h.onExit),
			})
		}
		return nil
	}
}

func eraseFn[servicesGroup any, M Statabler](sg servicesGroup, fn TransitionFn[servicesGroup, M]) gist.StateMachineTriggerFn {
	if fn == nil {
		return nil
	}
	return func(ctx context.Context, statable any) error { return fn(sg, ctx, statable.(M)) }
}

func eraseFns[servicesGroup any, M Statabler](sg servicesGroup, fns []TransitionFn[servicesGroup, M]) []gist.StateMachineTriggerFn {
	out := make([]gist.StateMachineTriggerFn, len(fns))
	for i, fn := range fns {
		out[i] = eraseFn(sg, fn)
	}
	return out
}

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

// attemptTransition is the actual RPC to gist-server: validate trigger
// is permitted from currentState against the configured graph, and if
// so apply it.
func (s *Service) attemptTransition(ctx context.Context, trigger, currentState string) (newState string, err error) {
	resp, err := rpcconn.MustFor(s.server).StateMachine.Transition(ctx, &proto.TransitionRequest{
		ServiceId:    s.serviceID,
		Trigger:      trigger,
		CurrentState: currentState,
	})
	if err != nil {
		return "", fmt.Errorf("gist-state-machine: transition: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return "", fmt.Errorf("gist-state-machine: transition: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return resp.GetNewState(), nil
}

// Transition fires trigger on statable against svc: every OnEnter
// function in order, the transition attempt itself, OnAction, then
// every OnExit function in order - see the package doc for the full
// sequencing and abort semantics. trigger must have been registered
// against svc's own service id via Attach.
func Transition[M Statabler](ctx context.Context, svc *Service, trigger string, statable M) error {
	action, ok := svc.server.StateMachineTrigger(svc.serviceID, trigger)
	if !ok {
		return fmt.Errorf("gist-state-machine: service %q: no trigger %q registered - attach it with giststatemachine.Attach", svc.serviceID, trigger)
	}

	for _, fn := range action.OnEnter {
		if err := fn(ctx, statable); err != nil {
			return err
		}
	}

	newState, err := svc.attemptTransition(ctx, trigger, statable.GetState())
	if err != nil {
		return err
	}
	statable.SetState(newState)

	if err := action.OnAction(ctx, statable); err != nil {
		return err
	}

	for _, fn := range action.OnExit {
		if err := fn(ctx, statable); err != nil {
			return err
		}
	}
	return nil
}

// graph is the shared RPC both Graph and GraphSVG call - sends every
// trigger name actually Attach-ed against svc's own service id (via
// server.RegisteredStateMachineTriggers), so gist-server can mark any
// configured-but-unattached trigger as unimplemented in what it sends
// back. gist-server has no way to know this on its own, since Attach
// runs entirely in this process.
func (svc *Service) graph(ctx context.Context) (*proto.GraphResponse, error) {
	resp, err := rpcconn.MustFor(svc.server).StateMachine.Graph(ctx, &proto.GraphRequest{
		ServiceId:           svc.serviceID,
		ImplementedTriggers: svc.server.RegisteredStateMachineTriggers(svc.serviceID),
	})
	if err != nil {
		return nil, fmt.Errorf("gist-state-machine: graph: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return nil, fmt.Errorf("gist-state-machine: graph: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return resp, nil
}

// Graph returns the DOT-format representation of svc's configured
// transition graph (https://graphviz.org/doc/info/lang.html) - render
// it with any Graphviz front-end (e.g. `dot -Tpng graph.dot -o
// graph.png`) to visualize it. Purely structural: gist-server holds no
// live object, so this reflects the graph itself, seeded with its own
// configured initial state - not any particular object's current state.
// A trigger with no attached handler (see Attach) is marked with a
// "🚧 " prefix, the same convention gist-api-server's own docs use for
// an endpoint with no registered handler.
func (svc *Service) Graph(ctx context.Context) (string, error) {
	resp, err := svc.graph(ctx)
	if err != nil {
		return "", err
	}
	return resp.GetDot(), nil
}

// GraphSVG returns a self-contained, hand-rendered SVG diagram of the
// same graph Graph describes - no external Graphviz/layout engine
// needed to view it, just an SVG-capable viewer (any browser, or an
// <img>/<object> tag in a page your own process already serves).
func (svc *Service) GraphSVG(ctx context.Context) (string, error) {
	resp, err := svc.graph(ctx)
	if err != nil {
		return "", err
	}
	return resp.GetSvg(), nil
}

// MultiLaneGraphSVG returns one combined, hand-rendered SVG spanning
// every one of serviceIDs' configured gist-state-machine instances, each
// its own horizontal swimlane, connected wherever a transition's
// to-service names another instance in the list - see gist-server's own
// RenderMultiLaneSVG. Unlike Graph/GraphSVG, svc's own service id plays
// no special role here beyond providing the connection - pass whichever
// ids (including or excluding svc's own) make up the diagram you want;
// gist-server resolves each against its own configured instances,
// regardless of which of them this process has attached any triggers
// for. A trigger not registered via Attach against its own instance's id
// (see RegisteredStateMachineTriggers) still renders, marked
// unimplemented - exactly the same convention Graph/GraphSVG use, now
// applied per lane instead of to a single machine.
func (svc *Service) MultiLaneGraphSVG(ctx context.Context, serviceIDs []string) (string, error) {
	implementedTriggers := make(map[string]*proto.ImplementedTriggers, len(serviceIDs))
	for _, id := range serviceIDs {
		implementedTriggers[id] = &proto.ImplementedTriggers{Triggers: svc.server.RegisteredStateMachineTriggers(id)}
	}

	resp, err := rpcconn.MustFor(svc.server).StateMachine.MultiLaneGraph(ctx, &proto.MultiLaneGraphRequest{
		ServiceIds:          serviceIDs,
		ImplementedTriggers: implementedTriggers,
	})
	if err != nil {
		return "", fmt.Errorf("gist-state-machine: multi-lane graph: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return "", fmt.Errorf("gist-state-machine: multi-lane graph: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return resp.GetSvg(), nil
}
