package gist

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/wieoapps/gist/internal/rpcconn"
	"github.com/wieoapps/gist/logging"
	"github.com/wieoapps/gist/proto"
)

type Config struct {
	ConfigPath string
}

// grpcRecvCeiling raises both directions of every gRPC connection this
// package makes (or serves) past grpc-go's 4MiB default - a limit that's
// easy to hit on a Find/GenerateFixtures/etc. call against a large result
// set, and gRPC has no way to negotiate a bigger one after the fact; it
// just rejects the message. Deliberately math.MaxInt32 (gRPC's own
// practical ceiling - a message's wire-format length prefix is 4 bytes),
// not a value borrowed from any license tier: gist-sdk is open source and
// deliberately never reads the license file (gist-server does), so a real
// tier-specific number here would (a) be meaningless, since gist-sdk can't
// be trusted to enforce a cap against itself, and (b) invite exactly the
// misreading that a customer could edit this constant to raise their own
// tier's limit. Neither is true: the actual demo/paid enforcement happens
// entirely on gist-server's own sending/receiving side (see
// gist-server/license.License.MaxMessageBytes, applied in main.go and
// callback.go), which gRPC enforces independently of whatever this side
// claims to accept - a message exceeding the sender's own configured max
// is rejected before it's even transmitted, so this value only ever
// widens what gist-sdk itself is willing to receive, never what
// gist-server is willing to send.
const grpcRecvCeiling = math.MaxInt32

type DispatchFunc func(ctx context.Context, input json.RawMessage) (json.RawMessage, error)

type CodeError interface {
	error
	StatusCode() int32
}

const (
	internalCode int32 = 13
	notFoundCode int32 = 5
)

// MiddlewareDispatchFunc is one named middleware's registered callback,
// type-erased the same way DispatchFunc is for an endpoint - proto
// types are the shared vocabulary here (not a customer-facing type)
// because gistapiserver.MiddlewareRequest/MiddlewareResponse live in a
// package that imports this one; this package can't import back without
// a cycle. gistapiserver.MiddlewareHandler's own registration closure is
// what converts between these and its typed, customer-facing fn.
type MiddlewareDispatchFunc func(ctx context.Context, req *proto.InvokeMiddlewareRequest) (*proto.InvokeMiddlewareResponse, error)

type middlewareDispatchEntry struct {
	fn MiddlewareDispatchFunc
}

type dispatchEntry struct {
	fn DispatchFunc

	// declaredErrors maps each code the endpoint declared to that code's
	// own default message - the safe, curated text shown publicly for an
	// error that isn't itself a CodeError (see Invoke). Not just a set of
	// codes: a plain error's own Error() text is never guaranteed safe to
	// expose (a driver/SQL error bubbling out of a repo call, for one),
	// so undeclared-message errors fall back to this instead of leaking
	// whatever the underlying error happened to say.
	declaredErrors map[int32]string
}

// CustomService is the interface a customer's own custom-service type
// must implement - PostBuild/Start/Stop, the same three methods every
// built-in gist-server service's own internal lifecycle.Service
// interface has. Build isn't part of the interface for the same reason
// it isn't part of lifecycle.Service either - each kind's own Config
// type differs, so it's enforced by RegisterCustomService's build
// function instead, not a fourth interface method.
//
// Start follows the exact same contract every built-in service's Start
// does: return promptly once ready() has been called, backgrounding
// anything long-running in its own goroutine - never block here.
// StartCustomService (the RPC gist-server calls to activate an
// instance) waits for Start to return before considering the instance
// up, the same way gist-server's own built-in services all work now;
// blocking here would hold that RPC open indefinitely. ready itself is
// currently a no-op kept for signature symmetry with lifecycle.Service
// - gist-server treats "the RPC returned successfully" as the readiness
// signal for its own wait-for graph, not a separate call ready makes.
type CustomService interface {
	PostBuild() error
	Start(ctx context.Context, ready func()) error
	Stop(ctx context.Context) error
}

// customServiceKind is what RegisterCustomService registers per kind -
// type-erased (build and instances operate on CustomService, not the
// customer's own Config/S) so Server.customServices can hold every
// registered kind side by side, the same type-erasure StateMachineTrigger
// already uses for gist-state-machine's own typed OnEnter/OnAction/OnExit.
// instances is keyed by id (one config.json array entry can register
// several instances of the same kind), populated by StartCustomService
// and consulted by StopCustomService - fixes the previous design's gap,
// where one handler was shared across every id of a kind with no way
// for Stop to reach a specific instance.
type customServiceKind struct {
	build func(configJSON json.RawMessage) (CustomService, error)

	mu        sync.Mutex
	instances map[string]CustomService
}

// RegisterCustomService lets a customer register their own service
// kind - referenced in config.json's "services" object exactly like a
// built-in kind (id/enabled/delayed-start/wait-for are all handled
// generically by gist-server before this is ever called, see
// gist-server's own gist-custom package). Config and S both infer from
// build's own signature - nothing to spell out at the call site, the
// same trick giststatemachine.RegisterTriggerFunc uses.
func RegisterCustomService[Config any, S CustomService](kind string, build func(Config) (S, error)) Option {
	return func(server *Server) error {
		k := &customServiceKind{
			instances: map[string]CustomService{},
			build: func(configJSON json.RawMessage) (CustomService, error) {
				var cfg Config
				if len(configJSON) > 0 {
					if err := json.Unmarshal(configJSON, &cfg); err != nil {
						return nil, fmt.Errorf("gistsdk: custom service %q: could not decode config: %w", kind, err)
					}
				}
				return build(cfg)
			},
		}
		server.mu.Lock()
		if server.customServices == nil {
			server.customServices = map[string]*customServiceKind{}
		}
		server.customServices[kind] = k
		server.mu.Unlock()
		return nil
	}
}

func RegisterScheduleFunc[dep any](schedulerID string, d dep, fn func(dep)) Option {
	return func(server *Server) error {
		server.mu.Lock()
		if server.schedulers == nil {
			server.schedulers = map[string]func(ctx context.Context){}
		}
		server.schedulers[schedulerID] = func(_ context.Context) { fn(d) }
		server.mu.Unlock()
		return nil
	}
}

// RabbitMQDelivery is one consumed message, handed to a func registered
// via RegisterRabbitMQConsumer.
type RabbitMQDelivery struct {
	Body        []byte
	ContentType string
	Headers     map[string]string
}

// rabbitMQConsumerOptions/RabbitMQConsumerOption/WithRabbitMQAutoAck are
// RegisterRabbitMQConsumer's own functional option, not a plain trailing
// bool - a bare `true`/`false` at a call site doesn't say what it means,
// the same reasoning giststatemachine.OnEnter/OnExit already follows.
type rabbitMQConsumerOptions struct {
	autoAck bool
}

type RabbitMQConsumerOption func(*rabbitMQConsumerOptions)

// WithRabbitMQAutoAck tells gist-server to ack a message the instant it
// is delivered, before fn even runs - simpler, but a crash partway
// through fn silently drops the message. Default (not passing this) is
// manual ack: fn's own return value decides ack (nil) vs. nack-and-
// requeue (non-nil) - the safer default, matching what RabbitMQ's own
// at-least-once delivery guarantee is for.
func WithRabbitMQAutoAck() RabbitMQConsumerOption {
	return func(o *rabbitMQConsumerOptions) { o.autoAck = true }
}

// RegisterRabbitMQConsumer activates consuming queue on serviceID's
// gist-rabbit-mq-client instance and registers fn as the handler every message
// on it is delivered to. gist-server owns the actual amqp091-go Consume
// loop (see gistrabbitmqclient.Service.StartConsuming) and calls back into
// this process for every message - the same push-driven shape
// gist-scheduler's Tick already uses for cron fires (a queue name plays
// the role Tick's id does), not giststatemachine.Attach's shape:
// Consume is driven by the broker's own timing, not something the
// customer's code ever initiates itself. d is any single
// customer-supplied value (often a whole servicesGroup, but that's the
// caller's choice - unlike Attach, this never builds one automatically),
// matching RegisterScheduleFunc's own shape.
func RegisterRabbitMQConsumer[dep any](serviceID, queue string, d dep, fn func(dep, context.Context, RabbitMQDelivery) error, opts ...RabbitMQConsumerOption) Option {
	var o rabbitMQConsumerOptions
	for _, opt := range opts {
		opt(&o)
	}

	return func(server *Server) error {
		server.mu.Lock()
		if server.rabbitMQConsumers == nil {
			server.rabbitMQConsumers = map[string]func(context.Context, RabbitMQDelivery) error{}
		}
		server.rabbitMQConsumers[serviceID+"/"+queue] = func(ctx context.Context, d2 RabbitMQDelivery) error {
			return fn(d, ctx, d2)
		}
		server.mu.Unlock()

		resp, err := rpcconn.MustFor(server).RabbitMQ.StartConsuming(context.Background(), &proto.StartConsumingRequest{
			ServiceId: serviceID,
			Queue:     queue,
			AutoAck:   o.autoAck,
		})
		if err != nil {
			return fmt.Errorf("gistsdk: could not start consuming %q/%q: %w", serviceID, queue, err)
		}
		if resp.GetErrorCode() != "" {
			return fmt.Errorf("gistsdk: could not start consuming %q/%q: %s: %s", serviceID, queue, resp.GetErrorCode(), resp.GetErrorMessage())
		}
		return nil
	}
}

// BubbleUpErrors is the code-level equivalent of config.json's top-level
// "bubble-up-errors": true (see readBubbleUpErrors) - either one opts an
// app back into forwarding a plain (non-CodeError) error's own Error()
// text verbatim as the public API caller's error message, for any code
// the endpoint declared - not just codes on errors explicitly authored as
// public (e.g. via ExpectedError.WithMessage). The two are additive (OR,
// not override): setting this Option can't turn off what config.json
// already enabled. Prefer the config.json field for anything
// environment-dependent (on for local dev, off in production) - reach for
// this Option instead when the choice belongs in code (e.g. conditional
// on a build tag), not in a file that gets copied between environments.
//
// Off by default: a plain error returned from a handler - a raw driver/
// SQL error bubbling out of a repo call, say - was only ever vetted as
// safe for the customer's own process, not for the actual external
// caller of the public API. Without either, Invoke instead falls back to
// the endpoint's own declared default message for that error's code, and
// logs the real error via Server.Logger so nothing is lost for the
// customer's own debugging - see gist-sdk's information-disclosure
// review for the incident this closes.
//
// Enable this only if every handler in this app already returns
// sanitized errors (or you specifically want raw errors surfaced, e.g.
// while developing).
func BubbleUpErrors() Option {
	return func(server *Server) error {
		server.bubbleUpRawErrors = true
		return nil
	}
}

func (a *Server) RegisterDispatch(id string, fn DispatchFunc, declaredErrors map[int32]string) {
	a.mu.Lock()
	if a.dispatch == nil {
		a.dispatch = map[string]dispatchEntry{}
	}
	a.dispatch[id] = dispatchEntry{fn: fn, declaredErrors: declaredErrors}
	a.mu.Unlock()
}

// RegisterMiddlewareDispatch is RegisterDispatch's counterpart for a
// named middleware - see gistapiserver.MiddlewareHandler, the typed,
// customer-facing entry point that builds fn.
func (a *Server) RegisterMiddlewareDispatch(name string, fn MiddlewareDispatchFunc) {
	a.mu.Lock()
	if a.middlewareDispatch == nil {
		a.middlewareDispatch = map[string]middlewareDispatchEntry{}
	}
	a.middlewareDispatch[name] = middlewareDispatchEntry{fn: fn}
	a.mu.Unlock()
}

// StateMachineTriggerFn is one OnEnter/OnAction/OnExit phase for a
// gist-state-machine trigger, type-erased - statable is the live object
// passed to giststatemachine.Transition, typed any here since this
// package doesn't know giststatemachine.Statabler. giststatemachine's
// own generic wrapper (Attach) builds these from the customer's typed
// TransitionFn[M], and asserts back to M when it looks them up.
type StateMachineTriggerFn func(ctx context.Context, statable any) error

// StateMachineTrigger is one trigger's registered phases, type-erased -
// see giststatemachine.RegisterTriggerFunc/Attach, which build these
// from the customer's typed OnAction/OnEnter/OnExit.
type StateMachineTrigger struct {
	OnEnter  []StateMachineTriggerFn
	OnAction StateMachineTriggerFn
	OnExit   []StateMachineTriggerFn
}

// RegisterStateMachineTrigger attaches action to serviceID's trigger -
// see giststatemachine.Attach, the customer-facing, typed entry point
// that actually calls this.
func (a *Server) RegisterStateMachineTrigger(serviceID, trigger string, action StateMachineTrigger) {
	a.mu.Lock()
	if a.stateMachineTriggers == nil {
		a.stateMachineTriggers = map[string]StateMachineTrigger{}
	}
	a.stateMachineTriggers[serviceID+"/"+trigger] = action
	a.mu.Unlock()
}

// StateMachineTrigger returns serviceID's registered trigger, if any -
// see giststatemachine.Transition, which looks triggers up by name here
// rather than needing the caller to hold a reference to what Attach
// registered.
func (a *Server) StateMachineTrigger(serviceID, trigger string) (StateMachineTrigger, bool) {
	a.mu.Lock()
	action, ok := a.stateMachineTriggers[serviceID+"/"+trigger]
	a.mu.Unlock()
	return action, ok
}

// RegisteredStateMachineTriggers returns every trigger name attached
// against serviceID so far - see giststatemachine.Service.Graph, which
// sends this list to gist-server so it can mark any configured trigger
// NOT in it as unimplemented in the returned graph. gist-server has no
// way to know this on its own, since Attach runs entirely here, in the
// customer's own process.
func (a *Server) RegisteredStateMachineTriggers(serviceID string) []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	prefix := serviceID + "/"
	var out []string
	for key := range a.stateMachineTriggers {
		if trigger, ok := strings.CutPrefix(key, prefix); ok {
			out = append(out, trigger)
		}
	}
	return out
}

type authSubjectKey struct{}

func withAuthSubject(ctx context.Context, subject string) context.Context {
	return context.WithValue(ctx, authSubjectKey{}, subject)
}

func AuthSubject(ctx context.Context) (string, bool) {
	subject, ok := ctx.Value(authSubjectKey{}).(string)
	return subject, ok
}

type Server struct {
	cfg Config

	cmd            *exec.Cmd
	exited         chan struct{}
	exitErr        error
	dir            string
	adminSocket    string
	callbackSocket string

	adminConn *grpc.ClientConn

	// Logger is populated automatically into any servicesGroup field of
	// this exact interface type (see BuildServiceGroup) - the split-
	// architecture counterpart of the old monolith's g.ServiceGroup,
	// which every customer servicesGroup had to embed to get a Logger
	// field for free via Fx. There's no Fx here and nothing to embed, so
	// this restores the same "just declare the field, no wiring
	// required" ergonomics through the existing name-tag reflection
	// mechanism instead - a Logger field is simply never tagged, since
	// there's exactly one (unlike each built-in service's own Service
	// type, which is per-config-id and resolved by name tag). Set in
	// Start to a logging.NewLogger(a) that forwards every call to
	// gist-server over the admin connection - which backend actually
	// writes it (config.json's "logger" field) is gist-server's own
	// concern now, not gist-sdk's.
	Logger Logger

	callbackServer *grpc.Server

	mu                   sync.Mutex
	dispatch             map[string]dispatchEntry
	middlewareDispatch   map[string]middlewareDispatchEntry
	customServices       map[string]*customServiceKind
	schedulers           map[string]func(ctx context.Context)
	stateMachineTriggers map[string]StateMachineTrigger
	rabbitMQConsumers    map[string]func(context.Context, RabbitMQDelivery) error

	// bubbleUpRawErrors reverts Invoke to gist-sdk's pre-fix behavior:
	// forwarding a plain (non-CodeError) error's own Error() text
	// verbatim as the public API response's error message, instead of
	// falling back to the endpoint's own declared default message for
	// that code. Off by default - see BubbleUpErrors's doc comment for
	// why forwarding unvetted error text isn't the safe default.
	bubbleUpRawErrors bool
}

func Start(cfg Config) (*Server, error) {
	if cfg.ConfigPath == "" {
		return nil, fmt.Errorf("gistsdk: Config.ConfigPath is required")
	}

	bubbleUpErrors, err := readBubbleUpErrors(cfg.ConfigPath)
	if err != nil {
		return nil, err
	}

	dir, err := os.MkdirTemp("", "gist-server-*")
	if err != nil {
		return nil, fmt.Errorf("gistsdk: could not create socket directory: %w", err)
	}

	app := &Server{
		cfg:                  cfg,
		dir:                  dir,
		adminSocket:          filepath.Join(dir, "admin.sock"),
		callbackSocket:       filepath.Join(dir, "callback.sock"),
		dispatch:             map[string]dispatchEntry{},
		middlewareDispatch:   map[string]middlewareDispatchEntry{},
		customServices:       map[string]*customServiceKind{},
		schedulers:           map[string]func(ctx context.Context){},
		stateMachineTriggers: map[string]StateMachineTrigger{},
		rabbitMQConsumers:    map[string]func(context.Context, RabbitMQDelivery) error{},
		bubbleUpRawErrors:    bubbleUpErrors,
	}
	// Both send through the same RPC path (see logging.NewLogger) - the
	// only difference is whether a servicesGroup with a Logger field is
	// in scope (sg.Logger) or not (the package-level logging.Info etc).
	// Safe to wire up before dialAdmin below has actually connected:
	// logging.send checks whether owner's clients are registered yet on
	// every call and falls back to stderr until they are, rather than
	// requiring this to happen in a specific order.
	app.Logger = logging.NewLogger(app)
	logging.SetOwner(app)

	if err := app.startCallbackServer(); err != nil {
		if err := os.RemoveAll(dir); err != nil {
			return nil, err
		}
		return nil, err
	}

	if err := app.spawnGistServer(); err != nil {
		if err := app.Stop(); err != nil {
			return nil, err
		}
		return nil, err
	}

	if err := app.dialAdmin(); err != nil {
		if err := app.Stop(); err != nil {
			return nil, err
		}
		return nil, err
	}

	return app, nil
}

func (a *Server) startCallbackServer() error {
	lis, err := net.Listen("unix", a.callbackSocket)
	if err != nil {
		return fmt.Errorf("gistsdk: could not listen on callback socket: %w", err)
	}

	a.callbackServer = grpc.NewServer(grpc.MaxRecvMsgSize(grpcRecvCeiling))
	proto.RegisterCallbackServiceServer(a.callbackServer, &callbackServer{app: a})

	go func() {
		_ = a.callbackServer.Serve(lis)
	}()
	return nil
}

func (a *Server) spawnGistServer() error {
	if a.cfg.ConfigPath == "" {
		return fmt.Errorf("gistsdk: Config.ConfigPath is required")
	}

	gistBinary, err := readGistBinaryPath(a.cfg.ConfigPath)
	if err != nil {
		return err
	}
	if gistBinary == "" {
		return fmt.Errorf("gistsdk: %s is missing top-level \"gist-binary\"", a.cfg.ConfigPath)
	}

	a.cmd = exec.Command(gistBinary,
		"-admin-socket", a.adminSocket,
		"-callback-socket", a.callbackSocket,
		"-config", a.cfg.ConfigPath,
	)
	a.cmd.Stdout = os.Stdout
	a.cmd.Stderr = os.Stderr

	if err := a.cmd.Start(); err != nil {
		return fmt.Errorf("gistsdk: could not start gist-server: %w", err)
	}

	a.exited = make(chan struct{})
	go func() {
		a.exitErr = a.cmd.Wait()
		close(a.exited)
	}()
	return nil
}

func readGistBinaryPath(configPath string) (string, error) {
	raw, err := os.ReadFile(configPath)
	if err != nil {
		return "", fmt.Errorf("gistsdk: could not read %s: %w", configPath, err)
	}

	var cfg struct {
		GistBinary string `json:"gist-binary"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return "", fmt.Errorf("gistsdk: malformed config file %s: %w", configPath, err)
	}
	return cfg.GistBinary, nil
}

// readBubbleUpErrors reads config.json's top-level "bubble-up-errors"
// field - see BubbleUpErrors's doc comment for what setting it changes.
// Missing/absent is fine and means false, the safe default.
func readBubbleUpErrors(configPath string) (bool, error) {
	raw, err := os.ReadFile(configPath)
	if err != nil {
		return false, fmt.Errorf("gistsdk: could not read %s: %w", configPath, err)
	}

	var cfg struct {
		BubbleUpErrors bool `json:"bubble-up-errors"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return false, fmt.Errorf("gistsdk: malformed config file %s: %w", configPath, err)
	}
	return cfg.BubbleUpErrors, nil
}

func (a *Server) dialAdmin() error {
	conn, err := grpc.NewClient("passthrough:///"+a.adminSocket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", a.adminSocket)
		}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(grpcRecvCeiling)),
	)
	if err != nil {
		return fmt.Errorf("gistsdk: could not create admin client: %w", err)
	}
	a.adminConn = conn
	rpcconn.Register(a, &rpcconn.Clients{
		Admin:              proto.NewBootstrapServiceClient(conn),
		DB:                 proto.NewMySQLServiceClient(conn),
		PG:                 proto.NewPostgresServiceClient(conn),
		Elasticsearch:      proto.NewElasticsearchServiceClient(conn),
		GoogleCloudStorage: proto.NewGoogleCloudStorageServiceClient(conn),
		StateMachine:       proto.NewStateMachineServiceClient(conn),
		HTTPClient:         proto.NewHTTPClientServiceClient(conn),
		Logging:            proto.NewLoggingServiceClient(conn),
		RabbitMQ:           proto.NewRabbitMQServiceClient(conn),
		Redis:              proto.NewRedisServiceClient(conn),
		PubSub:             proto.NewPubSubServiceClient(conn),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return a.handshake(ctx, conn)
		}

		changed := make(chan bool, 1)
		go func() { changed <- conn.WaitForStateChange(ctx, state) }()

		select {
		case <-a.exited:
			return fmt.Errorf("gistsdk: gist-server exited before becoming ready: %w", a.exitErr)
		case ok := <-changed:
			if !ok {
				return fmt.Errorf("gistsdk: gist-server did not become ready: %w", ctx.Err())
			}
		}
	}
}

// handshake is called once, right as the admin channel becomes ready and
// before any other admin RPC - see gist-server's Handshake implementation
// (adminserver.go) for the version comparison itself. A mismatch here is
// fatal: dialAdmin's own caller (Start) tears the connection back down on
// any error, exactly as it already does for a failed connect.
func (a *Server) handshake(ctx context.Context, conn *grpc.ClientConn) error {
	client := proto.NewBootstrapServiceClient(conn)
	if _, err := client.Handshake(ctx, &proto.HandshakeRequest{SdkVersion: sdkVersion()}); err != nil {
		return fmt.Errorf("gistsdk: version handshake with gist-server failed: %w", err)
	}
	return nil
}

func (a *Server) Stop() error {
	rpcconn.Unregister(a)
	if a.cmd != nil && a.cmd.Process != nil {
		_ = a.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-a.exited:
		case <-time.After(5 * time.Second):
			_ = a.cmd.Process.Kill()
			<-a.exited
		}
		a.cmd = nil
	}
	if a.callbackServer != nil {
		a.callbackServer.GracefulStop()
		a.callbackServer = nil
	}
	if a.adminConn != nil {
		_ = a.adminConn.Close()
		a.adminConn = nil
	}
	if a.dir != "" {
		_ = os.RemoveAll(a.dir)
		a.dir = ""
	}
	return nil
}

// ValidateMiddlewares confirms every configured gist-api-server
// middleware name has a real callback registered for it (see
// AttachMiddlewares) - called once by App.Run, after every Option has
// already run, since gist-server's own startup happens strictly before
// this process even connects and has nothing to check against yet
// (see bootstrap.proto's ValidateMiddlewaresRequest doc). Exported so
// App.Run (a sibling file in this same package) can call it, but not
// meant to be called directly by customer code - AttachMiddlewares is
// the customer-facing entry point.
func (a *Server) ValidateMiddlewares() error {
	resp, err := rpcconn.MustFor(a).Admin.ValidateMiddlewares(context.Background(), &proto.ValidateMiddlewaresRequest{})
	if err != nil {
		return fmt.Errorf("gistsdk: could not validate configured middlewares: %w", err)
	}
	if len(resp.GetMissing()) > 0 {
		return fmt.Errorf("gistsdk: %d configured middleware(s) never registered via gistapiserver.AttachMiddlewares:\n%s",
			len(resp.GetMissing()), strings.Join(resp.GetMissing(), "\n"))
	}
	return nil
}

func (a *Server) WaitForInterrupt() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch
	_ = a.Stop()
}

type callbackServer struct {
	proto.UnimplementedCallbackServiceServer
	app *Server
}

func (s *callbackServer) Invoke(ctx context.Context, req *proto.InvokeRequest) (*proto.InvokeResponse, error) {
	s.app.mu.Lock()
	entry, ok := s.app.dispatch[req.GetEndpointId()]
	s.app.mu.Unlock()

	if !ok {
		return &proto.InvokeResponse{
			ErrorCode:    notFoundCode,
			ErrorMessage: "endpoint not registered: " + req.GetEndpointId(),
		}, nil
	}

	if req.GetAuthSubject() != "" {
		ctx = withAuthSubject(ctx, req.GetAuthSubject())
	}

	output, err := entry.fn(ctx, req.GetInput())
	if err != nil {
		code := internalCode
		ce, isCodeError := err.(CodeError)
		if isCodeError {
			code = ce.StatusCode()
		}

		defaultMessage, declared := entry.declaredErrors[code]
		if !declared {
			panic(fmt.Sprintf("gistsdk: endpoint %q returned undeclared error code %d: %s", req.GetEndpointId(), code, err.Error()))
		}

		// Only a CodeError's own Error() text was ever deliberately
		// authored as a public message (see ExpectedError.WithMessage) -
		// anything else (a plain error bubbling out of a repo call, say)
		// falls back to the endpoint's own declared default for that
		// code instead of forwarding err.Error() verbatim, unless the app
		// opted back into the old behavior via BubbleUpErrors. Without
		// either, a handler that just does `return err` on e.g. a
		// database error would leak that error's raw text - table/
		// column/driver detail never meant to be public - straight into
		// the actual external API response, as long as the endpoint
		// happened to declare that error's code (internalCode is a
		// common one to declare).
		message := defaultMessage
		var traceID string
		switch {
		case isCodeError:
			message = err.Error()
		case s.app.bubbleUpRawErrors:
			message = err.Error()
		case s.app.Logger != nil:
			// The public message reveals nothing about the real error -
			// traceID is the only thread back to it, so it's logged
			// alongside the real error here and handed back in the
			// response for a customer's support team to search for.
			traceID = newTraceID()
			s.app.Logger.Error("gistsdk: endpoint returned an error with no public message",
				map[string]any{"endpoint": req.GetEndpointId(), "code": code, "error": err, "trace_id": traceID})
		}

		return &proto.InvokeResponse{ErrorCode: code, ErrorMessage: message, ErrorTraceId: traceID}, nil
	}

	return &proto.InvokeResponse{Output: output}, nil
}

// InvokeMiddleware is Invoke's counterpart for a named middleware -
// simpler on the error path than Invoke's own declared-error-code
// system, since a middleware doesn't go through EndpointHandler's
// ExpectedError ceremony: a plain error falls back to a generic
// internal-error message (trace-ID logged, unless BubbleUpErrors),
// and a CodeError's own message/code are used directly, the same "this
// was deliberately authored as public" signal ExpectedError.WithMessage
// gives Invoke.
func (s *callbackServer) InvokeMiddleware(ctx context.Context, req *proto.InvokeMiddlewareRequest) (*proto.InvokeMiddlewareResponse, error) {
	s.app.mu.Lock()
	entry, ok := s.app.middlewareDispatch[req.GetName()]
	s.app.mu.Unlock()

	if !ok {
		return &proto.InvokeMiddlewareResponse{
			ErrorCode:    notFoundCode,
			ErrorMessage: "middleware not registered: " + req.GetName(),
		}, nil
	}

	resp, err := entry.fn(ctx, req)
	if err != nil {
		code := internalCode
		message := "internal error"
		var traceID string
		switch ce, isCodeError := err.(CodeError); {
		case isCodeError:
			code = ce.StatusCode()
			message = err.Error()
		case s.app.bubbleUpRawErrors:
			message = err.Error()
		case s.app.Logger != nil:
			traceID = newTraceID()
			s.app.Logger.Error("gistsdk: middleware returned an error with no public message",
				map[string]any{"middleware": req.GetName(), "error": err, "trace_id": traceID})
		}
		return &proto.InvokeMiddlewareResponse{ErrorCode: code, ErrorMessage: message, ErrorTraceId: traceID}, nil
	}

	return resp, nil
}

const notRegisteredCode = "not_registered"

// StartCustomService activates one instance of a registered custom
// service kind - decode config, Build, PostBuild, Start, in that order,
// the same sequence every built-in service's own main.go wiring
// follows. The instance is tracked by (kind, id) so StopCustomService
// can find it again later.
func (s *callbackServer) StartCustomService(ctx context.Context, req *proto.StartCustomServiceRequest) (*proto.CustomServiceAck, error) {
	s.app.mu.Lock()
	k, ok := s.app.customServices[req.GetKind()]
	s.app.mu.Unlock()

	if !ok {
		return &proto.CustomServiceAck{ErrorCode: notRegisteredCode, ErrorMessage: "no service registered for kind " + req.GetKind()}, nil
	}

	svc, err := k.build(req.GetConfigJson())
	if err != nil {
		return &proto.CustomServiceAck{ErrorCode: "internal", ErrorMessage: err.Error()}, nil
	}
	if err := svc.PostBuild(); err != nil {
		return &proto.CustomServiceAck{ErrorCode: "internal", ErrorMessage: err.Error()}, nil
	}
	if err := svc.Start(ctx, func() {}); err != nil {
		return &proto.CustomServiceAck{ErrorCode: "internal", ErrorMessage: err.Error()}, nil
	}

	k.mu.Lock()
	k.instances[req.GetId()] = svc
	k.mu.Unlock()

	return &proto.CustomServiceAck{}, nil
}

func (s *callbackServer) StopCustomService(ctx context.Context, req *proto.StopCustomServiceRequest) (*proto.CustomServiceAck, error) {
	s.app.mu.Lock()
	k, ok := s.app.customServices[req.GetKind()]
	s.app.mu.Unlock()

	if !ok {
		return &proto.CustomServiceAck{}, nil
	}

	k.mu.Lock()
	svc, ok := k.instances[req.GetId()]
	delete(k.instances, req.GetId())
	k.mu.Unlock()

	if !ok {
		return &proto.CustomServiceAck{}, nil
	}
	if err := svc.Stop(ctx); err != nil {
		return &proto.CustomServiceAck{ErrorCode: "internal", ErrorMessage: err.Error()}, nil
	}
	return &proto.CustomServiceAck{}, nil
}

func (s *callbackServer) Tick(ctx context.Context, req *proto.TickRequest) (*proto.TickAck, error) {
	s.app.mu.Lock()
	fn, ok := s.app.schedulers[req.GetId()]
	s.app.mu.Unlock()

	if !ok {
		return &proto.TickAck{ErrorCode: notRegisteredCode, ErrorMessage: "no schedule func registered for " + req.GetId()}, nil
	}
	fn(ctx)
	return &proto.TickAck{}, nil
}

// Deliver runs the fn RegisterRabbitMQConsumer registered for
// (service_id, queue), reporting its outcome back as the ack/nack
// verdict gist-rabbit-mq-client's own Service.handleDelivery applies to the
// broker - see that method's doc comment for why this is one round
// trip, not a separate later Ack/Nack RPC.
func (s *callbackServer) Deliver(ctx context.Context, req *proto.DeliverRequest) (*proto.DeliverAck, error) {
	s.app.mu.Lock()
	fn, ok := s.app.rabbitMQConsumers[req.GetServiceId()+"/"+req.GetQueue()]
	s.app.mu.Unlock()

	if !ok {
		return &proto.DeliverAck{ErrorCode: notRegisteredCode, ErrorMessage: "no consumer registered for " + req.GetServiceId() + "/" + req.GetQueue()}, nil
	}

	delivery := RabbitMQDelivery{Body: req.GetBody(), ContentType: req.GetContentType(), Headers: req.GetHeaders()}
	if err := fn(ctx, delivery); err != nil {
		return &proto.DeliverAck{Ack: false, Requeue: true}, nil
	}
	return &proto.DeliverAck{Ack: true}, nil
}
