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

type dispatchEntry struct {
	fn DispatchFunc

	declaredErrors map[int32]string
}

type CustomService interface {
	PostBuild() error
	Start(ctx context.Context, ready func()) error
	Stop(ctx context.Context) error
}

type customServiceKind struct {
	build func(configJSON json.RawMessage) (CustomService, error)

	mu        sync.Mutex
	instances map[string]CustomService
}

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

type RabbitMQDelivery struct {
	Body        []byte
	ContentType string
	Headers     map[string]string
}

type rabbitMQConsumerOptions struct {
	autoAck bool
}

type RabbitMQConsumerOption func(*rabbitMQConsumerOptions)

func WithRabbitMQAutoAck() RabbitMQConsumerOption {
	return func(o *rabbitMQConsumerOptions) { o.autoAck = true }
}

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

type StateMachineTriggerFn func(ctx context.Context, statable any) error

type StateMachineTrigger struct {
	OnEnter  []StateMachineTriggerFn
	OnAction StateMachineTriggerFn
	OnExit   []StateMachineTriggerFn
}

func (a *Server) RegisterStateMachineTrigger(serviceID, trigger string, action StateMachineTrigger) {
	a.mu.Lock()
	if a.stateMachineTriggers == nil {
		a.stateMachineTriggers = map[string]StateMachineTrigger{}
	}
	a.stateMachineTriggers[serviceID+"/"+trigger] = action
	a.mu.Unlock()
}

func (a *Server) StateMachineTrigger(serviceID, trigger string) (StateMachineTrigger, bool) {
	a.mu.Lock()
	action, ok := a.stateMachineTriggers[serviceID+"/"+trigger]
	a.mu.Unlock()
	return action, ok
}

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

	Logger Logger

	callbackServer *grpc.Server

	mu                   sync.Mutex
	dispatch             map[string]dispatchEntry
	customServices       map[string]*customServiceKind
	schedulers           map[string]func(ctx context.Context)
	stateMachineTriggers map[string]StateMachineTrigger
	rabbitMQConsumers    map[string]func(context.Context, RabbitMQDelivery) error

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
		customServices:       map[string]*customServiceKind{},
		schedulers:           map[string]func(ctx context.Context){},
		stateMachineTriggers: map[string]StateMachineTrigger{},
		rabbitMQConsumers:    map[string]func(context.Context, RabbitMQDelivery) error{},
		bubbleUpRawErrors:    bubbleUpErrors,
	}
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

		message := defaultMessage
		var traceID string
		switch {
		case isCodeError:
			message = err.Error()
		case s.app.bubbleUpRawErrors:
			message = err.Error()
		case s.app.Logger != nil:
			traceID = newTraceID()
			s.app.Logger.Error("gistsdk: endpoint returned an error with no public message",
				map[string]any{"endpoint": req.GetEndpointId(), "code": code, "error": err, "trace_id": traceID})
		}

		return &proto.InvokeResponse{ErrorCode: code, ErrorMessage: message, ErrorTraceId: traceID}, nil
	}

	return &proto.InvokeResponse{Output: output}, nil
}

const notRegisteredCode = "not_registered"

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
