package gist

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"

	"github.com/wieoapps/gist/internal/rpcconn"
	"github.com/wieoapps/gist/proto"
)

// fakeRabbitMQClient implements proto.RabbitMQServiceClient - only
// StartConsuming is exercised by these tests (RegisterRabbitMQConsumer's
// own RPC call), the rest exist to satisfy the interface.
type fakeRabbitMQClient struct {
	lastStartConsuming *proto.StartConsumingRequest
	startConsumingResp *proto.StartConsumingResponse
	startConsumingErr  error
}

func (f *fakeRabbitMQClient) Publish(context.Context, *proto.PublishRequest, ...grpc.CallOption) (*proto.PublishResponse, error) {
	panic("not used by these tests")
}

func (f *fakeRabbitMQClient) ExchangeDeclare(context.Context, *proto.ExchangeDeclareRequest, ...grpc.CallOption) (*proto.ExchangeDeclareResponse, error) {
	panic("not used by these tests")
}

func (f *fakeRabbitMQClient) QueueDeclare(context.Context, *proto.QueueDeclareRequest, ...grpc.CallOption) (*proto.QueueDeclareResponse, error) {
	panic("not used by these tests")
}

func (f *fakeRabbitMQClient) QueueBind(context.Context, *proto.QueueBindRequest, ...grpc.CallOption) (*proto.QueueBindResponse, error) {
	panic("not used by these tests")
}

func (f *fakeRabbitMQClient) StartConsuming(_ context.Context, in *proto.StartConsumingRequest, _ ...grpc.CallOption) (*proto.StartConsumingResponse, error) {
	f.lastStartConsuming = in
	if f.startConsumingErr != nil {
		return nil, f.startConsumingErr
	}
	if f.startConsumingResp != nil {
		return f.startConsumingResp, nil
	}
	return &proto.StartConsumingResponse{}, nil
}

func TestRegisterRabbitMQConsumer_CallsStartConsuming_ManualAckByDefault(t *testing.T) {
	fake := &fakeRabbitMQClient{}
	s := &Server{}
	rpcconn.Register(s, &rpcconn.Clients{RabbitMQ: fake})
	defer rpcconn.Unregister(s)

	type dep struct{ tag string }
	err := RegisterRabbitMQConsumer("rmq1", "orders", dep{tag: "d"}, func(dep, context.Context, RabbitMQDelivery) error { return nil })(s)
	if err != nil {
		t.Fatalf("RegisterRabbitMQConsumer option failed: %v", err)
	}

	if fake.lastStartConsuming.GetServiceId() != "rmq1" {
		t.Errorf("service_id = %q, want rmq1", fake.lastStartConsuming.GetServiceId())
	}
	if fake.lastStartConsuming.GetQueue() != "orders" {
		t.Errorf("queue = %q, want orders", fake.lastStartConsuming.GetQueue())
	}
	if fake.lastStartConsuming.GetAutoAck() {
		t.Error("expected auto_ack=false by default")
	}
}

func TestRegisterRabbitMQConsumer_WithRabbitMQAutoAck_SetsAutoAck(t *testing.T) {
	fake := &fakeRabbitMQClient{}
	s := &Server{}
	rpcconn.Register(s, &rpcconn.Clients{RabbitMQ: fake})
	defer rpcconn.Unregister(s)

	err := RegisterRabbitMQConsumer("rmq1", "orders", struct{}{}, func(struct{}, context.Context, RabbitMQDelivery) error { return nil }, WithRabbitMQAutoAck())(s)
	if err != nil {
		t.Fatalf("RegisterRabbitMQConsumer option failed: %v", err)
	}
	if !fake.lastStartConsuming.GetAutoAck() {
		t.Error("expected auto_ack=true when WithRabbitMQAutoAck is passed")
	}
}

func TestRegisterRabbitMQConsumer_TransportError_PropagatesFromOption(t *testing.T) {
	fake := &fakeRabbitMQClient{startConsumingErr: errors.New("boom")}
	s := &Server{}
	rpcconn.Register(s, &rpcconn.Clients{RabbitMQ: fake})
	defer rpcconn.Unregister(s)

	err := RegisterRabbitMQConsumer("rmq1", "orders", struct{}{}, func(struct{}, context.Context, RabbitMQDelivery) error { return nil })(s)
	if err == nil {
		t.Fatal("expected the option to fail when the RPC transport fails")
	}
}

func TestRegisterRabbitMQConsumer_WireError_PropagatesFromOption(t *testing.T) {
	fake := &fakeRabbitMQClient{startConsumingResp: &proto.StartConsumingResponse{ErrorCode: "internal", ErrorMessage: "no such queue"}}
	s := &Server{}
	rpcconn.Register(s, &rpcconn.Clients{RabbitMQ: fake})
	defer rpcconn.Unregister(s)

	err := RegisterRabbitMQConsumer("rmq1", "orders", struct{}{}, func(struct{}, context.Context, RabbitMQDelivery) error { return nil })(s)
	if err == nil {
		t.Fatal("expected the option to fail when the wire response carries an error_code")
	}
}

func TestDeliver_RunsRegisteredConsumer_AcksOnSuccess(t *testing.T) {
	s := &Server{}
	var gotDelivery RabbitMQDelivery
	var gotDep string
	s.rabbitMQConsumers = map[string]func(context.Context, RabbitMQDelivery) error{
		"rmq1/orders": func(ctx context.Context, d RabbitMQDelivery) error {
			gotDelivery = d
			gotDep = "handled"
			return nil
		},
	}

	cs := &callbackServer{app: s}
	resp, err := cs.Deliver(context.Background(), &proto.DeliverRequest{
		ServiceId:   "rmq1",
		Queue:       "orders",
		Body:        []byte(`{"id":1}`),
		ContentType: "application/json",
		Headers:     map[string]string{"x-retry": "0"},
	})
	if err != nil {
		t.Fatalf("Deliver returned a transport error: %v", err)
	}
	if resp.GetErrorCode() != "" {
		t.Fatalf("unexpected error: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	if !resp.GetAck() {
		t.Error("expected ack=true on a successful handler")
	}
	if gotDep != "handled" {
		t.Error("expected the registered handler to have run")
	}
	if string(gotDelivery.Body) != `{"id":1}` {
		t.Errorf("Body = %q", gotDelivery.Body)
	}
	if gotDelivery.ContentType != "application/json" {
		t.Errorf("ContentType = %q", gotDelivery.ContentType)
	}
	if gotDelivery.Headers["x-retry"] != "0" {
		t.Errorf("Headers = %+v", gotDelivery.Headers)
	}
}

func TestDeliver_HandlerError_NacksWithRequeue(t *testing.T) {
	s := &Server{}
	s.rabbitMQConsumers = map[string]func(context.Context, RabbitMQDelivery) error{
		"rmq1/orders": func(context.Context, RabbitMQDelivery) error { return errors.New("db down") },
	}

	cs := &callbackServer{app: s}
	resp, err := cs.Deliver(context.Background(), &proto.DeliverRequest{ServiceId: "rmq1", Queue: "orders"})
	if err != nil {
		t.Fatalf("Deliver returned a transport error: %v", err)
	}
	if resp.GetAck() {
		t.Error("expected ack=false when the handler returns an error")
	}
	if !resp.GetRequeue() {
		t.Error("expected requeue=true when the handler returns an error")
	}
}

func TestDeliver_UnregisteredPair_ReturnsNotRegistered(t *testing.T) {
	s := &Server{}
	cs := &callbackServer{app: s}
	resp, err := cs.Deliver(context.Background(), &proto.DeliverRequest{ServiceId: "rmq1", Queue: "unknown-queue"})
	if err != nil {
		t.Fatalf("Deliver returned a transport error: %v", err)
	}
	if resp.GetErrorCode() != notRegisteredCode {
		t.Errorf("expected error code %q, got %q", notRegisteredCode, resp.GetErrorCode())
	}
}
