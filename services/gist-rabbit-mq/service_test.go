package gistrabbitmq

import (
	"context"
	"testing"

	"google.golang.org/grpc"

	"github.com/wieoapps/gist"
	"github.com/wieoapps/gist-proto"
	"github.com/wieoapps/gist/internal/rpcconn"
)

// fakeRabbitMQClient records the last request of each kind and returns a
// scripted response - exercises the real client-side request building
// with no live broker or gist-server needed.
type fakeRabbitMQClient struct {
	lastPublish  *gistproto.PublishRequest
	publishResp  *gistproto.PublishResponse
	publishErr   error
	lastExchange *gistproto.ExchangeDeclareRequest
	exchangeResp *gistproto.ExchangeDeclareResponse
	exchangeErr  error
	lastQueue    *gistproto.QueueDeclareRequest
	queueResp    *gistproto.QueueDeclareResponse
	queueErr     error
	lastBind     *gistproto.QueueBindRequest
	bindResp     *gistproto.QueueBindResponse
	bindErr      error
}

func (f *fakeRabbitMQClient) Publish(_ context.Context, in *gistproto.PublishRequest, _ ...grpc.CallOption) (*gistproto.PublishResponse, error) {
	f.lastPublish = in
	if f.publishErr != nil {
		return nil, f.publishErr
	}
	return f.publishResp, nil
}

func (f *fakeRabbitMQClient) ExchangeDeclare(_ context.Context, in *gistproto.ExchangeDeclareRequest, _ ...grpc.CallOption) (*gistproto.ExchangeDeclareResponse, error) {
	f.lastExchange = in
	if f.exchangeErr != nil {
		return nil, f.exchangeErr
	}
	return f.exchangeResp, nil
}

func (f *fakeRabbitMQClient) QueueDeclare(_ context.Context, in *gistproto.QueueDeclareRequest, _ ...grpc.CallOption) (*gistproto.QueueDeclareResponse, error) {
	f.lastQueue = in
	if f.queueErr != nil {
		return nil, f.queueErr
	}
	return f.queueResp, nil
}

func (f *fakeRabbitMQClient) QueueBind(_ context.Context, in *gistproto.QueueBindRequest, _ ...grpc.CallOption) (*gistproto.QueueBindResponse, error) {
	f.lastBind = in
	if f.bindErr != nil {
		return nil, f.bindErr
	}
	return f.bindResp, nil
}

func (f *fakeRabbitMQClient) StartConsuming(context.Context, *gistproto.StartConsumingRequest, ...grpc.CallOption) (*gistproto.StartConsumingResponse, error) {
	panic("not used by this package's own client - see gistsdk.RegisterRabbitMQConsumer")
}

func newTestService(fake *fakeRabbitMQClient) *Service {
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{RabbitMQ: fake})
	return NewService(server, "rmq1")
}

func TestPublish_SendsRequestAndReportsWireError(t *testing.T) {
	fake := &fakeRabbitMQClient{publishResp: &gistproto.PublishResponse{}}
	svc := newTestService(fake)

	err := svc.Publish(context.Background(), "orders-exchange", "orders.created", []byte(`{"id":1}`), "application/json", map[string]string{"x-retry": "0"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.lastPublish.GetServiceId() != "rmq1" {
		t.Errorf("service_id = %q, want rmq1", fake.lastPublish.GetServiceId())
	}
	if fake.lastPublish.GetExchange() != "orders-exchange" || fake.lastPublish.GetRoutingKey() != "orders.created" {
		t.Errorf("unexpected exchange/routing key: %+v", fake.lastPublish)
	}
	if string(fake.lastPublish.GetBody()) != `{"id":1}` {
		t.Errorf("body = %q", fake.lastPublish.GetBody())
	}

	fake.publishResp = &gistproto.PublishResponse{ErrorCode: "internal", ErrorMessage: "no route"}
	if err := svc.Publish(context.Background(), "x", "y", nil, "", nil); err == nil {
		t.Fatal("expected an error when the wire response carries an error_code")
	}
}

func TestExchangeDeclare_SendsRequest(t *testing.T) {
	fake := &fakeRabbitMQClient{exchangeResp: &gistproto.ExchangeDeclareResponse{}}
	svc := newTestService(fake)

	if err := svc.ExchangeDeclare(context.Background(), "orders-exchange", "topic", true, false, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.lastExchange.GetName() != "orders-exchange" || fake.lastExchange.GetKind() != "topic" {
		t.Errorf("unexpected request: %+v", fake.lastExchange)
	}
	if !fake.lastExchange.GetDurable() {
		t.Error("expected durable=true")
	}
}

func TestQueueDeclare_ReturnsBrokerAssignedName(t *testing.T) {
	fake := &fakeRabbitMQClient{queueResp: &gistproto.QueueDeclareResponse{Name: "amq.gen-abc123"}}
	svc := newTestService(fake)

	name, err := svc.QueueDeclare(context.Background(), "", true, false, true, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if name != "amq.gen-abc123" {
		t.Errorf("name = %q, want amq.gen-abc123", name)
	}
	if fake.lastQueue.GetExclusive() != true {
		t.Error("expected exclusive=true to have been sent")
	}
}

func TestQueueBind_SendsRequestAndReportsTransportError(t *testing.T) {
	fake := &fakeRabbitMQClient{bindErr: context.DeadlineExceeded}
	svc := newTestService(fake)

	err := svc.QueueBind(context.Background(), "orders-queue", "orders-exchange", "orders.created", nil)
	if err == nil {
		t.Fatal("expected an error when the RPC call itself fails")
	}
	if fake.lastBind.GetQueue() != "orders-queue" {
		t.Errorf("queue = %q, want orders-queue", fake.lastBind.GetQueue())
	}
}
