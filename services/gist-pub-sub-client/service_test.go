package gistpubsubclient

import (
	"context"
	"testing"

	"google.golang.org/grpc"

	"github.com/wieoapps/gist"
	"github.com/wieoapps/gist/internal/rpcconn"
	"github.com/wieoapps/gist/proto"
)

// fakePubSubClient records the last request of each kind and returns a
// scripted response - exercises the real client-side request building
// with no live gist-server or Pub/Sub needed.
type fakePubSubClient struct {
	publishResp *proto.PubSubPublishResponse
	publishErr  error
	lastPublish *proto.PubSubPublishRequest
	pullResp    *proto.PullResponse
	pullErr     error
	lastPull    *proto.PullRequest
	ackResp     *proto.AckResponse
	ackErr      error
	lastAck     *proto.AckRequest
}

func (f *fakePubSubClient) Publish(_ context.Context, in *proto.PubSubPublishRequest, _ ...grpc.CallOption) (*proto.PubSubPublishResponse, error) {
	f.lastPublish = in
	if f.publishErr != nil {
		return nil, f.publishErr
	}
	return f.publishResp, nil
}

func (f *fakePubSubClient) Pull(_ context.Context, in *proto.PullRequest, _ ...grpc.CallOption) (*proto.PullResponse, error) {
	f.lastPull = in
	if f.pullErr != nil {
		return nil, f.pullErr
	}
	return f.pullResp, nil
}

func (f *fakePubSubClient) Ack(_ context.Context, in *proto.AckRequest, _ ...grpc.CallOption) (*proto.AckResponse, error) {
	f.lastAck = in
	if f.ackErr != nil {
		return nil, f.ackErr
	}
	return f.ackResp, nil
}

func newTestService(fake *fakePubSubClient) *Service {
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{PubSub: fake})
	return NewService(server, "events1")
}

func TestPublish_SendsRequestAndReturnsMessageID(t *testing.T) {
	fake := &fakePubSubClient{publishResp: &proto.PubSubPublishResponse{MessageId: "msg-1"}}
	svc := newTestService(fake)

	id, err := svc.Publish(context.Background(), "order-events", []byte(`{"id":1}`), map[string]string{"type": "created"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != "msg-1" {
		t.Errorf("message id = %q, want msg-1", id)
	}
	if fake.lastPublish.GetServiceId() != "events1" || fake.lastPublish.GetTopic() != "order-events" {
		t.Errorf("unexpected request: %+v", fake.lastPublish)
	}
	if string(fake.lastPublish.GetData()) != `{"id":1}` {
		t.Errorf("data = %q", fake.lastPublish.GetData())
	}

	fake.publishResp = &proto.PubSubPublishResponse{ErrorCode: "internal", ErrorMessage: "boom"}
	if _, err := svc.Publish(context.Background(), "t", nil, nil); err == nil {
		t.Fatal("expected an error when the wire response carries an error_code")
	}
}

func TestPull_MapsMessagesBack(t *testing.T) {
	fake := &fakePubSubClient{pullResp: &proto.PullResponse{Messages: []*proto.PulledMessage{
		{AckId: "ack-1", Data: []byte("payload"), MessageId: "msg-1", Attributes: map[string]string{"k": "v"}},
	}}}
	svc := newTestService(fake)

	messages, err := svc.Pull(context.Background(), "order-events-sub", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}
	if messages[0].AckID != "ack-1" || string(messages[0].Data) != "payload" || messages[0].MessageID != "msg-1" {
		t.Errorf("unexpected message: %+v", messages[0])
	}
	if fake.lastPull.GetMaxMessages() != 10 {
		t.Errorf("max_messages = %d, want 10", fake.lastPull.GetMaxMessages())
	}
}

func TestPull_EmptyResultIsNotAnError(t *testing.T) {
	fake := &fakePubSubClient{pullResp: &proto.PullResponse{}}
	svc := newTestService(fake)

	messages, err := svc.Pull(context.Background(), "sub", 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(messages) != 0 {
		t.Errorf("expected no messages, got %d", len(messages))
	}
}

func TestAck_SendsAllIDs(t *testing.T) {
	fake := &fakePubSubClient{ackResp: &proto.AckResponse{}}
	svc := newTestService(fake)

	if err := svc.Ack(context.Background(), "sub", "ack-1", "ack-2"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fake.lastAck.GetAckIds()) != 2 {
		t.Errorf("expected 2 ack ids sent, got %d", len(fake.lastAck.GetAckIds()))
	}

	fake.ackErr = context.DeadlineExceeded
	if err := svc.Ack(context.Background(), "sub", "ack-3"); err == nil {
		t.Fatal("expected an error when the RPC call itself fails")
	}
}
