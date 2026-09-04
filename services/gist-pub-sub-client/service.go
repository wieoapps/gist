// Package gistpubsubclient is the thin, injectable client half of
// gist-pub-sub-client - Publish/Pull/Ack, every call forwarded to
// gist-server's own PubSubService, routed by this instance's own config
// id. Pull is a single synchronous poll, not a long-lived streaming
// subscription - see pubsub.proto's own doc comment for why - so a
// caller polls Pull on its own schedule and Acks whatever it actually
// processed.
package gistpubsubclient

import (
	"context"
	"fmt"

	"github.com/wieoapps/gist"
	"github.com/wieoapps/gist/internal/rpcconn"
	"github.com/wieoapps/gist/proto"
)

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

// Publish sends one message to topic (short name, e.g. "order-events" -
// the project is already known server-side from this instance's own
// config, so a caller never repeats it), returning Pub/Sub's own
// generated message ID.
func (s *Service) Publish(ctx context.Context, topic string, data []byte, attributes map[string]string) (messageID string, err error) {
	resp, err := rpcconn.MustFor(s.server).PubSub.Publish(ctx, &proto.PubSubPublishRequest{ServiceId: s.serviceID, Topic: topic, Data: data, Attributes: attributes})
	if err != nil {
		return "", fmt.Errorf("gistpubsubclient: publish: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return "", fmt.Errorf("gistpubsubclient: publish: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return resp.GetMessageId(), nil
}

// PulledMessage is one message returned by Pull, still unacknowledged.
type PulledMessage struct {
	AckID      string
	Data       []byte
	Attributes map[string]string
	MessageID  string
}

// Pull polls subscription once for up to maxMessages waiting messages.
// Returns an empty slice, not an error, when nothing is waiting.
func (s *Service) Pull(ctx context.Context, subscription string, maxMessages int32) ([]PulledMessage, error) {
	resp, err := rpcconn.MustFor(s.server).PubSub.Pull(ctx, &proto.PullRequest{ServiceId: s.serviceID, Subscription: subscription, MaxMessages: maxMessages})
	if err != nil {
		return nil, fmt.Errorf("gistpubsubclient: pull: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return nil, fmt.Errorf("gistpubsubclient: pull: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}

	messages := make([]PulledMessage, len(resp.GetMessages()))
	for i, m := range resp.GetMessages() {
		messages[i] = PulledMessage{AckID: m.GetAckId(), Data: m.GetData(), Attributes: m.GetAttributes(), MessageID: m.GetMessageId()}
	}
	return messages, nil
}

// Ack acknowledges every ID in ackIDs against subscription, so Pub/Sub
// doesn't redeliver them.
func (s *Service) Ack(ctx context.Context, subscription string, ackIDs ...string) error {
	resp, err := rpcconn.MustFor(s.server).PubSub.Ack(ctx, &proto.AckRequest{ServiceId: s.serviceID, Subscription: subscription, AckIds: ackIDs})
	if err != nil {
		return fmt.Errorf("gistpubsubclient: ack: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return fmt.Errorf("gistpubsubclient: ack: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return nil
}
