// Package gistrabbitmqclient is the thin, injectable client half of
// gist-rabbit-mq-client - Publish/ExchangeDeclare/QueueDeclare/QueueBind, every
// call forwarded to gist-server's own RabbitMQService, routed by this
// instance's own config id. Consuming a queue (the push direction, not
// a call this client makes) is a separate concern - see
// gistsdk.RegisterRabbitMQConsumer, a root-level Option like
// RegisterScheduleFunc, not a method here.
package gistrabbitmqclient

import (
	"context"
	"fmt"

	"github.com/wieoapps/gist"
	gistproto "github.com/wieoapps/gist/proto"
	"github.com/wieoapps/gist/internal/rpcconn"
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

// Publish sends one message. exchange = "" targets the default
// exchange, where routingKey is treated as a queue name - the standard
// AMQP "publish straight to a queue by name" shortcut.
func (s *Service) Publish(ctx context.Context, exchange, routingKey string, body []byte, contentType string, headers map[string]string) error {
	resp, err := rpcconn.MustFor(s.server).RabbitMQ.Publish(ctx, &gistproto.PublishRequest{
		ServiceId:   s.serviceID,
		Exchange:    exchange,
		RoutingKey:  routingKey,
		Body:        body,
		ContentType: contentType,
		Headers:     headers,
	})
	if err != nil {
		return fmt.Errorf("gistrabbitmqclient: publish: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return fmt.Errorf("gistrabbitmqclient: publish: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return nil
}

// ExchangeDeclare declares exchange name of the given kind ("direct" |
// "fanout" | "topic" | "headers"). args is string-only (see
// rabbitmq.proto's own doc comment on why) - a typed AMQP arg (e.g. an
// integer TTL) is passed as its string form.
func (s *Service) ExchangeDeclare(ctx context.Context, name, kind string, durable, autoDelete bool, args map[string]string) error {
	resp, err := rpcconn.MustFor(s.server).RabbitMQ.ExchangeDeclare(ctx, &gistproto.ExchangeDeclareRequest{
		ServiceId:  s.serviceID,
		Name:       name,
		Kind:       kind,
		Durable:    durable,
		AutoDelete: autoDelete,
		Args:       args,
	})
	if err != nil {
		return fmt.Errorf("gistrabbitmqclient: exchange declare: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return fmt.Errorf("gistrabbitmqclient: exchange declare: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return nil
}

// QueueDeclare declares a queue and returns its real name - only
// differs from name when name was empty, letting the broker generate
// one (RabbitMQ's usual pattern for a private, per-consumer queue).
func (s *Service) QueueDeclare(ctx context.Context, name string, durable, autoDelete, exclusive bool, args map[string]string) (string, error) {
	resp, err := rpcconn.MustFor(s.server).RabbitMQ.QueueDeclare(ctx, &gistproto.QueueDeclareRequest{
		ServiceId:  s.serviceID,
		Name:       name,
		Durable:    durable,
		AutoDelete: autoDelete,
		Exclusive:  exclusive,
		Args:       args,
	})
	if err != nil {
		return "", fmt.Errorf("gistrabbitmqclient: queue declare: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return "", fmt.Errorf("gistrabbitmqclient: queue declare: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return resp.GetName(), nil
}

// QueueBind binds queue to exchange under routingKey.
func (s *Service) QueueBind(ctx context.Context, queue, exchange, routingKey string, args map[string]string) error {
	resp, err := rpcconn.MustFor(s.server).RabbitMQ.QueueBind(ctx, &gistproto.QueueBindRequest{
		ServiceId:  s.serviceID,
		Queue:      queue,
		Exchange:   exchange,
		RoutingKey: routingKey,
		Args:       args,
	})
	if err != nil {
		return fmt.Errorf("gistrabbitmqclient: queue bind: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return fmt.Errorf("gistrabbitmqclient: queue bind: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return nil
}
