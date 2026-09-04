// Package gistredisclient is the thin, injectable client half of
// gist-redis-client - Get/Set/Del/Exists/Incr/IncrBy/Expire, every call
// forwarded to gist-server's own RedisService, routed by this instance's
// own config id. See PLAN_REDIS_PUBSUB_AUTH_TLS.md for why this is a
// first-pass scalar-key surface rather than all of Redis: hashes,
// lists, sets, sorted sets, and Redis's own pub/sub aren't covered yet.
package gistredisclient

import (
	"context"
	"fmt"
	"time"

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

// Get returns key's value. found is false when the key doesn't exist -
// distinct from a genuinely empty value.
func (s *Service) Get(ctx context.Context, key string) (value []byte, found bool, err error) {
	resp, err := rpcconn.MustFor(s.server).Redis.Get(ctx, &proto.GetRequest{ServiceId: s.serviceID, Key: key})
	if err != nil {
		return nil, false, fmt.Errorf("gistredisclient: get: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return nil, false, fmt.Errorf("gistredisclient: get: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return resp.GetValue(), resp.GetFound(), nil
}

// Set stores value under key. ttl <= 0 means no expiry.
func (s *Service) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	resp, err := rpcconn.MustFor(s.server).Redis.Set(ctx, &proto.SetRequest{ServiceId: s.serviceID, Key: key, Value: value, TtlSeconds: int64(ttl.Seconds())})
	if err != nil {
		return fmt.Errorf("gistredisclient: set: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return fmt.Errorf("gistredisclient: set: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return nil
}

// Del removes keys, returning how many actually existed to be removed.
func (s *Service) Del(ctx context.Context, keys ...string) (deleted int64, err error) {
	resp, err := rpcconn.MustFor(s.server).Redis.Del(ctx, &proto.DelRequest{ServiceId: s.serviceID, Keys: keys})
	if err != nil {
		return 0, fmt.Errorf("gistredisclient: del: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return 0, fmt.Errorf("gistredisclient: del: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return resp.GetDeleted(), nil
}

// Exists reports how many of keys exist - a key repeated in the list
// counts twice, matching Redis's own EXISTS semantics.
func (s *Service) Exists(ctx context.Context, keys ...string) (count int64, err error) {
	resp, err := rpcconn.MustFor(s.server).Redis.Exists(ctx, &proto.ExistsRequest{ServiceId: s.serviceID, Keys: keys})
	if err != nil {
		return 0, fmt.Errorf("gistredisclient: exists: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return 0, fmt.Errorf("gistredisclient: exists: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return resp.GetCount(), nil
}

// Incr increments key by 1 (creating it at 0 first if it doesn't exist)
// and returns the new value.
func (s *Service) Incr(ctx context.Context, key string) (value int64, err error) {
	resp, err := rpcconn.MustFor(s.server).Redis.Incr(ctx, &proto.IncrRequest{ServiceId: s.serviceID, Key: key})
	if err != nil {
		return 0, fmt.Errorf("gistredisclient: incr: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return 0, fmt.Errorf("gistredisclient: incr: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return resp.GetValue(), nil
}

// IncrBy increments key by by and returns the new value.
func (s *Service) IncrBy(ctx context.Context, key string, by int64) (value int64, err error) {
	resp, err := rpcconn.MustFor(s.server).Redis.IncrBy(ctx, &proto.IncrByRequest{ServiceId: s.serviceID, Key: key, By: by})
	if err != nil {
		return 0, fmt.Errorf("gistredisclient: incrby: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return 0, fmt.Errorf("gistredisclient: incrby: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return resp.GetValue(), nil
}

// Expire sets key's TTL. existed is false when key didn't exist, so
// there was nothing to set a TTL on.
func (s *Service) Expire(ctx context.Context, key string, ttl time.Duration) (existed bool, err error) {
	resp, err := rpcconn.MustFor(s.server).Redis.Expire(ctx, &proto.ExpireRequest{ServiceId: s.serviceID, Key: key, TtlSeconds: int64(ttl.Seconds())})
	if err != nil {
		return false, fmt.Errorf("gistredisclient: expire: %w", err)
	}
	if resp.GetErrorCode() != "" {
		return false, fmt.Errorf("gistredisclient: expire: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return resp.GetExisted(), nil
}
