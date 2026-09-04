package gistredisclient

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/wieoapps/gist"
	"github.com/wieoapps/gist/internal/rpcconn"
	"github.com/wieoapps/gist/proto"
)

// fakeRedisClient records the last request of each kind and returns a
// scripted response - exercises the real client-side request building
// with no live gist-server or Redis needed.
type fakeRedisClient struct {
	getResp    *proto.GetResponse
	getErr     error
	lastGet    *proto.GetRequest
	setResp    *proto.SetResponse
	setErr     error
	lastSet    *proto.SetRequest
	delResp    *proto.DelResponse
	delErr     error
	lastDel    *proto.DelRequest
	existsResp *proto.ExistsResponse
	existsErr  error
	lastExists *proto.ExistsRequest
	incrResp   *proto.IncrResponse
	incrErr    error
	lastIncr   *proto.IncrRequest
	incrByResp *proto.IncrByResponse
	incrByErr  error
	lastIncrBy *proto.IncrByRequest
	expireResp *proto.ExpireResponse
	expireErr  error
	lastExpire *proto.ExpireRequest
}

func (f *fakeRedisClient) Get(_ context.Context, in *proto.GetRequest, _ ...grpc.CallOption) (*proto.GetResponse, error) {
	f.lastGet = in
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.getResp, nil
}

func (f *fakeRedisClient) Set(_ context.Context, in *proto.SetRequest, _ ...grpc.CallOption) (*proto.SetResponse, error) {
	f.lastSet = in
	if f.setErr != nil {
		return nil, f.setErr
	}
	return f.setResp, nil
}

func (f *fakeRedisClient) Del(_ context.Context, in *proto.DelRequest, _ ...grpc.CallOption) (*proto.DelResponse, error) {
	f.lastDel = in
	if f.delErr != nil {
		return nil, f.delErr
	}
	return f.delResp, nil
}

func (f *fakeRedisClient) Exists(_ context.Context, in *proto.ExistsRequest, _ ...grpc.CallOption) (*proto.ExistsResponse, error) {
	f.lastExists = in
	if f.existsErr != nil {
		return nil, f.existsErr
	}
	return f.existsResp, nil
}

func (f *fakeRedisClient) Incr(_ context.Context, in *proto.IncrRequest, _ ...grpc.CallOption) (*proto.IncrResponse, error) {
	f.lastIncr = in
	if f.incrErr != nil {
		return nil, f.incrErr
	}
	return f.incrResp, nil
}

func (f *fakeRedisClient) IncrBy(_ context.Context, in *proto.IncrByRequest, _ ...grpc.CallOption) (*proto.IncrByResponse, error) {
	f.lastIncrBy = in
	if f.incrByErr != nil {
		return nil, f.incrByErr
	}
	return f.incrByResp, nil
}

func (f *fakeRedisClient) Expire(_ context.Context, in *proto.ExpireRequest, _ ...grpc.CallOption) (*proto.ExpireResponse, error) {
	f.lastExpire = in
	if f.expireErr != nil {
		return nil, f.expireErr
	}
	return f.expireResp, nil
}

func newTestService(fake *fakeRedisClient) *Service {
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{Redis: fake})
	return NewService(server, "cache1")
}

func TestGet_FoundAndNotFound(t *testing.T) {
	fake := &fakeRedisClient{getResp: &proto.GetResponse{Value: []byte("hello"), Found: true}}
	svc := newTestService(fake)

	value, found, err := svc.Get(context.Background(), "greeting")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found || string(value) != "hello" {
		t.Errorf("value=%q found=%v, want %q true", value, found, "hello")
	}
	if fake.lastGet.GetServiceId() != "cache1" || fake.lastGet.GetKey() != "greeting" {
		t.Errorf("unexpected request: %+v", fake.lastGet)
	}

	fake.getResp = &proto.GetResponse{Found: false}
	_, found, err = svc.Get(context.Background(), "missing")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Error("expected found=false for a missing key")
	}
}

func TestSet_SendsTTLInSeconds(t *testing.T) {
	fake := &fakeRedisClient{setResp: &proto.SetResponse{}}
	svc := newTestService(fake)

	if err := svc.Set(context.Background(), "k", []byte("v"), 90*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.lastSet.GetTtlSeconds() != 90 {
		t.Errorf("ttl_seconds = %d, want 90", fake.lastSet.GetTtlSeconds())
	}

	fake.setResp = &proto.SetResponse{ErrorCode: "internal", ErrorMessage: "boom"}
	if err := svc.Set(context.Background(), "k", []byte("v"), 0); err == nil {
		t.Fatal("expected an error when the wire response carries an error_code")
	}
}

func TestDel_ReturnsDeletedCount(t *testing.T) {
	fake := &fakeRedisClient{delResp: &proto.DelResponse{Deleted: 2}}
	svc := newTestService(fake)

	deleted, err := svc.Del(context.Background(), "a", "b", "c")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if deleted != 2 {
		t.Errorf("deleted = %d, want 2", deleted)
	}
	if len(fake.lastDel.GetKeys()) != 3 {
		t.Errorf("expected 3 keys sent, got %d", len(fake.lastDel.GetKeys()))
	}
}

func TestExists_ReturnsCount(t *testing.T) {
	fake := &fakeRedisClient{existsResp: &proto.ExistsResponse{Count: 1}}
	svc := newTestService(fake)

	count, err := svc.Exists(context.Background(), "a", "b")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 1 {
		t.Errorf("count = %d, want 1", count)
	}
}

func TestIncrAndIncrBy(t *testing.T) {
	fake := &fakeRedisClient{incrResp: &proto.IncrResponse{Value: 1}, incrByResp: &proto.IncrByResponse{Value: 11}}
	svc := newTestService(fake)

	v, err := svc.Incr(context.Background(), "counter")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != 1 {
		t.Errorf("Incr value = %d, want 1", v)
	}

	v, err = svc.IncrBy(context.Background(), "counter", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != 11 {
		t.Errorf("IncrBy value = %d, want 11", v)
	}
	if fake.lastIncrBy.GetBy() != 10 {
		t.Errorf("by = %d, want 10", fake.lastIncrBy.GetBy())
	}
}

func TestExpire_ReportsWhetherKeyExisted(t *testing.T) {
	fake := &fakeRedisClient{expireResp: &proto.ExpireResponse{Existed: false}}
	svc := newTestService(fake)

	existed, err := svc.Expire(context.Background(), "missing", time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if existed {
		t.Error("expected existed=false")
	}
	if fake.lastExpire.GetTtlSeconds() != 60 {
		t.Errorf("ttl_seconds = %d, want 60", fake.lastExpire.GetTtlSeconds())
	}
}

func TestGet_ReportsTransportError(t *testing.T) {
	fake := &fakeRedisClient{getErr: context.DeadlineExceeded}
	svc := newTestService(fake)

	if _, _, err := svc.Get(context.Background(), "k"); err == nil {
		t.Fatal("expected an error when the RPC call itself fails")
	}
}
