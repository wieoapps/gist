package gisthttpclient

import (
	"context"
	"net/http"
	"testing"

	"github.com/wieoapps/gist"
	"github.com/wieoapps/gist/internal/rpcconn"
	"github.com/wieoapps/gist/proto"
	"github.com/wieoapps/gist/services/gist-http-client/delete"
	"github.com/wieoapps/gist/services/gist-http-client/get"
	"github.com/wieoapps/gist/services/gist-http-client/patch"
	"github.com/wieoapps/gist/services/gist-http-client/post"
	"github.com/wieoapps/gist/services/gist-http-client/put"
	"google.golang.org/grpc"
)

// fakeHTTPClient records the last request it received and returns a
// scripted response - lets us exercise Service.Get/Post/Put/Patch/Delete
// end to end (real wire-building/decoding) with no gRPC connection or
// live gist-server needed at all.
type fakeHTTPClient struct {
	lastReq *proto.HTTPDoRequest
	resp    *proto.HTTPDoResponse
	err     error
}

func (f *fakeHTTPClient) Do(_ context.Context, in *proto.HTTPDoRequest, _ ...grpc.CallOption) (*proto.HTTPDoResponse, error) {
	f.lastReq = in
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

func newTestService(fake *fakeHTTPClient) *Service {
	server := &gist.Server{}
	rpcconn.Register(server, &rpcconn.Clients{HTTPClient: fake})
	return NewService(server, "svc1")
}

func TestService_Get_SendsGETMethod(t *testing.T) {
	fake := &fakeHTTPClient{resp: &proto.HTTPDoResponse{StatusCode: 200}}
	svc := newTestService(fake)

	svc.Get(context.Background(), "/echo", get.WithHeader("X-Test", "hi"), get.WithQueryParams("q", "1"))

	if fake.lastReq.GetMethod() != http.MethodGet {
		t.Fatalf("expected method GET, got %q", fake.lastReq.GetMethod())
	}
	if fake.lastReq.GetServiceId() != "svc1" {
		t.Fatalf("expected service id svc1, got %q", fake.lastReq.GetServiceId())
	}
	if fake.lastReq.GetUrl() != "/echo" {
		t.Fatalf("expected url /echo, got %q", fake.lastReq.GetUrl())
	}
	if fake.lastReq.GetHeaders()["X-Test"] != "hi" {
		t.Fatalf("expected header X-Test=hi, got %+v", fake.lastReq.GetHeaders())
	}
	if fake.lastReq.GetQueryParams()["q"] != "1" {
		t.Fatalf("expected query param q=1, got %+v", fake.lastReq.GetQueryParams())
	}
}

func TestService_Post_SendsPOSTMethodAndBody(t *testing.T) {
	fake := &fakeHTTPClient{resp: &proto.HTTPDoResponse{StatusCode: 200}}
	svc := newTestService(fake)

	svc.Post(context.Background(), "/echo", post.WithBody([]byte("hello")))

	if fake.lastReq.GetMethod() != http.MethodPost {
		t.Fatalf("expected method POST, got %q", fake.lastReq.GetMethod())
	}
	if string(fake.lastReq.GetBody()) != "hello" {
		t.Fatalf("expected body 'hello', got %q", fake.lastReq.GetBody())
	}
}

func TestService_Put_SendsPUTMethod(t *testing.T) {
	fake := &fakeHTTPClient{resp: &proto.HTTPDoResponse{StatusCode: 200}}
	svc := newTestService(fake)
	svc.Put(context.Background(), "/echo", put.WithBody([]byte("x")))
	if fake.lastReq.GetMethod() != http.MethodPut {
		t.Fatalf("expected method PUT, got %q", fake.lastReq.GetMethod())
	}
}

func TestService_Patch_SendsPATCHMethod(t *testing.T) {
	fake := &fakeHTTPClient{resp: &proto.HTTPDoResponse{StatusCode: 200}}
	svc := newTestService(fake)
	svc.Patch(context.Background(), "/echo", patch.WithBody([]byte("x")))
	if fake.lastReq.GetMethod() != http.MethodPatch {
		t.Fatalf("expected method PATCH, got %q", fake.lastReq.GetMethod())
	}
}

func TestService_Delete_SendsDELETEMethod(t *testing.T) {
	fake := &fakeHTTPClient{resp: &proto.HTTPDoResponse{StatusCode: 200}}
	svc := newTestService(fake)
	svc.Delete(context.Background(), "/echo", delete.WithHeader("X-Test", "bye"))
	if fake.lastReq.GetMethod() != http.MethodDelete {
		t.Fatalf("expected method DELETE, got %q", fake.lastReq.GetMethod())
	}
	if fake.lastReq.GetHeaders()["X-Test"] != "bye" {
		t.Fatalf("expected header X-Test=bye, got %+v", fake.lastReq.GetHeaders())
	}
}

func TestService_WithOmitResponseHeaders_SetsFlag(t *testing.T) {
	fake := &fakeHTTPClient{resp: &proto.HTTPDoResponse{StatusCode: 200}}
	svc := newTestService(fake)
	svc.Get(context.Background(), "/echo", get.WithOmitResponseHeaders())
	if !fake.lastReq.GetOmitResponseHeaders() {
		t.Fatal("expected omit_response_headers to be set on the wire request")
	}
}

func TestService_Do_DecodesMultiValueResponseHeaders(t *testing.T) {
	fake := &fakeHTTPClient{resp: &proto.HTTPDoResponse{
		StatusCode: 200,
		ResponseHeaders: map[string]*proto.HeaderValues{
			"Set-Cookie": {Values: []string{"a=1", "b=2"}},
		},
	}}
	svc := newTestService(fake)
	resp := svc.Get(context.Background(), "/echo")

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	got := resp.Headers["Set-Cookie"]
	if len(got) != 2 || got[0] != "a=1" || got[1] != "b=2" {
		t.Fatalf("expected two Set-Cookie values to survive the round trip, got %+v", got)
	}
}

func TestService_Do_WireErrorMessageBecomesError(t *testing.T) {
	fake := &fakeHTTPClient{resp: &proto.HTTPDoResponse{StatusCode: 500, ErrorMessage: "boom"}}
	svc := newTestService(fake)
	resp := svc.Get(context.Background(), "/echo")

	if resp.Error == nil {
		t.Fatal("expected a non-nil error when the wire response carries an error_message")
	}
	if resp.Error.Error() != "boom" {
		t.Fatalf("expected error message 'boom', got %q", resp.Error.Error())
	}
	if resp.StatusCode != 500 {
		t.Fatalf("expected status code 500, got %d", resp.StatusCode)
	}
}

func TestService_Do_TransportErrorBecomesResponseError(t *testing.T) {
	fake := &fakeHTTPClient{err: context.DeadlineExceeded}
	svc := newTestService(fake)
	resp := svc.Get(context.Background(), "/echo")

	if resp.Error == nil {
		t.Fatal("expected a non-nil error when the RPC call itself fails")
	}
	if resp.StatusCode != 500 {
		t.Fatalf("expected a synthetic 500 status code on transport failure, got %d", resp.StatusCode)
	}
}

func TestService_Do_NoResponseHeaders_HeadersNil(t *testing.T) {
	fake := &fakeHTTPClient{resp: &proto.HTTPDoResponse{StatusCode: 200}}
	svc := newTestService(fake)
	resp := svc.Get(context.Background(), "/echo")
	if resp.Headers != nil {
		t.Fatalf("expected nil headers when the wire response has none, got %+v", resp.Headers)
	}
}
