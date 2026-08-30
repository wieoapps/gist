package gisthttpclient

import (
	"context"
	"net/http"

	"github.com/wieoapps/gist"
	"github.com/wieoapps/gist-proto"
	"github.com/wieoapps/gist/internal/rpcconn"
	"github.com/wieoapps/gist/services/gist-http-client/delete"
	"github.com/wieoapps/gist/services/gist-http-client/get"
	"github.com/wieoapps/gist/services/gist-http-client/option"
	"github.com/wieoapps/gist/services/gist-http-client/patch"
	"github.com/wieoapps/gist/services/gist-http-client/post"
	"github.com/wieoapps/gist/services/gist-http-client/put"
	"github.com/wieoapps/gist/services/gist-http-client/request"
)

type Response struct {
	StatusCode int
	Headers    http.Header
	Body       []byte
	Error      error
}

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

func (s *Service) do(ctx context.Context, r request.Request) Response {
	resp, err := rpcconn.MustFor(s.server).HTTPClient.Do(ctx, &gistproto.HTTPDoRequest{
		ServiceId:           s.serviceID,
		Method:              r.Method,
		Url:                 r.Url,
		Headers:             r.Headers,
		QueryParams:         r.QueryParams,
		Body:                r.Body,
		OmitResponseHeaders: r.OmitResponseHeaders,
	})
	if err != nil {
		return Response{StatusCode: 500, Error: err}
	}

	var headers http.Header
	if len(resp.GetResponseHeaders()) > 0 {
		headers = http.Header{}
		for k, v := range resp.GetResponseHeaders() {
			headers[k] = v.GetValues()
		}
	}

	var respErr error
	if resp.GetErrorMessage() != "" {
		respErr = errString(resp.GetErrorMessage())
	}
	return Response{StatusCode: int(resp.GetStatusCode()), Headers: headers, Body: resp.GetBody(), Error: respErr}
}

type errString string

func (e errString) Error() string { return string(e) }

func (s *Service) Get(ctx context.Context, endpoint string, options ...get.Option) Response {
	r := request.Request{Method: http.MethodGet, Url: endpoint}
	for _, o := range options {
		option.Option(o)(&r)
	}
	return s.do(ctx, r)
}

func (s *Service) Post(ctx context.Context, endpoint string, options ...post.Option) Response {
	r := request.Request{Method: http.MethodPost, Url: endpoint}
	for _, o := range options {
		option.Option(o)(&r)
	}
	return s.do(ctx, r)
}

func (s *Service) Put(ctx context.Context, endpoint string, options ...put.Option) Response {
	r := request.Request{Method: http.MethodPut, Url: endpoint}
	for _, o := range options {
		option.Option(o)(&r)
	}
	return s.do(ctx, r)
}

func (s *Service) Patch(ctx context.Context, endpoint string, options ...patch.Option) Response {
	r := request.Request{Method: http.MethodPatch, Url: endpoint}
	for _, o := range options {
		option.Option(o)(&r)
	}
	return s.do(ctx, r)
}

func (s *Service) Delete(ctx context.Context, endpoint string, options ...delete.Option) Response {
	r := request.Request{Method: http.MethodDelete, Url: endpoint}
	for _, o := range options {
		option.Option(o)(&r)
	}
	return s.do(ctx, r)
}
