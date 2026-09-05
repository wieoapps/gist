package gistapiserver

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"github.com/wieoapps/gist"
	"github.com/wieoapps/gist/internal/rpcconn"
	"github.com/wieoapps/gist/proto"
)

// MiddlewareRequest is what a named middleware gets to inspect about the
// incoming request - method/path/headers/query params only, not the
// body: none of the realistic cases (custom auth, rate limiting,
// request logging, CORS-style checks) need it, and reading it here
// would mean marshaling a full body copy per middleware per request.
// See callback.proto's InvokeMiddlewareRequest for the wire shape this
// mirrors.
type MiddlewareRequest struct {
	EndpointID  string
	Method      string
	Path        string
	Headers     http.Header
	QueryParams url.Values
}

// MiddlewareResponse is a middleware's decision: Blocked = true means
// the request goes no further - StatusCode/Headers/Body are written
// back to the caller verbatim instead of ever reaching the endpoint's
// own handler. Blocked = false (every other field unused) means
// proceed normally.
type MiddlewareResponse struct {
	Blocked    bool
	StatusCode int
	Headers    http.Header
	Body       []byte
}

// Middleware is one named middleware's registration, built by
// MiddlewareHandler and wired up by AttachMiddlewares - the same
// two-step shape EndpointHandler/Attach already use.
type Middleware[servicesGroup any] struct {
	name   string
	attach func(server *gist.Server, serviceID string, sg servicesGroup) error
}

// MiddlewareHandler declares one named middleware - name is whatever
// string an endpoint's (or an instance's own) config.json "middlewares"
// list references. fn runs once per matching request, right after auth
// checks and before the endpoint's own handler; returning a
// MiddlewareResponse with Blocked set to true stops the request there.
func MiddlewareHandler[servicesGroup any](
	name string,
	fn func(sg servicesGroup, ctx context.Context, req *MiddlewareRequest) (*MiddlewareResponse, error),
) *Middleware[servicesGroup] {
	return &Middleware[servicesGroup]{
		name: name,
		attach: func(server *gist.Server, serviceID string, sg servicesGroup) error {
			return registerMiddleware(server, serviceID, sg, name, fn)
		},
	}
}

// AttachMiddlewares wires one or more MiddlewareHandler declarations
// into server, the same way Attach wires EndpointHandler declarations -
// a separate function, not a parameter on Attach, so an existing
// Attach(serviceID, handlers...) call site never has to change shape to
// add a middleware alongside its handlers.
func AttachMiddlewares[servicesGroup any](serviceID string, middlewares ...*Middleware[servicesGroup]) gist.Option {
	return func(server *gist.Server) error {
		sg, err := gist.BuildServiceGroup[servicesGroup](server)
		if err != nil {
			return err
		}
		for _, m := range middlewares {
			if err := m.attach(server, serviceID, sg); err != nil {
				return err
			}
		}
		return nil
	}
}

func registerMiddleware[servicesGroup any](
	server *gist.Server,
	serviceID string,
	sg servicesGroup,
	name string,
	fn func(sg servicesGroup, ctx context.Context, req *MiddlewareRequest) (*MiddlewareResponse, error),
) error {
	if _, err := rpcconn.MustFor(server).Admin.RegisterMiddleware(context.Background(), &proto.RegisterMiddlewareRequest{ServiceId: serviceID, Name: name}); err != nil {
		return fmt.Errorf("gistapi: could not register middleware %q on service %q: %w", name, serviceID, err)
	}

	server.RegisterMiddlewareDispatch(name, func(ctx context.Context, req *proto.InvokeMiddlewareRequest) (*proto.InvokeMiddlewareResponse, error) {
		headers := make(http.Header, len(req.GetHeaders()))
		for k, v := range req.GetHeaders() {
			headers[k] = v.GetValues()
		}
		query := make(url.Values, len(req.GetQueryParams()))
		for k, v := range req.GetQueryParams() {
			query.Set(k, v)
		}

		resp, err := fn(sg, ctx, &MiddlewareRequest{
			EndpointID:  req.GetEndpointId(),
			Method:      req.GetMethod(),
			Path:        req.GetPath(),
			Headers:     headers,
			QueryParams: query,
		})
		if err != nil {
			return nil, err
		}

		var wireHeaders map[string]*proto.HeaderValues
		if len(resp.Headers) > 0 {
			wireHeaders = make(map[string]*proto.HeaderValues, len(resp.Headers))
			for k, v := range resp.Headers {
				wireHeaders[k] = &proto.HeaderValues{Values: v}
			}
		}
		return &proto.InvokeMiddlewareResponse{
			Blocked:    resp.Blocked,
			StatusCode: int32(resp.StatusCode),
			Headers:    wireHeaders,
			Body:       resp.Body,
		}, nil
	})

	return nil
}
