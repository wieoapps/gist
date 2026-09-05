package gistapiserver

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	"github.com/wieoapps/gist"
	"github.com/wieoapps/gist/internal/rpcconn"
	"github.com/wieoapps/gist/proto"
)

type Handler[servicesGroup any] struct {
	id     string
	attach func(server *gist.Server, serviceID string, sg servicesGroup) error
}

func EndpointHandler[servicesGroup any, in any, out any](
	id string,
	expectedErrors []ExpectedError,
	fn func(sg servicesGroup, ctx context.Context, in in, output *out) error,
	mockData *out,
) *Handler[servicesGroup] {
	return &Handler[servicesGroup]{
		id: id,
		attach: func(server *gist.Server, serviceID string, sg servicesGroup) error {
			return registerEndpoint(server, serviceID, sg, id, expectedErrors, fn, mockData)
		},
	}
}

// RawEndpointHandler is EndpointHandler's counterpart for a response that
// isn't a JSON-encoded struct at all - an SVG diagram, a PDF, a plain
// image - anything whose bytes need to reach the caller exactly as the
// handler produced them, under a Content-Type other than
// "application/json". A separate function rather than a parameter on
// EndpointHandler: that one's out is always JSON-marshaled by the
// dispatch closure itself (see registerEndpoint), and generic type
// inference has no way to make that conditional on a runtime contentType
// argument - fn here returns the exact bytes to send instead of
// populating a typed output struct, and there is no out type parameter
// to infer in the first place.
func RawEndpointHandler[servicesGroup any, in any](
	id string,
	expectedErrors []ExpectedError,
	contentType string,
	fn func(sg servicesGroup, ctx context.Context, in in) ([]byte, error),
) *Handler[servicesGroup] {
	return &Handler[servicesGroup]{
		id: id,
		attach: func(server *gist.Server, serviceID string, sg servicesGroup) error {
			return registerRawEndpoint(server, serviceID, sg, id, expectedErrors, contentType, fn)
		},
	}
}

func Attach[servicesGroup any](serviceID string, handlers ...*Handler[servicesGroup]) gist.Option {
	return func(server *gist.Server) error {
		sg, err := gist.BuildServiceGroup[servicesGroup](server)
		if err != nil {
			return err
		}
		for _, h := range handlers {
			if err := h.attach(server, serviceID, sg); err != nil {
				return err
			}
		}
		return nil
	}
}

func registerEndpoint[servicesGroup any, in any, out any](
	server *gist.Server,
	serviceID string,
	sg servicesGroup,
	id string,
	expectedErrors []ExpectedError,
	fn func(sg servicesGroup, ctx context.Context, in in, output *out) error,
	mockData *out,
) error {
	es := &proto.EndpointSchema{
		Id:             id,
		Fields:         reflectFields(reflect.TypeOf(*new(in))),
		OutputFields:   reflectFields(reflect.TypeOf(*new(out))),
		ExpectedErrors: make([]*proto.ExpectedError, len(expectedErrors)),
	}
	declaredErrors := make(map[int32]string, len(expectedErrors))
	for i, e := range expectedErrors {
		es.ExpectedErrors[i] = &proto.ExpectedError{Code: int32(e.Code), Description: e.Message}
		declaredErrors[int32(e.Code)] = e.Message
	}

	if mockData != nil {
		mockJSON, err := json.Marshal(mockData)
		if err != nil {
			return fmt.Errorf("gistapi: could not encode mock data: %w", err)
		}
		es.Mock = mockJSON
	}

	if _, err := rpcconn.MustFor(server).Admin.Register(context.Background(), &proto.RegisterRequest{ServiceId: serviceID, Schema: es}); err != nil {
		return fmt.Errorf("gistapi: could not register endpoint %q on service %q: %w", id, serviceID, err)
	}

	server.RegisterDispatch(id, func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		var typedIn in
		if len(input) > 0 {
			if err := json.Unmarshal(input, &typedIn); err != nil {
				return nil, fmt.Errorf("gistapi: could not decode callback input: %w", err)
			}
		}

		var output out
		if err := fn(sg, ctx, typedIn, &output); err != nil {
			return nil, err
		}

		return json.Marshal(output)
	}, declaredErrors)

	return nil
}

// registerRawEndpoint mirrors registerEndpoint - same schema
// registration, same callback dispatch - except there is no out type to
// reflect OutputFields from (a raw response has no fields at all, JSON
// or otherwise) and fn's own return value is written to the wire
// exactly as given, never passed through json.Marshal.
func registerRawEndpoint[servicesGroup any, in any](
	server *gist.Server,
	serviceID string,
	sg servicesGroup,
	id string,
	expectedErrors []ExpectedError,
	contentType string,
	fn func(sg servicesGroup, ctx context.Context, in in) ([]byte, error),
) error {
	es := &proto.EndpointSchema{
		Id:             id,
		Fields:         reflectFields(reflect.TypeOf(*new(in))),
		ContentType:    contentType,
		ExpectedErrors: make([]*proto.ExpectedError, len(expectedErrors)),
	}
	declaredErrors := make(map[int32]string, len(expectedErrors))
	for i, e := range expectedErrors {
		es.ExpectedErrors[i] = &proto.ExpectedError{Code: int32(e.Code), Description: e.Message}
		declaredErrors[int32(e.Code)] = e.Message
	}

	if _, err := rpcconn.MustFor(server).Admin.Register(context.Background(), &proto.RegisterRequest{ServiceId: serviceID, Schema: es}); err != nil {
		return fmt.Errorf("gistapi: could not register endpoint %q on service %q: %w", id, serviceID, err)
	}

	server.RegisterDispatch(id, func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		var typedIn in
		if len(input) > 0 {
			if err := json.Unmarshal(input, &typedIn); err != nil {
				return nil, fmt.Errorf("gistapi: could not decode callback input: %w", err)
			}
		}

		raw, err := fn(sg, ctx, typedIn)
		if err != nil {
			return nil, err
		}
		return raw, nil
	}, declaredErrors)

	return nil
}

func reflectFields(t reflect.Type) []*proto.Field {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}

	var fields []*proto.Field
	for f := range t.Fields() {
		if !f.IsExported() {
			continue
		}

		ft := f.Type
		slice := false
		if ft.Kind() == reflect.Slice {
			slice = true
			ft = ft.Elem()
		}
		for ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}

		wf := &proto.Field{
			Kind:    kindOf(ft),
			Slice:   slice,
			Example: f.Tag.Get("example"),
			Pattern: f.Tag.Get("pattern"),
		}
		if v, ok := f.Tag.Lookup("minLength"); ok {
			if n, err := strconv.Atoi(v); err == nil {
				n32 := int32(n)
				wf.MinLength = &n32
			}
		}
		if v, ok := f.Tag.Lookup("maxLength"); ok {
			if n, err := strconv.Atoi(v); err == nil {
				n32 := int32(n)
				wf.MaxLength = &n32
			}
		}
		if v, ok := f.Tag.Lookup("minimum"); ok {
			if n, err := strconv.ParseFloat(v, 64); err == nil {
				wf.Minimum = &n
			}
		}
		if v, ok := f.Tag.Lookup("maximum"); ok {
			if n, err := strconv.ParseFloat(v, 64); err == nil {
				wf.Maximum = &n
			}
		}
		if _, ok := f.Tag.Lookup("required"); ok {
			wf.Required = true
		}

		if name, ok := f.Tag.Lookup("path"); ok {
			wf.Name, wf.In = name, "path"
		} else if name, ok := f.Tag.Lookup("query"); ok {
			wf.Name, wf.In = name, "query"
		} else if name, ok := f.Tag.Lookup("json"); ok {
			wf.Name, wf.In = name, "json"
		} else {
			continue
		}

		fields = append(fields, wf)
	}
	return fields
}

func kindOf(t reflect.Type) string {
	switch t.Kind() {
	case reflect.String:
		return "string"
	case reflect.Bool:
		return "bool"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return "int"
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "uint"
	case reflect.Float32, reflect.Float64:
		return "float"
	default:
		return "any"
	}
}
