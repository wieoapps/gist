package gistapiserver

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	"github.com/wieoapps/gist"
	"github.com/wieoapps/gist-proto"
	"github.com/wieoapps/gist/internal/rpcconn"
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
	es := &gistproto.EndpointSchema{
		Id:             id,
		Fields:         reflectFields(reflect.TypeOf(*new(in))),
		OutputFields:   reflectFields(reflect.TypeOf(*new(out))),
		ExpectedErrors: make([]*gistproto.ExpectedError, len(expectedErrors)),
	}
	declaredErrors := make(map[int32]string, len(expectedErrors))
	for i, e := range expectedErrors {
		es.ExpectedErrors[i] = &gistproto.ExpectedError{Code: int32(e.Code), Description: e.Message}
		declaredErrors[int32(e.Code)] = e.Message
	}

	if mockData != nil {
		mockJSON, err := json.Marshal(mockData)
		if err != nil {
			return fmt.Errorf("gistapi: could not encode mock data: %w", err)
		}
		es.Mock = mockJSON
	}

	if _, err := rpcconn.MustFor(server).Admin.Register(context.Background(), &gistproto.RegisterRequest{ServiceId: serviceID, Schema: es}); err != nil {
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

func reflectFields(t reflect.Type) []*gistproto.Field {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}

	var fields []*gistproto.Field
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

		wf := &gistproto.Field{
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
