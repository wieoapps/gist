package gist

import (
	"fmt"
	"reflect"
)

var loggerType = reflect.TypeFor[Logger]()

func RegisterServiceType[T any](factory func(server *Server, serviceID string) *T) {
	serviceFactories[reflect.TypeFor[*T]()] = func(server *Server, serviceID string) any {
		return factory(server, serviceID)
	}
}

var serviceFactories = map[reflect.Type]func(server *Server, serviceID string) any{}

func BuildServiceGroup[T any](server *Server) (T, error) {
	var sg T
	v := reflect.ValueOf(&sg).Elem()
	if v.Kind() != reflect.Struct {
		return sg, fmt.Errorf("gistsdk: BuildServiceGroup requires a struct type, got %s", v.Kind())
	}

	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		if field.Type == loggerType {
			v.Field(i).Set(reflect.ValueOf(server.Logger))
			continue
		}

		id, ok := field.Tag.Lookup("name")
		if !ok {
			continue
		}

		factory, ok := serviceFactories[field.Type]
		if !ok {
			return sg, fmt.Errorf("gistsdk: no registered service type for field %q (%s) tagged name:%q - is its package imported?", field.Name, field.Type, id)
		}
		v.Field(i).Set(reflect.ValueOf(factory(server, id)))
	}

	return sg, nil
}
