package gistmysqlclient

import (
	"reflect"
	"strings"

	"github.com/wieoapps/gist-proto"
)

const (
	tagSource = "source"
	tagJSON   = "json"
	tagColumn = "db"
	tagJoin   = "join"
)

func buildModelSchema(t reflect.Type) *gistproto.ModelSchema {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	schema := &gistproto.ModelSchema{}
	parseFields(t, schema)
	return schema
}

func parseFields(t reflect.Type, schema *gistproto.ModelSchema) {
	for sf := range t.Fields() {

		if source, ok := sf.Tag.Lookup(tagSource); ok {
			schema.Sources = strings.Split(source, ".")
		}

		if !sf.IsExported() {
			continue
		}

		ft := sf.Type
		for ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}

		if ft.Kind() == reflect.Struct && sf.Anonymous {
			parseFields(ft, schema)
			continue
		}

		joinTag, hasJoin := sf.Tag.Lookup(tagJoin)

		if hasJoin && ft.Kind() == reflect.Struct {
			schema.Fields = append(schema.Fields, structuralField(sf, ft, joinTag, false))
			continue
		}

		if ft.Kind() == reflect.Slice && ft.Elem().Kind() != reflect.Uint8 {
			elemType := ft.Elem()
			for elemType.Kind() == reflect.Pointer {
				elemType = elemType.Elem()
			}
			if elemType.Kind() == reflect.Struct {
				schema.Fields = append(schema.Fields, structuralField(sf, elemType, joinTag, true))
				continue
			}
		}

		col, ok := sf.Tag.Lookup(tagColumn)
		if !ok {
			continue
		}
		schema.Fields = append(schema.Fields, &gistproto.ModelField{
			Name:     col,
			JsonName: jsonName(sf),
			// The MySQL driver returns TINYINT/BIT columns as a numeric Go
			// type regardless of whether the column is conventionally used
			// as a boolean - telling the server this field's real Go type
			// is bool lets it coerce that number into a real JSON boolean
			// before sending the row back, instead of leaving decode to
			// fail client-side against a bool/*bool field.
			IsBool: ft.Kind() == reflect.Bool,
		})
	}
}

func structuralField(sf reflect.StructField, nestedType reflect.Type, joinTag string, isSlice bool) *gistproto.ModelField {
	nested := &gistproto.ModelSchema{}
	parseFields(nestedType, nested)

	return &gistproto.ModelField{
		Name:     sf.Tag.Get(tagColumn),
		JsonName: jsonName(sf),
		JoinTag:  joinTag,
		IsSlice:  isSlice,
		IsStruct: true,
		Nested:   nested,
	}
}

func jsonName(sf reflect.StructField) string {
	tag, ok := sf.Tag.Lookup(tagJSON)
	if !ok {
		return ""
	}
	name, _, _ := strings.Cut(tag, ",")
	return name
}
