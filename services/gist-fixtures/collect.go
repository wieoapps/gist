package gistfixtures

import (
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"strings"
)

const (
	tagSource = "source"
	tagColumn = "db"
	tagJoin   = "join"
)

type tableRows map[string][]map[string]any

type columnKinds map[string]map[string]string

func collect(out tableRows, kinds columnKinds, v reflect.Value) {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return
		}
		v = v.Elem()
	}

	if v.Kind() == reflect.Slice || v.Kind() == reflect.Array {
		for i := 0; i < v.Len(); i++ {
			collect(out, kinds, v.Index(i))
		}
		return
	}

	if v.Kind() != reflect.Struct {
		return
	}

	t := v.Type()
	table := ""
	record := map[string]any{}
	recordKinds := map[string]string{}

	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)

		if source, ok := sf.Tag.Lookup(tagSource); ok {
			table = source[strings.LastIndex(source, ".")+1:]
		}
		if !sf.IsExported() {
			continue
		}

		fv := v.Field(i)
		if _, joined := sf.Tag.Lookup(tagJoin); joined {
			if fv.Kind() == reflect.Slice {
				for j := 0; j < fv.Len(); j++ {
					collect(out, kinds, fv.Index(j))
				}
			}
			continue
		}

		col, ok := sf.Tag.Lookup(tagColumn)
		if !ok {
			continue
		}
		record[col] = fieldValue(fv)
		recordKinds[col] = fieldKind(fv)
	}

	if table == "" {
		panic(fmt.Sprintf("gistfixtures: %s has no `source` tag", t.Name()))
	}
	if len(record) > 0 {
		out[table] = append(out[table], record)
		if kinds[table] == nil {
			kinds[table] = map[string]string{}
		}
		maps.Copy(kinds[table], recordKinds)
	}
}

func fieldKind(v reflect.Value) string {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return "other"
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "int"
	case reflect.Float32, reflect.Float64:
		return "float"
	case reflect.Bool:
		return "bool"
	case reflect.String, reflect.Map:

		return "string"
	default:
		return "other"
	}
}

func fieldValue(v reflect.Value) any {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	if v.Kind() == reflect.Map {
		b, err := json.Marshal(v.Interface())
		if err != nil {
			panic(fmt.Sprintf("gistfixtures: could not marshal field to JSON: %v", err))
		}
		return string(b)
	}
	return v.Interface()
}
