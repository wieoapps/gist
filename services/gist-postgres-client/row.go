package gistpostgresclient

import "reflect"

func buildModelRow(v reflect.Value) map[string]any {
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return map[string]any{}
		}
		v = v.Elem()
	}
	row := map[string]any{}
	collectRow(v, row)
	return row
}

func collectRow(v reflect.Value, row map[string]any) {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		fv := v.Field(i)
		if !sf.IsExported() {
			continue
		}

		ft := sf.Type
		for ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}

		if ft.Kind() == reflect.Struct && sf.Anonymous {
			fvv := fv
			for fvv.Kind() == reflect.Pointer {
				if fvv.IsNil() {
					fvv = reflect.Value{}
					break
				}
				fvv = fvv.Elem()
			}
			if fvv.IsValid() {
				collectRow(fvv, row)
			}
			continue
		}

		if _, hasJoin := sf.Tag.Lookup(tagJoin); hasJoin {
			continue
		}
		if ft.Kind() == reflect.Slice && ft.Elem().Kind() != reflect.Uint8 {
			elemType := ft.Elem()
			for elemType.Kind() == reflect.Pointer {
				elemType = elemType.Elem()
			}
			if elemType.Kind() == reflect.Struct {
				continue
			}
		}

		col, ok := sf.Tag.Lookup(tagColumn)
		if !ok {
			continue
		}

		if sf.Type.Kind() == reflect.Pointer {
			if fv.IsNil() {
				continue
			}
			row[col] = fv.Elem().Interface()
			continue
		}
		row[col] = fv.Interface()
	}
}
