package dynamo

import (
	"encoding"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// special attribute encoders
var (
	rtypeAttr            = reflect.TypeOf((*dynamodb.AttributeValue)(nil))
	rtypeTimePtr         = reflect.TypeOf((*time.Time)(nil))
	rtypeTime            = reflect.TypeOf(time.Time{})
	rtypeUnmarshaler     = reflect.TypeOf((*Unmarshaler)(nil)).Elem()
	rtypeAWSUnmarshaler  = reflect.TypeOf((*dynamodbattribute.Unmarshaler)(nil)).Elem()
	rtypeTextUnmarshaler = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
)

// special item encoders
var (
	rtypeItem            = reflect.TypeOf((**map[string]*dynamodb.AttributeValue)(nil)).Elem()
	rtypeItemUnmarshaler = reflect.TypeOf((*ItemUnmarshaler)(nil)).Elem()
	rtypeAWSBypass       = reflect.TypeOf(awsEncoder{})
)

func indirect(rv reflect.Value) reflect.Value {
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			if !rv.CanSet() {
				return rv
			}
			rv.Set(reflect.New(rv.Type().Elem()))
		}
		rv = rv.Elem()
	}
	return rv
}

func indirectPtr(rv reflect.Value) reflect.Value {
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			if !rv.CanSet() {
				return rv
			}
			rv.Set(reflect.New(rv.Type().Elem()))
		}
		if rv.Type().Elem().Kind() != reflect.Pointer {
			return rv
		}
		rv = rv.Elem()
	}
	return rv
}

func visitFields(item map[string]*dynamodb.AttributeValue, rv reflect.Value, seen map[string]struct{}, fn func(av *dynamodb.AttributeValue, flags encodeFlags, v reflect.Value) error) error {
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			if !rv.CanSet() {
				return nil
			}
			rv.Set(reflect.New(rv.Type().Elem()))
		}
		rv = rv.Elem()
	}

	if rv.Kind() != reflect.Struct {
		panic("not a struct")
	}

	if seen == nil {
		seen = make(map[string]struct{})
	}

	// fields := make(map[string]reflect.Value)
	for i := 0; i < rv.Type().NumField(); i++ {
		field := rv.Type().Field(i)
		fv := rv.Field(i)
		isPtr := fv.Type().Kind() == reflect.Ptr

		name, flags := fieldInfo(field)
		if name == "-" {
			// skip
			continue
		}

		if _, ok := seen[name]; ok {
			continue
		}

		// embed anonymous structs, they could be pointers so test that too
		if (fv.Type().Kind() == reflect.Struct || isPtr && fv.Type().Elem().Kind() == reflect.Struct) && field.Anonymous {
			if isPtr {
				fv = indirect(fv)
			}

			if !fv.IsValid() {
				// inaccessible
				continue
			}

			if err := visitFields(item, fv, seen, fn); err != nil {
				return err
			}
			continue
		}

		if !field.IsExported() {
			continue
		}

		if seen != nil {
			seen[name] = struct{}{}
		}
		av := item[name] // might be nil
		// debugf("visit: %s --> %s[%s](%v, %v, %v)", name, runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name(), field.Type, av, flags, fv)
		if err := fn(av, flags, fv); err != nil {
			return err
		}
	}
	return nil
}

func visitTypeFields(rt reflect.Type, seen map[string]struct{}, fn func(flags encodeFlags, vt reflect.Type) error) error {
	for rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	if rt.Kind() != reflect.Struct {
		panic("not a struct")
	}

	if seen == nil {
		seen = make(map[string]struct{})
	}

	// fields := make(map[string]reflect.Value)
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		ft := field.Type
		isPtr := ft.Kind() == reflect.Ptr

		name, flags := fieldInfo(field)
		if name == "-" {
			// skip
			continue
		}

		if _, ok := seen[name]; ok {
			continue
		}

		// embed anonymous structs, they could be pointers so test that too
		if (ft.Kind() == reflect.Struct || isPtr && ft.Elem().Kind() == reflect.Struct) && field.Anonymous {
			for ft.Kind() == reflect.Ptr {
				ft = ft.Elem()
			}

			if err := visitTypeFields(ft, seen, fn); err != nil {
				return err
			}
			continue
		}

		if !field.IsExported() {
			continue
		}

		if seen != nil {
			seen[name] = struct{}{}
		}

		if err := fn(flags, ft); err != nil {
			return err
		}
	}
	return nil
}

func reallocSlice(v reflect.Value, size int) {
	if v.IsNil() || v.Cap() < size {
		slicev := reflect.MakeSlice(v.Type(), size, size)
		v.Set(slicev)
		return
	}
	v.Set(v.Slice(0, size))
}

func reallocMap(v reflect.Value, size int) {
	if v.IsNil() || v.Len() > 0 {
		v.Set(reflect.MakeMapWithSize(v.Type(), size))
	}
}

func nullish(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		return v.IsNil()
	case reflect.Slice:
		return v.IsNil() || v.Len() == 0
	case reflect.Map:
		return v.IsNil() || v.Len() == 0
	}
	return false
}
