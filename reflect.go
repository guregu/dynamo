package dynamo

import (
	"encoding"
	"fmt"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// special attribute encoders
var (
	// *dynamodb.AttributeValue
	rtypeAttr = reflect.TypeOf((*dynamodb.AttributeValue)(nil))
	// *time.Time
	rtypeTimePtr = reflect.TypeOf((*time.Time)(nil))
	// time.Time
	rtypeTime = reflect.TypeOf(time.Time{})

	// Unmarshaler
	rtypeUnmarshaler = reflect.TypeOf((*Unmarshaler)(nil)).Elem()
	// dynamodbattribute.Unmarshaler
	rtypeAWSUnmarshaler = reflect.TypeOf((*dynamodbattribute.Unmarshaler)(nil)).Elem()
	// encoding.TextUnmarshaler
	rtypeTextUnmarshaler = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()

	// Marshaler
	rtypeMarshaler = reflect.TypeOf((*Marshaler)(nil)).Elem()
	// dynamodbattribute.Marshaler
	rtypeAWSMarshaler = reflect.TypeOf((*dynamodbattribute.Marshaler)(nil)).Elem()
	// encoding.TextMarshaler
	rtypeTextMarshaler = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()

	// interface{ IsZero() bool } (time.Time, etc.)
	rtypeIsZeroer = reflect.TypeOf((*isZeroer)(nil)).Elem()
	// struct{}
	rtypeEmptyStruct = reflect.TypeOf(struct{}{})
)

// special item encoders
var (
	rtypeItemPtr         = reflect.TypeOf((*map[string]*dynamodb.AttributeValue)(nil))
	rtypeItem            = rtypeItemPtr.Elem()
	rtypeItemUnmarshaler = reflect.TypeOf((*ItemUnmarshaler)(nil)).Elem()
	rtypeItemMarshaler   = reflect.TypeOf((*ItemMarshaler)(nil)).Elem()
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

func indirectNoAlloc(rv reflect.Value) reflect.Value {
	if !rv.IsValid() {
		return rv
	}
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return reflect.Value{}
		}
		rv = rv.Elem()
	}
	return rv
}

func indirectPtrNoAlloc(rv reflect.Value) reflect.Value {
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return rv
		}
		if rv.Type().Elem().Kind() != reflect.Pointer {
			return rv
		}
		rv = rv.Elem()
	}
	return rv
}

func dig(rv reflect.Value, index []int) reflect.Value {
	rv = indirectNoAlloc(rv)
	for i, idx := range index {
		if i == len(index)-1 {
			rv = indirectPtrNoAlloc(rv.Field(idx))
		} else {
			rv = indirectNoAlloc(rv.Field(idx))
		}
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

func visitTypeFields(rt reflect.Type, seen map[string]struct{}, trail []int, fn func(name string, index []int, flags encodeFlags, vt reflect.Type) error) error {
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
			index := field.Index
			if len(trail) > 0 {
				index = append(trail, field.Index...)
			}
			if err := visitTypeFields(ft, seen, index, fn); err != nil {
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
		index := field.Index
		if len(trail) > 0 {
			index = append(trail, field.Index...)
		}
		if err := fn(name, index, flags, ft); err != nil {
			return err
		}
	}
	return nil
}

func reallocSlice(v reflect.Value, size int) {
	v.Set(reflect.MakeSlice(v.Type(), size, size))
}

func reallocMap(v reflect.Value, size int) {
	v.Set(reflect.MakeMapWithSize(v.Type(), size))
}

type decodeKeyFunc func(reflect.Value, string) error

func decodeMapKeyFunc(rt reflect.Type) decodeKeyFunc {
	if reflect.PtrTo(rt.Key()).Implements(rtypeTextUnmarshaler) {
		return func(kv reflect.Value, s string) error {
			tm := kv.Interface().(encoding.TextUnmarshaler)
			if err := tm.UnmarshalText([]byte(s)); err != nil {
				return fmt.Errorf("dynamo: unmarshal map: key error: %w", err)
			}
			return nil
		}
	}
	return func(kv reflect.Value, s string) error {
		kv.Elem().SetString(s)
		return nil
	}
}

type encodeKeyFunc func(k reflect.Value) (string, error)

func encodeMapKeyFunc(rt reflect.Type) encodeKeyFunc {
	keyt := rt.Key()
	if keyt.Implements(rtypeTextMarshaler) {
		return func(rv reflect.Value) (string, error) {
			tm := rv.Interface().(encoding.TextMarshaler)
			txt, err := tm.MarshalText()
			if err != nil {
				return "", fmt.Errorf("dynamo: marshal map: key error: %v", err)
			}
			return string(txt), nil
		}
	}
	if keyt.Kind() == reflect.String {
		return func(rv reflect.Value) (string, error) {
			return rv.String(), nil
		}
	}
	return nil
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

func emptylike(rt reflect.Type) bool {
	if rt == rtypeEmptyStruct {
		return true
	}
	return rt.Kind() == reflect.Struct && rt.NumField() == 0
}

func truthy(rt reflect.Type) reflect.Value {
	elemt := rt.Elem()
	switch {
	case elemt.Kind() == reflect.Bool:
		return reflect.ValueOf(true).Convert(elemt)
	case emptylike(elemt):
		return reflect.ValueOf(struct{}{}).Convert(elemt)
	}
	return reflect.Value{}
}

// func deref(rv reflect.Value, depth int) reflect.Value {
// 	switch {
// 	case depth < 0:
// 		for i := 0; i >= depth; i-- {
// 			if !rv.CanAddr() {
// 				return rv
// 			}
// 			rv = rv.Addr()
// 		}
// 		return rv
// 	case depth > 0:
// 		for i := 0; i < depth; i++ {
// 			if !rv.IsValid() || rv.IsNil() {
// 				return rv
// 			}
// 			rv = rv.Elem()
// 		}
// 	}
// 	return rv
// }
