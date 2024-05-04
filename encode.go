package dynamo

import (
	"encoding"
	"reflect"
	"strconv"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Marshaler is the interface implemented by objects that can marshal themselves into
// an AttributeValue.
type Marshaler interface {
	MarshalDynamo() (*dynamodb.AttributeValue, error)
}

// ItemMarshaler is the interface implemented by objects that can marshal themselves
// into an Item (a map of strings to AttributeValues).
type ItemMarshaler interface {
	MarshalDynamoItem() (map[string]*dynamodb.AttributeValue, error)
}

// MarshalItem converts the given struct into a DynamoDB item.
func MarshalItem(v interface{}) (map[string]*dynamodb.AttributeValue, error) {
	return marshalItem(v)
}

func marshalItem(v interface{}) (map[string]*dynamodb.AttributeValue, error) {
	rv := reflect.ValueOf(v)
	rt := rv.Type()
	plan, err := typedefOf(rt)
	if err != nil {
		return nil, err
	}

	return plan.encodeItem(rv)
}

// Marshal converts the given value into a DynamoDB attribute value.
func Marshal(v interface{}) (*dynamodb.AttributeValue, error) {
	return marshal(v, flagNone)
}

func marshal(v interface{}, flags encodeFlags) (*dynamodb.AttributeValue, error) {
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return nil, nil
	}

	rt := rv.Type()
	def, err := typedefOf(rt)
	if err != nil {
		return nil, err
	}
	enc, err := def.encodeType(rt, flags)
	if err != nil {
		return nil, err
	}

	rv = indirectPtrNoAlloc(rv)
	if !rv.IsValid() {
		return nil, nil
	}
	return enc(rv, flags)
}

func marshalSliceNoOmit(values []interface{}) ([]*dynamodb.AttributeValue, error) {
	avs := make([]*dynamodb.AttributeValue, 0, len(values))
	for _, v := range values {
		av, err := marshal(v, flagAllowEmpty)
		if err != nil {
			return nil, err
		}
		avs = append(avs, av)
	}
	return avs, nil
}

func encodeItem(fields []structField, rv reflect.Value) (Item, error) {
	item := make(Item, len(fields))
	for _, field := range fields {
		fv := dig(rv, field.index)
		if !fv.IsValid() {
			// TODO: encode NULL?
			continue
		}

		if field.flags&flagOmitEmpty != 0 && field.isZero != nil {
			if field.isZero(fv) {
				continue
			}
		}

		av, err := field.enc(fv, field.flags)
		if err != nil {
			return nil, err
		}
		if av == nil {
			if field.flags&flagNull != 0 {
				item[field.name] = nullAV
			}
			continue
		}
		item[field.name] = av
	}
	return item, nil
}

type isZeroer interface {
	IsZero() bool
}

func (def *typedef) isZeroFunc(rt reflect.Type) func(rv reflect.Value) bool {
	if rt.Implements(rtypeIsZeroer) {
		return isZeroIface(rt, func(v isZeroer) bool {
			return v.IsZero()
		})
	}

	// simplified check for certain interfaces
	// their output will be checked during encoding process
	switch {
	case rt.Implements(rtypeMarshaler):
		return isZeroIface(rt, func(v Marshaler) bool {
			return false
		})
	case rt.Implements(rtypeTextMarshaler):
		return isZeroIface(rt, func(v encoding.TextMarshaler) bool {
			return false
		})
	}

	switch rt.Kind() {
	case reflect.Map, reflect.Slice:
		return isNil

	case reflect.Array:
		return def.isZeroArray(rt)

	case reflect.Struct:
		return def.isZeroStruct(rt)
	}

	return isZeroValue
}

func isZeroIface[T any](rt reflect.Type, isZero func(v T) bool) func(rv reflect.Value) bool {
	ifaceType := reflect.TypeOf((*T)(nil)).Elem()
	// use IsZero for supported types
	if (rt.Kind() == reflect.Pointer && rt.Elem().Implements(ifaceType)) || rt.Kind() == reflect.Interface {
		// avoid calling IsZero if it would panic
		return func(rv reflect.Value) bool {
			if rv.IsNil() || !rv.CanInterface() {
				return true
			}
			return isZero(rv.Interface().(T))
		}
	}
	return func(rv reflect.Value) bool {
		if !rv.CanInterface() {
			return true
		}
		return isZero(rv.Interface().(T))
	}
}

func (def *typedef) isZeroStruct(rt reflect.Type) func(rv reflect.Value) bool {
	fields, err := def.structFields(rt, false)
	if err != nil {
		return nil
	}
	return func(rv reflect.Value) bool {
		for _, info := range *fields {
			if info.isZero == nil {
				continue
			}

			field := dig(rv, info.index)
			if !field.IsValid() {
				return true
			}

			if !info.isZero(field) {
				return false
			}
		}
		return true
	}
}

func (def *typedef) isZeroArray(rt reflect.Type) func(reflect.Value) bool {
	elemIsZero := def.isZeroFunc(rt.Elem())
	return func(rv reflect.Value) bool {
		for i := 0; i < rv.Len(); i++ {
			if !elemIsZero(rv.Index(i)) {
				return false
			}
		}
		return true
	}
}

func isZeroValue(rv reflect.Value) bool {
	return rv.IsZero()
}

func isNil(rv reflect.Value) bool {
	return rv.IsNil()
}

func formatFloat(f float64, _ int) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}
