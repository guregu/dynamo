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
	// TODO
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return nil, nil
	}

	rt := rv.Type()
	enc, err := encodeType(rt, flags)
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

func encodeItem(fields []fieldMeta, rv reflect.Value) (Item, error) {
	item := make(Item, len(fields))
	for _, field := range fields {
		fv := dig(rv, field.index)
		if !fv.IsValid() {
			// TODO: encode NULL?
			continue
		}

		if field.flags&flagOmitEmpty != 0 && isZero(fv) {
			continue
		}

		av, err := field.enc(fv, field.flags)
		if err != nil {
			return nil, err
		}
		if av == nil {
			if field.flags&flagNull != 0 {
				null := true
				item[field.name] = &dynamodb.AttributeValue{NULL: &null}
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

// thanks James Henstridge
func isZero(rv reflect.Value) bool {
	// use IsZero for supported types
	if rv.CanInterface() {
		if zeroer, ok := rv.Interface().(isZeroer); ok {
			if rv.Kind() == reflect.Ptr && rv.IsNil() {
				if _, cantCall := rv.Type().Elem().MethodByName("IsZero"); cantCall {
					// can't call a value method on a nil pointer type
					return true
				}
			}
			return zeroer.IsZero()
		}
	}

	// always return false for certain interfaces, check these later
	iface := rv.Interface()
	switch iface.(type) {
	case Marshaler:
		return false
	case encoding.TextMarshaler:
		return false
	}

	switch rv.Kind() {
	case reflect.Func, reflect.Map, reflect.Slice:
		return rv.IsNil()
	case reflect.Array:
		z := true
		for i := 0; i < rv.Len(); i++ {
			z = z && isZero(rv.Index(i))
		}
		return z
	case reflect.Struct:
		z := true
		for i := 0; i < rv.NumField(); i++ {
			z = z && isZero(rv.Field(i))
		}
		return z
	}
	// Compare other types directly:
	return rv.IsZero()
}

func formatFloat(f float64, _ int) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}
