package dynamo

import (
	"encoding"
	"fmt"
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
	plan, err := getDecodePlan(rt)
	if err != nil {
		return nil, err
	}

	return plan.encodeItem(rv)
	// switch x := v.(type) {
	// case map[string]*dynamodb.AttributeValue:
	// 	return x, nil
	// case awsEncoder:
	// 	// special case for AWSEncoding
	// 	return dynamodbattribute.MarshalMap(x.iface)
	// case ItemMarshaler:
	// 	return x.MarshalDynamoItem()
	// }

	// rv := reflect.ValueOf(v)

	// switch rv.Type().Kind() {
	// case reflect.Ptr:
	// 	return marshalItem(rv.Elem().Interface())
	// case reflect.Struct:
	// 	return marshalStruct(rv)
	// case reflect.Map:
	// 	return marshalItemMap(rv.Interface())
	// }
	// return nil, fmt.Errorf("dynamo: marshal item: unsupported type %T: %v", rv.Interface(), rv.Interface())
}

func marshalItemMap(v interface{}) (map[string]*dynamodb.AttributeValue, error) {
	// TODO: maybe unify this with the map stuff in marshal
	av, err := marshal(v, flagNone)
	if err != nil {
		return nil, err
	}
	if av.M == nil {
		return nil, fmt.Errorf("dynamo: internal error: encoding map but M was empty")
	}
	return av.M, nil
}

func marshalStruct(rv reflect.Value) (map[string]*dynamodb.AttributeValue, error) {
	item := make(map[string]*dynamodb.AttributeValue)
	var err error

	for i := 0; i < rv.Type().NumField(); i++ {
		field := rv.Type().Field(i)
		fv := rv.Field(i)

		name, flags := fieldInfo(field)
		omitempty := flags&flagOmitEmpty != 0
		anonStruct := fv.Type().Kind() == reflect.Struct && field.Anonymous
		pointerAnonStruct := fv.Type().Kind() == reflect.Ptr && fv.Type().Elem().Kind() == reflect.Struct && field.Anonymous
		switch {
		case !fv.CanInterface():
			// skip unexported unembedded fields
			if !anonStruct && !pointerAnonStruct {
				continue
			}
		case name == "-":
			continue
		case omitempty:
			if isZero(fv) {
				continue
			}
		}

		// embed anonymous structs
		if anonStruct || pointerAnonStruct {
			if pointerAnonStruct {
				if fv.IsNil() {
					continue
				}
				fv = fv.Elem()
			}

			avs, err := marshalStruct(fv)
			if err != nil {
				return nil, err
			}
			for k, v := range avs {
				// don't clobber pre-existing fields
				if _, exists := item[k]; exists {
					continue
				}
				item[k] = v
			}
			continue
		}

		av, err := marshal(fv.Interface(), flags)
		if err != nil {
			return nil, err
		}
		if av != nil {
			item[name] = av
		}
	}
	return item, err
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
	enc, err := encoderFor(rt, flags)
	if err != nil {
		return nil, err
	}
	rv = indirectPtrNoAlloc(rv)
	if !rv.IsValid() {
		return nil, nil
	}
	return enc(rv, flags)
}

var emptyStructType = reflect.TypeOf(struct{}{})

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

type encodeFlags uint

const (
	flagSet encodeFlags = 1 << iota
	flagOmitEmpty
	flagOmitEmptyElem
	flagAllowEmpty
	flagAllowEmptyElem
	flagNull
	flagUnixTime

	flagNone encodeFlags = 0
)

func fieldInfo(field reflect.StructField) (name string, flags encodeFlags) {
	tag := field.Tag.Get("dynamo")
	if tag == "" {
		return field.Name, flagNone
	}

	begin := 0
	for i := 0; i <= len(tag); i++ {
		if !(i == len(tag) || tag[i] == ',') {
			continue
		}
		part := tag[begin:i]
		begin = i + 1

		if name == "" {
			if part == "" {
				name = field.Name
			} else {
				name = part
			}
			continue
		}

		switch part {
		case "set":
			flags |= flagSet
		case "omitempty":
			flags |= flagOmitEmpty
		case "omitemptyelem":
			flags |= flagOmitEmptyElem
		case "allowempty":
			flags |= flagAllowEmpty
		case "allowemptyelem":
			flags |= flagAllowEmptyElem
		case "null":
			flags |= flagNull
		case "unixtime":
			flags |= flagUnixTime
		}
	}

	return
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
