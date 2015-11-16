package dynamo

import (
	"encoding"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Marshaler interface {
	MarshalDynamo() (*dynamodb.AttributeValue, error)
}

func marshalStruct(v interface{}) (map[string]*dynamodb.AttributeValue, error) {
	item := make(map[string]*dynamodb.AttributeValue)
	var err error
	rv := reflect.ValueOf(v)

	if rv.Type().Kind() != reflect.Struct {
		if rv.Type().Kind() == reflect.Ptr {
			return marshalStruct(rv.Elem().Interface())
		}
		return nil, fmt.Errorf("marshal struct invalid type: %T (%+v)", v, v)
	}

	for i := 0; i < rv.Type().NumField(); i++ {
		field := rv.Type().Field(i)
		fv := rv.Field(i)
		kind := fv.Kind()

		name, special, omitempty := fieldInfo(field)
		_ = special // TODO: use this for lists / sets
		switch {
		case !fv.CanInterface():
			continue
		case name == "-":
			continue
		case omitempty:
			if isZero(rv) {
				continue
			}
		case kind == reflect.String,
			kind == reflect.Ptr,
			kind == reflect.Map,
			kind == reflect.Slice,
			kind == reflect.Interface:
			// automatically omit these types if nil
			// DynamoDB can't handle empty stuff in general
			// and it's better than sending "NULL: true"
			if isZero(fv) {
				continue
			}
		}

		// embed anonymous structs
		if fv.Type().Kind() == reflect.Struct && field.Anonymous {
			avs, err := marshalStruct(fv.Interface())
			if err != nil {
				return nil, err
			}
			for k, v := range avs {
				item[k] = v
			}
			continue
		}

		av, err := marshal(fv.Interface())
		if err != nil {
			return nil, err
		}
		if av != nil {
			item[name] = av
		}
	}
	return item, err
}

func marshal(v interface{}) (*dynamodb.AttributeValue, error) {
	switch x := v.(type) {
	case Marshaler:
		return x.MarshalDynamo()
	case encoding.TextMarshaler:
		text, err := x.MarshalText()
		if err != nil {
			return nil, err
		}
		if len(text) == 0 {
			return nil, nil
		}
		return &dynamodb.AttributeValue{S: aws.String(string(text))}, err

	case []byte:
		return &dynamodb.AttributeValue{B: x}, nil
	case [][]byte:
		return &dynamodb.AttributeValue{BS: x}, nil

	case bool:
		return &dynamodb.AttributeValue{BOOL: aws.Bool(x)}, nil

	case int:
		return &dynamodb.AttributeValue{N: aws.String(strconv.Itoa(x))}, nil
	case int64:
		return &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(x, 10))}, nil
	case int32:
		return &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(int64(x), 10))}, nil
	case int16:
		return &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(int64(x), 10))}, nil
	case int8:
		return &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(int64(x), 10))}, nil
	case byte:
		return &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(int64(x), 10))}, nil
	case float64:
		return &dynamodb.AttributeValue{N: aws.String(strconv.FormatFloat(x, 'f', -1, 64))}, nil
	case float32:
		return &dynamodb.AttributeValue{N: aws.String(strconv.FormatFloat(float64(x), 'f', -1, 32))}, nil

	case nil:
		return &dynamodb.AttributeValue{NULL: aws.Bool(true)}, nil

	case string:
		return &dynamodb.AttributeValue{S: aws.String(x)}, nil
	case *string:
		return &dynamodb.AttributeValue{S: x}, nil
	case []string:
		if len(x) == 0 {
			return &dynamodb.AttributeValue{NULL: aws.Bool(true)}, nil
		}

		// why are these pointers amazon seriously
		strptrs := make([]*string, 0, len(x))
		for _, s := range x {
			s := s
			strptrs = append(strptrs, &s)
		}
		return &dynamodb.AttributeValue{SS: strptrs}, nil
	case []*string:
		if len(x) == 0 {
			return &dynamodb.AttributeValue{NULL: aws.Bool(true)}, nil
		}

		return &dynamodb.AttributeValue{SS: x}, nil
	default:
		return marshalReflect(reflect.ValueOf(x))
	}
}

func marshalReflect(rv reflect.Value) (*dynamodb.AttributeValue, error) {
	// TODO: byte arrays and array of arrays
	// TODO: other kinds of arrays
	// TODO: structs
	// TODO: maps
	switch rv.Kind() {
	case reflect.Ptr:
		if rv.IsNil() {
			return &dynamodb.AttributeValue{NULL: aws.Bool(true)}, nil
		} else {
			return marshal(rv.Elem().Interface())
		}
	case reflect.Bool:
		return &dynamodb.AttributeValue{BOOL: aws.Bool(rv.Bool())}, nil
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		return &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(rv.Int(), 10))}, nil
	case reflect.String:
		return &dynamodb.AttributeValue{S: aws.String(rv.String())}, nil
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			return nil, fmt.Errorf("dynamo marshal: map key must be string: %T", rv.Interface())
		}
		avs := make(map[string]*dynamodb.AttributeValue)
		for _, key := range rv.MapKeys() {
			v, err := marshal(rv.MapIndex(key).Interface())
			if err != nil {
				return nil, err
			}
			if v != nil {
				avs[key.String()] = v
			}
		}
		return &dynamodb.AttributeValue{M: avs}, nil
	case reflect.Struct:
		avs, err := marshalStruct(rv.Interface())
		if err != nil {
			return nil, err
		}
		return &dynamodb.AttributeValue{M: avs}, nil
	default:
		return nil, fmt.Errorf("dynamo marshal: unknown type %s", rv.Type().String())
	}
}

func marshalSlice(values []interface{}) ([]*dynamodb.AttributeValue, error) {
	avs := make([]*dynamodb.AttributeValue, 0, len(values))
	for _, v := range values {
		av, err := marshal(v)
		if err != nil {
			return nil, err
		}
		if av != nil {
			avs = append(avs, av)
		}
	}
	return avs, nil
}

func fieldInfo(field reflect.StructField) (name, special string, omitempty bool) {
	tags := strings.Split(field.Tag.Get("dynamo"), ",")
	if len(tags) == 0 {
		return field.Name, "", false
	}

	name = tags[0]
	if name == "" {
		name = field.Name
	}

	for _, t := range tags[1:] {
		if t == "omitempty" {
			omitempty = true
		} else {
			special = t
		}
	}

	return
}

type isZeroer interface {
	IsZero() bool
}

// thanks James Henstridge
// TODO: tweak
func isZero(rv reflect.Value) bool {
	// use IsZero for supported types
	if rv.CanInterface() {
		if zeroer, ok := rv.Interface().(isZeroer); ok {
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
	z := reflect.Zero(rv.Type())
	return rv.Interface() == z.Interface()
}
