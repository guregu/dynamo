package dynamo

import (
	"encoding"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// Marshaler is the interface implemented by objects that can marshal themselves into
// an AttributeValue.
type Marshaler interface {
	MarshalDynamo() (*dynamodb.AttributeValue, error)
}

// MarshalItem converts the given struct into a DynamoDB item.
func MarshalItem(v interface{}) (map[string]*dynamodb.AttributeValue, error) {
	return marshalItem(v)
}

func marshalItem(v interface{}) (map[string]*dynamodb.AttributeValue, error) {
	rv := reflect.ValueOf(v)
	switch rv.Type().Kind() {
	case reflect.Ptr:
		return marshalItem(rv.Elem().Interface())
	case reflect.Struct:
		return marshalStruct(rv)
	case reflect.Map:
		return marshalMap(rv.Interface())
	}
	return nil, fmt.Errorf("dynamo: marshal item: unsupported type %T: %v", rv.Interface(), rv.Interface())
}

func marshalMap(v interface{}) (map[string]*dynamodb.AttributeValue, error) {
	// TODO: maybe unify this with the map stuff in marshal
	av, err := marshal(v, "")
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

		name, special, omitempty := fieldInfo(field)
		anonStruct := fv.Type().Kind() == reflect.Struct && field.Anonymous
		switch {
		case !fv.CanInterface():
			if !anonStruct {
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
		if anonStruct {
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

		av, err := marshal(fv.Interface(), special)
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
	return marshal(v, "")
}

func marshal(v interface{}, special string) (*dynamodb.AttributeValue, error) {
	switch x := v.(type) {
	case *dynamodb.AttributeValue:
		return x, nil
	case Marshaler:
		return x.MarshalDynamo()
	case dynamodbattribute.Marshaler:
		av := &dynamodb.AttributeValue{}
		return av, x.MarshalDynamoDBAttributeValue(av)
	case encoding.TextMarshaler:
		text, err := x.MarshalText()
		if err != nil {
			return nil, err
		}
		if len(text) == 0 {
			return nil, nil
		}
		return &dynamodb.AttributeValue{S: aws.String(string(text))}, err
	case nil:
		return nil, nil
	}
	return marshalReflect(reflect.ValueOf(v), special)
}

var nilTm encoding.TextMarshaler
var tmType = reflect.TypeOf(&nilTm).Elem()

func marshalReflect(rv reflect.Value, special string) (*dynamodb.AttributeValue, error) {
	switch rv.Kind() {
	case reflect.Ptr:
		if rv.IsNil() {
			return nil, nil
		}
		return marshal(rv.Elem().Interface(), special)
	case reflect.Bool:
		return &dynamodb.AttributeValue{BOOL: aws.Bool(rv.Bool())}, nil
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		return &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(rv.Int(), 10))}, nil
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		return &dynamodb.AttributeValue{N: aws.String(strconv.FormatUint(rv.Uint(), 10))}, nil
	case reflect.Float32, reflect.Float64:
		return &dynamodb.AttributeValue{N: aws.String(strconv.FormatFloat(rv.Float(), 'f', -1, 64))}, nil
	case reflect.String:
		s := rv.String()
		if len(s) == 0 {
			return nil, nil
		}
		return &dynamodb.AttributeValue{S: aws.String(s)}, nil
	case reflect.Map:
		if special == "set" {
			// sets can't be empty
			if rv.Len() == 0 {
				return nil, nil
			}
			return marshalSet(rv)
		}

		// automatically omit nil maps
		if rv.IsNil() {
			return nil, nil
		}

		var keyString func(k reflect.Value) (string, error)
		if ktype := rv.Type().Key(); ktype.Implements(tmType) {
			keyString = func(k reflect.Value) (string, error) {
				tm := k.Interface().(encoding.TextMarshaler)
				txt, err := tm.MarshalText()
				if err != nil {
					return "", fmt.Errorf("dynamo: marshal map: key error: %v", err)
				}
				return string(txt), nil
			}
		} else if ktype.Kind() == reflect.String {
			keyString = func(k reflect.Value) (string, error) {
				return k.String(), nil
			}
		} else {
			return nil, fmt.Errorf("dynamo marshal: map key must be string: %T", rv.Interface())
		}

		avs := make(map[string]*dynamodb.AttributeValue)
		for _, key := range rv.MapKeys() {
			v, err := marshal(rv.MapIndex(key).Interface(), "")
			if err != nil {
				return nil, err
			}
			if v != nil {
				kstr, err := keyString(key)
				if err != nil {
					return nil, err
				}
				avs[kstr] = v
			}
		}
		return &dynamodb.AttributeValue{M: avs}, nil
	case reflect.Struct:
		avs, err := marshalStruct(rv)
		if err != nil {
			return nil, err
		}
		return &dynamodb.AttributeValue{M: avs}, nil
	case reflect.Slice, reflect.Array:
		// special case: byte slice is B
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			// binary values can't be empty
			if rv.Len() == 0 {
				return nil, nil
			}
			var data []byte
			if rv.Kind() == reflect.Array {
				data = make([]byte, rv.Len())
				for i := 0; i < rv.Len(); i++ {
					data[i] = rv.Index(i).Interface().(byte)
				}
			} else {
				data = rv.Bytes()
			}
			return &dynamodb.AttributeValue{B: data}, nil
		}

		// sets
		if special == "set" {
			// sets can't be empty
			if rv.Len() == 0 {
				return nil, nil
			}
			return marshalSet(rv)
		}

		// lists CAN be empty
		avs := make([]*dynamodb.AttributeValue, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			innerVal := rv.Index(i)
			av, err := marshal(innerVal.Interface(), "")
			if err != nil {
				return nil, err
			}
			avs = append(avs, av)
		}
		return &dynamodb.AttributeValue{L: avs}, nil
	default:
		return nil, fmt.Errorf("dynamo marshal: unknown type %s", rv.Type().String())
	}
}

func marshalSet(rv reflect.Value) (*dynamodb.AttributeValue, error) {
	iface := reflect.Zero(rv.Type().Elem()).Interface()
	switch iface.(type) {
	case encoding.TextMarshaler:
		ss := make([]*string, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			tm := rv.Index(i).Interface().(encoding.TextMarshaler)
			text, err := tm.MarshalText()
			if err != nil {
				return nil, err
			}
			if len(text) > 0 {
				ss = append(ss, aws.String(string(text)))
			}
		}
		return &dynamodb.AttributeValue{SS: ss}, nil
	}

	switch rv.Type().Kind() {
	case reflect.Slice:
		switch rv.Type().Elem().Kind() {
		case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
			ns := make([]*string, 0, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				ns = append(ns, aws.String(strconv.FormatInt(rv.Index(i).Int(), 10)))
			}
			return &dynamodb.AttributeValue{NS: ns}, nil
		case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
			ns := make([]*string, 0, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				ns = append(ns, aws.String(strconv.FormatUint(rv.Index(i).Uint(), 10)))
			}
			return &dynamodb.AttributeValue{NS: ns}, nil
		case reflect.Float32, reflect.Float64:
			ns := make([]*string, 0, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				ns = append(ns, aws.String(strconv.FormatFloat(rv.Index(i).Float(), 'f', -1, 64)))
			}
			return &dynamodb.AttributeValue{NS: ns}, nil
		case reflect.String:
			ss := make([]*string, 0, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				ss = append(ss, aws.String(rv.Index(i).String()))
			}
			return &dynamodb.AttributeValue{SS: ss}, nil
		case reflect.Slice:
			if rv.Type().Elem().Elem().Kind() == reflect.Uint8 {
				bs := make([][]byte, 0, rv.Len())
				for i := 0; i < rv.Len(); i++ {
					bs = append(bs, rv.Index(i).Bytes())
				}
				return &dynamodb.AttributeValue{BS: bs}, nil
			}
		}
	case reflect.Map:
		useBool := rv.Type().Elem().Kind() == reflect.Bool
		if !useBool && rv.Type().Elem() != emptyStructType {
			return nil, fmt.Errorf("dynamo: cannot marshal type %v into a set", rv.Type())
		}

		switch rv.Type().Key().Kind() {
		case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
			ns := make([]*string, 0, rv.Len())
			for _, k := range rv.MapKeys() {
				if !useBool || rv.MapIndex(k).Bool() {
					ns = append(ns, aws.String(strconv.FormatInt(k.Int(), 10)))
				}
			}
			return &dynamodb.AttributeValue{NS: ns}, nil
		case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
			ns := make([]*string, 0, rv.Len())
			for _, k := range rv.MapKeys() {
				if !useBool || rv.MapIndex(k).Bool() {
					ns = append(ns, aws.String(strconv.FormatUint(k.Uint(), 10)))
				}
			}
			return &dynamodb.AttributeValue{NS: ns}, nil
		case reflect.Float32, reflect.Float64:
			ns := make([]*string, 0, rv.Len())
			for _, k := range rv.MapKeys() {
				if !useBool || rv.MapIndex(k).Bool() {
					ns = append(ns, aws.String(strconv.FormatFloat(k.Float(), 'f', -1, 64)))
				}
			}
			return &dynamodb.AttributeValue{NS: ns}, nil
		case reflect.String:
			ss := make([]*string, 0, rv.Len())
			for _, k := range rv.MapKeys() {
				if !useBool || rv.MapIndex(k).Bool() {
					ss = append(ss, aws.String(k.String()))
				}
			}
			return &dynamodb.AttributeValue{SS: ss}, nil
		case reflect.Array:
			if rv.Type().Key().Elem().Kind() == reflect.Uint8 {
				bs := make([][]byte, 0, rv.Len())
				for _, k := range rv.MapKeys() {
					if useBool && !rv.MapIndex(k).Bool() {
						continue
					}
					// TODO: is there a better way to turn [n]byte into []byte with reflection?
					key := make([]byte, k.Len())
					for i := 0; i < k.Len(); i++ {
						key[i] = (byte)(k.Index(i).Uint())
					}
					bs = append(bs, key)
				}
				return &dynamodb.AttributeValue{BS: bs}, nil
			}
		}
	}

	return nil, fmt.Errorf("dynamo marshal: unknown type for sets %s", rv.Type().String())
}

var emptyStructType = reflect.TypeOf(struct{}{})

func marshalSlice(values []interface{}) ([]*dynamodb.AttributeValue, error) {
	avs := make([]*dynamodb.AttributeValue, 0, len(values))
	for _, v := range values {
		av, err := marshal(v, "")
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
