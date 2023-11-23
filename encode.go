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

	// // encoders with precedence over interfaces
	// if flags&flagUnixTime != 0 {
	// 	switch x := v.(type) {
	// 	case *time.Time:
	// 		if x != nil {
	// 			return marshal(*x, flags)
	// 		}
	// 	case time.Time:
	// 		if x.IsZero() {
	// 			// omitempty behaviour
	// 			return nil, nil
	// 		}

	// 		ts := strconv.FormatInt(x.Unix(), 10)
	// 		return &dynamodb.AttributeValue{N: &ts}, nil
	// 	}
	// }

	// rv := reflect.ValueOf(v)

	// switch x := v.(type) {
	// case *dynamodb.AttributeValue:
	// 	return x, nil
	// case Marshaler:
	// 	if rv.Kind() == reflect.Ptr && rv.IsNil() {
	// 		if _, ok := rv.Type().Elem().MethodByName("MarshalDynamo"); ok {
	// 			// MarshalDynamo is defined on value type, but this is a nil ptr
	// 			if flags&flagNull != 0 {
	// 				return &dynamodb.AttributeValue{NULL: aws.Bool(true)}, nil
	// 			}
	// 			return nil, nil
	// 		}
	// 	}
	// 	return x.MarshalDynamo()
	// case dynamodbattribute.Marshaler:
	// 	if rv.Kind() == reflect.Ptr && rv.IsNil() {
	// 		if _, ok := rv.Type().Elem().MethodByName("MarshalDynamoDBAttributeValue"); ok {
	// 			// MarshalDynamoDBAttributeValue is defined on value type, but this is a nil ptr
	// 			if flags&flagNull != 0 {
	// 				return &dynamodb.AttributeValue{NULL: aws.Bool(true)}, nil
	// 			}
	// 			return nil, nil
	// 		}
	// 	}
	// 	av := &dynamodb.AttributeValue{}
	// 	return av, x.MarshalDynamoDBAttributeValue(av)
	// case encoding.TextMarshaler:
	// 	if rv.Kind() == reflect.Ptr && rv.IsNil() {
	// 		if _, ok := rv.Type().Elem().MethodByName("MarshalText"); ok {
	// 			// MarshalText is defined on value type, but this is a nil ptr
	// 			if flags&flagNull != 0 {
	// 				return &dynamodb.AttributeValue{NULL: aws.Bool(true)}, nil
	// 			}
	// 			return nil, nil
	// 		}
	// 	}
	// 	text, err := x.MarshalText()
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	if len(text) == 0 {
	// 		if flags&flagAllowEmpty != 0 {
	// 			return &dynamodb.AttributeValue{S: aws.String("")}, nil
	// 		}
	// 		return nil, nil
	// 	}
	// 	return &dynamodb.AttributeValue{S: aws.String(string(text))}, err
	// case nil:
	// 	if flags&flagNull != 0 {
	// 		return &dynamodb.AttributeValue{NULL: aws.Bool(true)}, nil
	// 	}
	// 	return nil, nil
	// }
	// return marshalReflect(rv, flags)
}

// func marshalReflect(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
// 	switch rv.Kind() {
// 	case reflect.Ptr:
// 		if rv.IsNil() {
// 			if flags&flagNull != 0 {
// 				return &dynamodb.AttributeValue{NULL: aws.Bool(true)}, nil
// 			}
// 			return nil, nil
// 		}
// 		return marshal(rv.Elem().Interface(), flags)
// 	case reflect.Bool:
// 		return &dynamodb.AttributeValue{BOOL: aws.Bool(rv.Bool())}, nil
// 	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
// 		return &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(rv.Int(), 10))}, nil
// 	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
// 		return &dynamodb.AttributeValue{N: aws.String(strconv.FormatUint(rv.Uint(), 10))}, nil
// 	case reflect.Float32, reflect.Float64:
// 		return &dynamodb.AttributeValue{N: aws.String(strconv.FormatFloat(rv.Float(), 'f', -1, 64))}, nil
// 	case reflect.String:
// 		s := rv.String()
// 		if len(s) == 0 {
// 			if flags&flagAllowEmpty != 0 {
// 				return &dynamodb.AttributeValue{S: aws.String("")}, nil
// 			}
// 			if flags&flagNull != 0 {
// 				return &dynamodb.AttributeValue{NULL: aws.Bool(true)}, nil
// 			}
// 			return nil, nil
// 		}
// 		return &dynamodb.AttributeValue{S: aws.String(s)}, nil
// 	case reflect.Map:
// 		if flags&flagSet != 0 {
// 			// sets can't be empty
// 			if rv.Len() == 0 {
// 				if flags&flagNull != 0 {
// 					return &dynamodb.AttributeValue{NULL: aws.Bool(true)}, nil
// 				}
// 				return nil, nil
// 			}
// 			return marshalSet(rv, flags)
// 		}

// 		// automatically omit nil maps
// 		if rv.IsNil() {
// 			if flags&flagAllowEmpty != 0 {
// 				return &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{}}, nil
// 			}
// 			if flags&flagNull != 0 {
// 				return &dynamodb.AttributeValue{NULL: aws.Bool(true)}, nil
// 			}
// 			return nil, nil
// 		}

// 		var keyString func(k reflect.Value) (string, error)
// 		if ktype := rv.Type().Key(); ktype.Implements(tmType) {
// 			keyString = func(k reflect.Value) (string, error) {
// 				tm := k.Interface().(encoding.TextMarshaler)
// 				txt, err := tm.MarshalText()
// 				if err != nil {
// 					return "", fmt.Errorf("dynamo: marshal map: key error: %v", err)
// 				}
// 				return string(txt), nil
// 			}
// 		} else if ktype.Kind() == reflect.String {
// 			keyString = func(k reflect.Value) (string, error) {
// 				return k.String(), nil
// 			}
// 		} else {
// 			return nil, fmt.Errorf("dynamo marshal: map key must be string: %T", rv.Interface())
// 		}

// 		avs := make(map[string]*dynamodb.AttributeValue)
// 		subflags := flagNone
// 		if flags&flagAllowEmptyElem != 0 {
// 			subflags |= flagAllowEmpty | flagNull
// 			// child containers of a map also have the allowEmptyElem behavior
// 			// i.e. lists inside a map or maps inside a map
// 			subflags |= flagAllowEmptyElem
// 		} else if flags&flagOmitEmptyElem != 0 {
// 			subflags |= flagOmitEmpty
// 		}
// 		for _, key := range rv.MapKeys() {
// 			v, err := marshal(rv.MapIndex(key).Interface(), subflags)
// 			if err != nil {
// 				return nil, err
// 			}
// 			kstr, err := keyString(key)
// 			if err != nil {
// 				return nil, err
// 			}
// 			if v != nil {
// 				avs[kstr] = v
// 			}
// 		}
// 		if flags&flagOmitEmpty != 0 && len(avs) == 0 {
// 			return nil, nil
// 		}
// 		return &dynamodb.AttributeValue{M: avs}, nil
// 	case reflect.Struct:
// 		avs, err := marshalStruct(rv)
// 		if err != nil {
// 			return nil, err
// 		}
// 		return &dynamodb.AttributeValue{M: avs}, nil
// 	case reflect.Slice, reflect.Array:
// 		// special case: byte slice is B
// 		if rv.Type().Elem().Kind() == reflect.Uint8 {
// 			if rv.Len() == 0 {
// 				if rv.IsNil() && flags&flagNull != 0 {
// 					return &dynamodb.AttributeValue{NULL: aws.Bool(true)}, nil
// 				}
// 				if flags&flagAllowEmpty != 0 {
// 					return &dynamodb.AttributeValue{B: []byte{}}, nil
// 				}
// 				return nil, nil
// 			}
// 			var data []byte
// 			if rv.Kind() == reflect.Array {
// 				data = make([]byte, rv.Len())
// 				reflect.Copy(reflect.ValueOf(data), rv)
// 			} else {
// 				data = rv.Bytes()
// 			}
// 			return &dynamodb.AttributeValue{B: data}, nil
// 		}

// 		if flags&flagNull != 0 && rv.IsNil() {
// 			return &dynamodb.AttributeValue{NULL: aws.Bool(true)}, nil
// 		}

// 		// sets
// 		if flags&flagSet != 0 {
// 			// sets can't be empty
// 			if rv.Len() == 0 {
// 				return nil, nil
// 			}
// 			return marshalSet(rv, flags)
// 		}

// 		// lists CAN be empty
// 		avs := make([]*dynamodb.AttributeValue, 0, rv.Len())
// 		subflags := flagNone
// 		if flags&flagOmitEmptyElem == 0 {
// 			// unless "omitemptyelem" flag is set, include empty/null values
// 			// this will preserve the position of items in the list
// 			subflags |= flagAllowEmpty | flagNull
// 		}
// 		if flags&flagAllowEmptyElem != 0 {
// 			// child containers of a list also have the allowEmptyElem behavior
// 			// e.g. maps inside a list
// 			subflags |= flagAllowEmptyElem
// 		}
// 		for i := 0; i < rv.Len(); i++ {
// 			innerVal := rv.Index(i)
// 			av, err := marshal(innerVal.Interface(), subflags)
// 			if err != nil {
// 				return nil, err
// 			}
// 			if av != nil {
// 				avs = append(avs, av)
// 			}
// 		}
// 		if flags&flagOmitEmpty != 0 && len(avs) == 0 {
// 			return nil, nil
// 		}
// 		return &dynamodb.AttributeValue{L: avs}, nil
// 	default:
// 		return nil, fmt.Errorf("dynamo marshal: unknown type %s", rv.Type().String())
// 	}
// }

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

// func encodeUnixTime(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
// 	var unix int64
// 	switch rv.Type() {
// 	case rtypeTimePtr:
// 		tp := rv.Interface().(*time.Time)
// 		if tp == nil || tp.IsZero() {
// 			return nil, nil
// 		}
// 		unix = tp.Unix()
// 	case rtypeTime:
// 		t := rv.Interface().(time.Time)
// 		if t.IsZero() {
// 			return nil, nil
// 		}
// 		unix = t.Unix()
// 	}
// 	str := strconv.FormatInt(unix, 10)
// 	return &dynamodb.AttributeValue{N: &str}, nil
// }

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
