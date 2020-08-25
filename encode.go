package dynamo

import (
	"bytes"
	"encoding"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
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
	switch x := v.(type) {
	case map[string]*dynamodb.AttributeValue:
		return x, nil
	case awsEncoder:
		// special case for AWSEncoding
		return dynamodbattribute.MarshalMap(x.iface)
	case ItemMarshaler:
		return x.MarshalDynamoItem()
	}

	rv := reflect.ValueOf(v)

	switch rv.Type().Kind() {
	case reflect.Ptr:
		return marshalItem(rv.Elem().Interface())
	case reflect.Struct:
		return marshalStruct(rv)
	case reflect.Map:
		return marshalItemMap(rv.Interface())
	}
	return nil, fmt.Errorf("dynamo: marshal item: unsupported type %T: %v", rv.Interface(), rv.Interface())
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
	// encoders with precedence over interfaces
	if flags&flagUnixTime != 0 {
		switch x := v.(type) {
		case *time.Time:
			if x != nil {
				return marshal(*x, flags)
			}
		case time.Time:
			if x.IsZero() {
				// omitempty behaviour
				return nil, nil
			}

			ts := strconv.FormatInt(x.Unix(), 10)
			return &dynamodb.AttributeValue{N: &ts}, nil
		}
	}

	rv := reflect.ValueOf(v)

	switch x := v.(type) {
	case *dynamodb.AttributeValue:
		return x, nil
	case Marshaler:
		if rv.Kind() == reflect.Ptr && rv.IsNil() {
			if _, ok := rv.Type().Elem().MethodByName("MarshalDynamo"); ok {
				// MarshalDynamo is defined on value type, but this is a nil ptr
				if flags&flagNull != 0 {
					return &dynamodb.AttributeValue{NULL: aws.Bool(true)}, nil
				}
				return nil, nil
			}
		}
		return x.MarshalDynamo()
	case dynamodbattribute.Marshaler:
		if rv.Kind() == reflect.Ptr && rv.IsNil() {
			if _, ok := rv.Type().Elem().MethodByName("MarshalDynamoDBAttributeValue"); ok {
				// MarshalDynamoDBAttributeValue is defined on value type, but this is a nil ptr
				if flags&flagNull != 0 {
					return &dynamodb.AttributeValue{NULL: aws.Bool(true)}, nil
				}
				return nil, nil
			}
		}
		av := &dynamodb.AttributeValue{}
		return av, x.MarshalDynamoDBAttributeValue(av)
	case encoding.TextMarshaler:
		if rv.Kind() == reflect.Ptr && rv.IsNil() {
			if _, ok := rv.Type().Elem().MethodByName("MarshalText"); ok {
				// MarshalText is defined on value type, but this is a nil ptr
				if flags&flagNull != 0 {
					return &dynamodb.AttributeValue{NULL: aws.Bool(true)}, nil
				}
				return nil, nil
			}
		}
		text, err := x.MarshalText()
		if err != nil {
			return nil, err
		}
		if len(text) == 0 {
			if flags&flagAllowEmpty != 0 {
				return &dynamodb.AttributeValue{S: aws.String("")}, nil
			}
			return nil, nil
		}
		return &dynamodb.AttributeValue{S: aws.String(string(text))}, err
	case nil:
		if flags&flagNull != 0 {
			return &dynamodb.AttributeValue{NULL: aws.Bool(true)}, nil
		}
		return nil, nil
	}
	return marshalReflect(rv, flags)
}

var nilTm encoding.TextMarshaler
var tmType = reflect.TypeOf(&nilTm).Elem()

func marshalReflect(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
	switch rv.Kind() {
	case reflect.Ptr:
		if rv.IsNil() {
			if flags&flagNull != 0 {
				return &dynamodb.AttributeValue{NULL: aws.Bool(true)}, nil
			}
			return nil, nil
		}
		return marshal(rv.Elem().Interface(), flags)
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
			if flags&flagAllowEmpty != 0 {
				return &dynamodb.AttributeValue{S: aws.String("")}, nil
			}
			if flags&flagNull != 0 {
				return &dynamodb.AttributeValue{NULL: aws.Bool(true)}, nil
			}
			return nil, nil
		}
		return &dynamodb.AttributeValue{S: aws.String(s)}, nil
	case reflect.Map:
		if flags&flagSet != 0 {
			// sets can't be empty
			if rv.Len() == 0 {
				if flags&flagNull != 0 {
					return &dynamodb.AttributeValue{NULL: aws.Bool(true)}, nil
				}
				return nil, nil
			}
			return marshalSet(rv, flags)
		}

		// automatically omit nil maps
		if rv.IsNil() {
			if flags&flagAllowEmpty != 0 {
				return &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{}}, nil
			}
			if flags&flagNull != 0 {
				return &dynamodb.AttributeValue{NULL: aws.Bool(true)}, nil
			}
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
		subflags := flagNone
		if flags&flagAllowEmptyElem != 0 {
			subflags |= flagAllowEmpty | flagNull
		} else if flags&flagOmitEmptyElem != 0 {
			subflags |= flagOmitEmpty
		}
		for _, key := range rv.MapKeys() {
			v, err := marshal(rv.MapIndex(key).Interface(), subflags)
			if err != nil {
				return nil, err
			}
			kstr, err := keyString(key)
			if err != nil {
				return nil, err
			}
			if v != nil {
				avs[kstr] = v
			}
		}
		if flags&flagOmitEmpty != 0 && len(avs) == 0 {
			return nil, nil
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
			if rv.Len() == 0 {
				if rv.IsNil() && flags&flagNull != 0 {
					return &dynamodb.AttributeValue{NULL: aws.Bool(true)}, nil
				}
				if flags&flagAllowEmpty != 0 {
					return &dynamodb.AttributeValue{B: []byte{}}, nil
				}
				return nil, nil
			}
			var data []byte
			if rv.Kind() == reflect.Array {
				data = make([]byte, rv.Len())
				reflect.Copy(reflect.ValueOf(data), rv)
			} else {
				data = rv.Bytes()
			}
			return &dynamodb.AttributeValue{B: data}, nil
		}

		if flags&flagNull != 0 && rv.IsNil() {
			return &dynamodb.AttributeValue{NULL: aws.Bool(true)}, nil
		}

		// sets
		if flags&flagSet != 0 {
			// sets can't be empty
			if rv.Len() == 0 {
				return nil, nil
			}
			return marshalSet(rv, flags)
		}

		// lists CAN be empty
		avs := make([]*dynamodb.AttributeValue, 0, rv.Len())
		subflags := flagNone
		if flags&flagOmitEmptyElem == 0 {
			// unless "omitemptyelem" flag is set, include empty/null values
			// this will preserve the position of items in the list
			subflags |= flagAllowEmpty | flagNull
		}
		for i := 0; i < rv.Len(); i++ {
			innerVal := rv.Index(i)
			av, err := marshal(innerVal.Interface(), subflags)
			if err != nil {
				return nil, err
			}
			if av != nil {
				avs = append(avs, av)
			}
		}
		if flags&flagOmitEmpty != 0 && len(avs) == 0 {
			return nil, nil
		}
		return &dynamodb.AttributeValue{L: avs}, nil
	default:
		return nil, fmt.Errorf("dynamo marshal: unknown type %s", rv.Type().String())
	}
}

func marshalSet(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
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
			if flags&flagOmitEmptyElem != 0 && len(text) == 0 {
				continue
			}
			ss = append(ss, aws.String(string(text)))
		}
		if len(ss) == 0 {
			return nil, nil
		}
		return &dynamodb.AttributeValue{SS: ss}, nil
	}

	switch rv.Type().Kind() {
	case reflect.Slice:
		switch rv.Type().Elem().Kind() {
		case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
			ns := make([]*string, 0, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				n := rv.Index(i).Int()
				if flags&flagOmitEmptyElem != 0 && n == 0 {
					continue
				}
				ns = append(ns, aws.String(strconv.FormatInt(n, 10)))
			}
			if len(ns) == 0 {
				return nil, nil
			}
			return &dynamodb.AttributeValue{NS: ns}, nil
		case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
			ns := make([]*string, 0, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				n := rv.Index(i).Uint()
				if flags&flagOmitEmptyElem != 0 && n == 0 {
					continue
				}
				ns = append(ns, aws.String(strconv.FormatUint(n, 10)))
			}
			if len(ns) == 0 {
				return nil, nil
			}
			return &dynamodb.AttributeValue{NS: ns}, nil
		case reflect.Float32, reflect.Float64:
			ns := make([]*string, 0, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				n := rv.Index(i).Float()
				if flags&flagOmitEmptyElem != 0 && n == 0 {
					continue
				}
				ns = append(ns, aws.String(strconv.FormatFloat(n, 'f', -1, 64)))
			}
			if len(ns) == 0 {
				return nil, nil
			}
			return &dynamodb.AttributeValue{NS: ns}, nil
		case reflect.String:
			ss := make([]*string, 0, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				s := rv.Index(i).String()
				if flags&flagOmitEmptyElem != 0 && s == "" {
					continue
				}
				ss = append(ss, aws.String(s))
			}
			if len(ss) == 0 {
				return nil, nil
			}
			return &dynamodb.AttributeValue{SS: ss}, nil
		case reflect.Slice:
			if rv.Type().Elem().Elem().Kind() == reflect.Uint8 {
				bs := make([][]byte, 0, rv.Len())
				for i := 0; i < rv.Len(); i++ {
					b := rv.Index(i).Bytes()
					if flags&flagOmitEmptyElem != 0 && len(b) == 0 {
						continue
					}
					bs = append(bs, b)
				}
				if len(bs) == 0 {
					return nil, nil
				}
				return &dynamodb.AttributeValue{BS: bs}, nil
			}
		}
	case reflect.Map:
		useBool := rv.Type().Elem().Kind() == reflect.Bool
		if !useBool && rv.Type().Elem() != emptyStructType && !(rv.Type().Elem().Kind() == reflect.Struct && rv.Type().Elem().NumField() == 0) {
			return nil, fmt.Errorf("dynamo: cannot marshal type %v into a set", rv.Type())
		}

		if rv.Type().Key().Implements(tmType) {
			ss := make([]*string, 0, rv.Len())
			for _, k := range rv.MapKeys() {
				if !useBool || rv.MapIndex(k).Bool() {
					txt, err := k.Interface().(encoding.TextMarshaler).MarshalText()
					if err != nil {
						return nil, err
					}
					if flags&flagOmitEmptyElem != 0 && len(txt) == 0 {
						continue
					}
					ss = append(ss, aws.String(string(txt)))
				}
			}
			if len(ss) == 0 {
				return nil, nil
			}
			return &dynamodb.AttributeValue{SS: ss}, nil
		}

		switch rv.Type().Key().Kind() {
		case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
			ns := make([]*string, 0, rv.Len())
			for _, k := range rv.MapKeys() {
				if !useBool || rv.MapIndex(k).Bool() {
					n := k.Int()
					if flags&flagOmitEmptyElem != 0 && n == 0 {
						continue
					}
					ns = append(ns, aws.String(strconv.FormatInt(n, 10)))
				}
			}
			if len(ns) == 0 {
				return nil, nil
			}
			return &dynamodb.AttributeValue{NS: ns}, nil
		case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
			ns := make([]*string, 0, rv.Len())
			for _, k := range rv.MapKeys() {
				if !useBool || rv.MapIndex(k).Bool() {
					n := k.Uint()
					if flags&flagOmitEmptyElem != 0 && n == 0 {
						continue
					}
					ns = append(ns, aws.String(strconv.FormatUint(n, 10)))
				}
			}
			if len(ns) == 0 {
				return nil, nil
			}
			return &dynamodb.AttributeValue{NS: ns}, nil
		case reflect.Float32, reflect.Float64:
			ns := make([]*string, 0, rv.Len())
			for _, k := range rv.MapKeys() {
				if !useBool || rv.MapIndex(k).Bool() {
					n := k.Float()
					if flags&flagOmitEmptyElem != 0 && n == 0 {
						continue
					}
					ns = append(ns, aws.String(strconv.FormatFloat(n, 'f', -1, 64)))
				}
			}
			if len(ns) == 0 {
				return nil, nil
			}
			return &dynamodb.AttributeValue{NS: ns}, nil
		case reflect.String:
			ss := make([]*string, 0, rv.Len())
			for _, k := range rv.MapKeys() {
				if !useBool || rv.MapIndex(k).Bool() {
					s := k.String()
					if flags&flagOmitEmptyElem != 0 && s == "" {
						continue
					}
					ss = append(ss, aws.String(s))
				}
			}
			if len(ss) == 0 {
				return nil, nil
			}
			return &dynamodb.AttributeValue{SS: ss}, nil
		case reflect.Array:
			if rv.Type().Key().Elem().Kind() == reflect.Uint8 {
				bs := make([][]byte, 0, rv.Len())
				for _, k := range rv.MapKeys() {
					if useBool && !rv.MapIndex(k).Bool() {
						continue
					}
					key := make([]byte, k.Len())
					reflect.Copy(reflect.ValueOf(key), k)
					bs = append(bs, key)
				}
				if len(bs) == 0 {
					return nil, nil
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
		av, err := marshal(v, flagNone)
		if err != nil {
			return nil, err
		}
		if av != nil {
			avs = append(avs, av)
		}
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
	tags := strings.Split(field.Tag.Get("dynamo"), ",")
	if len(tags) == 0 {
		return field.Name, flagNone
	}

	name = tags[0]
	if name == "" {
		name = field.Name
	}

	for _, t := range tags[1:] {
		switch t {
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
	z := reflect.Zero(rv.Type())
	return rv.Interface() == z.Interface()
}

// only works for primary key types
func isAVEqual(a, b *dynamodb.AttributeValue) bool {
	if a.S != nil {
		if b.S == nil {
			return false
		}
		return *a.S == *b.S
	}
	if a.N != nil {
		if b.N == nil {
			return false
		}
		// TODO: parse numbers?
		return *a.N == *b.N
	}
	if a.B != nil {
		if b.B == nil {
			return false
		}
		return bytes.Equal(a.B, b.B)
	}
	return false
}

// isNil returns true if v is considered nil
// this is used to determine if an attribute should be set or removed
func isNil(v interface{}) bool {
	if v == nil || v == "" {
		return true
	}

	// consider v nil if it's a special encoder defined on a value type, but v is a pointer
	rv := reflect.ValueOf(v)
	switch v.(type) {
	case Marshaler:
		if rv.Kind() == reflect.Ptr && rv.IsNil() {
			if _, ok := rv.Type().Elem().MethodByName("MarshalDynamo"); ok {
				return true
			}
		}
	case dynamodbattribute.Marshaler:
		if rv.Kind() == reflect.Ptr && rv.IsNil() {
			if _, ok := rv.Type().Elem().MethodByName("MarshalDynamoDBAttributeValue"); ok {
				return true
			}
		}
	case encoding.TextMarshaler:
		if rv.Kind() == reflect.Ptr && rv.IsNil() {
			if _, ok := rv.Type().Elem().MethodByName("MarshalText"); ok {
				return true
			}
		}
	default:
		// e.g. (*int)(nil)
		return rv.Kind() == reflect.Ptr && rv.IsNil()
	}

	// non-pointers or special encoders with a pointer receiver
	return false
}
