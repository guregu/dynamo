package dynamo

import (
	"encoding"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Marshaler is the interface implemented by objects that can marshal themselves into
// an AttributeValue.
type Marshaler interface {
	MarshalDynamo() (types.AttributeValue, error)
}

// ItemMarshaler is the interface implemented by objects that can marshal themselves
// into an Item (a map of strings to AttributeValues).
type ItemMarshaler interface {
	MarshalDynamoItem() (map[string]types.AttributeValue, error)
}

// MarshalItem converts the given struct into a DynamoDB item.
func MarshalItem(v interface{}) (map[string]types.AttributeValue, error) {
	return marshalItem(v)
}

func marshalItem(v interface{}) (map[string]types.AttributeValue, error) {
	switch x := v.(type) {
	case map[string]types.AttributeValue:
		return x, nil
	case awsEncoder:
		// special case for AWSEncoding
		return attributevalue.MarshalMap(x.iface)
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

func marshalItemMap(v interface{}) (map[string]types.AttributeValue, error) {
	// TODO: maybe unify this with the map stuff in marshal
	av, err := marshal(v, flagNone)
	if err != nil {
		return nil, err
	}
	m, _ := av.(*types.AttributeValueMemberM)
	if m == nil {
		return nil, fmt.Errorf("dynamo: internal error: encoding map but M was empty")
	}
	return m.Value, nil
}

func marshalStruct(rv reflect.Value) (map[string]types.AttributeValue, error) {
	item := make(map[string]types.AttributeValue)
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
func Marshal(v interface{}) (types.AttributeValue, error) {
	return marshal(v, flagNone)
}

func marshal(v interface{}, flags encodeFlags) (types.AttributeValue, error) {
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
			return &types.AttributeValueMemberN{Value: ts}, nil
		}
	}

	rv := reflect.ValueOf(v)

	switch x := v.(type) {
	case types.AttributeValue:
		return x, nil
	case Marshaler:
		if rv.Kind() == reflect.Ptr && rv.IsNil() {
			if _, ok := rv.Type().Elem().MethodByName("MarshalDynamo"); ok {
				// MarshalDynamo is defined on value type, but this is a nil ptr
				if flags&flagNull != 0 {
					return &types.AttributeValueMemberNULL{Value: true}, nil
				}
				return nil, nil
			}
		}
		return x.MarshalDynamo()
	case attributevalue.Marshaler:
		if rv.Kind() == reflect.Ptr && rv.IsNil() {
			if _, ok := rv.Type().Elem().MethodByName("MarshalDynamoDBAttributeValue"); ok {
				// MarshalDynamoDBAttributeValue is defined on value type, but this is a nil ptr
				if flags&flagNull != 0 {
					return &types.AttributeValueMemberNULL{Value: true}, nil
				}
				return nil, nil
			}
		}

		return x.MarshalDynamoDBAttributeValue()
	case encoding.TextMarshaler:
		if rv.Kind() == reflect.Ptr && rv.IsNil() {
			if _, ok := rv.Type().Elem().MethodByName("MarshalText"); ok {
				// MarshalText is defined on value type, but this is a nil ptr
				if flags&flagNull != 0 {
					return &types.AttributeValueMemberNULL{Value: true}, nil
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
				return &types.AttributeValueMemberS{Value: ""}, nil
			}
			return nil, nil
		}
		return &types.AttributeValueMemberS{Value: string(text)}, err
	case nil:
		if flags&flagNull != 0 {
			return &types.AttributeValueMemberNULL{Value: true}, nil
		}
		return nil, nil
	}
	return marshalReflect(rv, flags)
}

var nilTm encoding.TextMarshaler
var tmType = reflect.TypeOf(&nilTm).Elem()

func marshalReflect(rv reflect.Value, flags encodeFlags) (types.AttributeValue, error) {
	switch rv.Kind() {
	case reflect.Ptr:
		if rv.IsNil() {
			if flags&flagNull != 0 {
				return &types.AttributeValueMemberNULL{Value: true}, nil
			}
			return nil, nil
		}
		return marshal(rv.Elem().Interface(), flags)
	case reflect.Bool:
		return &types.AttributeValueMemberBOOL{Value: rv.Bool()}, nil
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		return &types.AttributeValueMemberN{Value: strconv.FormatInt(rv.Int(), 10)}, nil
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		return &types.AttributeValueMemberN{Value: strconv.FormatUint(rv.Uint(), 10)}, nil
	case reflect.Float32, reflect.Float64:
		return &types.AttributeValueMemberN{Value: strconv.FormatFloat(rv.Float(), 'f', -1, 64)}, nil
	case reflect.String:
		s := rv.String()
		if len(s) == 0 {
			if flags&flagAllowEmpty != 0 {
				return &types.AttributeValueMemberS{Value: ""}, nil
			}
			if flags&flagNull != 0 {
				return &types.AttributeValueMemberNULL{Value: true}, nil
			}
			return nil, nil
		}
		return &types.AttributeValueMemberS{Value: s}, nil
	case reflect.Map:
		if flags&flagSet != 0 {
			// sets can't be empty
			if rv.Len() == 0 {
				if flags&flagNull != 0 {
					return &types.AttributeValueMemberNULL{Value: true}, nil
				}
				return nil, nil
			}
			return marshalSet(rv, flags)
		}

		// automatically omit nil maps
		if rv.IsNil() {
			if flags&flagAllowEmpty != 0 {
				return &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{}}, nil
			}
			if flags&flagNull != 0 {
				return &types.AttributeValueMemberNULL{Value: true}, nil
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

		avs := make(map[string]types.AttributeValue)
		subflags := flagNone
		if flags&flagAllowEmptyElem != 0 {
			subflags |= flagAllowEmpty | flagNull
			// child containers of a map also have the allowEmptyElem behavior
			// i.e. lists inside a map or maps inside a map
			subflags |= flagAllowEmptyElem
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
		return &types.AttributeValueMemberM{Value: avs}, nil
	case reflect.Struct:
		avs, err := marshalStruct(rv)
		if err != nil {
			return nil, err
		}
		return &types.AttributeValueMemberM{Value: avs}, nil
	case reflect.Slice, reflect.Array:
		// special case: byte slice is B
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			if rv.Len() == 0 {
				if rv.IsNil() && flags&flagNull != 0 {
					return &types.AttributeValueMemberNULL{Value: true}, nil
				}
				if flags&flagAllowEmpty != 0 {
					return &types.AttributeValueMemberB{Value: []byte{}}, nil
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
			return &types.AttributeValueMemberB{Value: data}, nil
		}

		if flags&flagNull != 0 && rv.IsNil() {
			return &types.AttributeValueMemberNULL{Value: true}, nil
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
		avs := make([]types.AttributeValue, 0, rv.Len())
		subflags := flagNone
		if flags&flagOmitEmptyElem == 0 {
			// unless "omitemptyelem" flag is set, include empty/null values
			// this will preserve the position of items in the list
			subflags |= flagAllowEmpty | flagNull
		}
		if flags&flagAllowEmptyElem != 0 {
			// child containers of a list also have the allowEmptyElem behavior
			// e.g. maps inside a list
			subflags |= flagAllowEmptyElem
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
		return &types.AttributeValueMemberL{Value: avs}, nil
	default:
		return nil, fmt.Errorf("dynamo marshal: unknown type %s", rv.Type().String())
	}
}

func marshalSet(rv reflect.Value, flags encodeFlags) (types.AttributeValue, error) {
	iface := reflect.Zero(rv.Type().Elem()).Interface()
	switch iface.(type) {
	case encoding.TextMarshaler:
		ss := make([]string, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			tm := rv.Index(i).Interface().(encoding.TextMarshaler)
			text, err := tm.MarshalText()
			if err != nil {
				return nil, err
			}
			if flags&flagOmitEmptyElem != 0 && len(text) == 0 {
				continue
			}
			ss = append(ss, string(text))
		}
		if len(ss) == 0 {
			return nil, nil
		}
		return &types.AttributeValueMemberSS{Value: ss}, nil
	}

	switch rv.Type().Kind() {
	case reflect.Slice:
		switch rv.Type().Elem().Kind() {
		case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
			ns := make([]string, 0, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				n := rv.Index(i).Int()
				if flags&flagOmitEmptyElem != 0 && n == 0 {
					continue
				}
				ns = append(ns, strconv.FormatInt(n, 10))
			}
			if len(ns) == 0 {
				return nil, nil
			}
			return &types.AttributeValueMemberNS{Value: ns}, nil
		case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
			ns := make([]string, 0, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				n := rv.Index(i).Uint()
				if flags&flagOmitEmptyElem != 0 && n == 0 {
					continue
				}
				ns = append(ns, strconv.FormatUint(n, 10))
			}
			if len(ns) == 0 {
				return nil, nil
			}
			return &types.AttributeValueMemberNS{Value: ns}, nil
		case reflect.Float32, reflect.Float64:
			ns := make([]string, 0, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				n := rv.Index(i).Float()
				if flags&flagOmitEmptyElem != 0 && n == 0 {
					continue
				}
				ns = append(ns, strconv.FormatFloat(n, 'f', -1, 64))
			}
			if len(ns) == 0 {
				return nil, nil
			}
			return &types.AttributeValueMemberNS{Value: ns}, nil
		case reflect.String:
			ss := make([]string, 0, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				s := rv.Index(i).String()
				if flags&flagOmitEmptyElem != 0 && s == "" {
					continue
				}
				ss = append(ss, s)
			}
			if len(ss) == 0 {
				return nil, nil
			}
			return &types.AttributeValueMemberSS{Value: ss}, nil
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
				return &types.AttributeValueMemberBS{Value: bs}, nil
			}
		}
	case reflect.Map:
		useBool := rv.Type().Elem().Kind() == reflect.Bool
		if !useBool && rv.Type().Elem() != emptyStructType && !(rv.Type().Elem().Kind() == reflect.Struct && rv.Type().Elem().NumField() == 0) {
			return nil, fmt.Errorf("dynamo: cannot marshal type %v into a set", rv.Type())
		}

		if rv.Type().Key().Implements(tmType) {
			ss := make([]string, 0, rv.Len())
			for _, k := range rv.MapKeys() {
				if !useBool || rv.MapIndex(k).Bool() {
					txt, err := k.Interface().(encoding.TextMarshaler).MarshalText()
					if err != nil {
						return nil, err
					}
					if flags&flagOmitEmptyElem != 0 && len(txt) == 0 {
						continue
					}
					ss = append(ss, string(txt))
				}
			}
			if len(ss) == 0 {
				return nil, nil
			}
			return &types.AttributeValueMemberSS{Value: ss}, nil
		}

		switch rv.Type().Key().Kind() {
		case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
			ns := make([]string, 0, rv.Len())
			for _, k := range rv.MapKeys() {
				if !useBool || rv.MapIndex(k).Bool() {
					n := k.Int()
					if flags&flagOmitEmptyElem != 0 && n == 0 {
						continue
					}
					ns = append(ns, strconv.FormatInt(n, 10))
				}
			}
			if len(ns) == 0 {
				return nil, nil
			}
			return &types.AttributeValueMemberNS{Value: ns}, nil
		case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
			ns := make([]string, 0, rv.Len())
			for _, k := range rv.MapKeys() {
				if !useBool || rv.MapIndex(k).Bool() {
					n := k.Uint()
					if flags&flagOmitEmptyElem != 0 && n == 0 {
						continue
					}
					ns = append(ns, strconv.FormatUint(n, 10))
				}
			}
			if len(ns) == 0 {
				return nil, nil
			}
			return &types.AttributeValueMemberNS{Value: ns}, nil
		case reflect.Float32, reflect.Float64:
			ns := make([]string, 0, rv.Len())
			for _, k := range rv.MapKeys() {
				if !useBool || rv.MapIndex(k).Bool() {
					n := k.Float()
					if flags&flagOmitEmptyElem != 0 && n == 0 {
						continue
					}
					ns = append(ns, strconv.FormatFloat(n, 'f', -1, 64))
				}
			}
			if len(ns) == 0 {
				return nil, nil
			}
			return &types.AttributeValueMemberNS{Value: ns}, nil
		case reflect.String:
			ss := make([]string, 0, rv.Len())
			for _, k := range rv.MapKeys() {
				if !useBool || rv.MapIndex(k).Bool() {
					s := k.String()
					if flags&flagOmitEmptyElem != 0 && s == "" {
						continue
					}
					ss = append(ss, s)
				}
			}
			if len(ss) == 0 {
				return nil, nil
			}
			return &types.AttributeValueMemberSS{Value: ss}, nil
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
				return &types.AttributeValueMemberBS{Value: bs}, nil
			}
		}
	}

	return nil, fmt.Errorf("dynamo marshal: unknown type for sets %s", rv.Type().String())
}

var emptyStructType = reflect.TypeOf(struct{}{})

func marshalSlice(values []interface{}) ([]types.AttributeValue, error) {
	avs := make([]types.AttributeValue, 0, len(values))
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
