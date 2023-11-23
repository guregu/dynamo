package dynamo

import (
	"encoding"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"golang.org/x/exp/constraints"
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

func encodeSliceSet(rt /* []T */ reflect.Type, flags encodeFlags) (encodeFunc, error) {
	switch {
	case rt.Elem().Implements(rtypeTextMarshaler):
		return func(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
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
		}, nil
	}
	switch rt.Elem().Kind() {
	// NS
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		return encodeSliceNS[int64]((reflect.Value).Int, strconv.FormatInt), nil
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		return encodeSliceNS[uint64]((reflect.Value).Uint, strconv.FormatUint), nil
	case reflect.Float64, reflect.Float32:
		return encodeSliceNS[float64]((reflect.Value).Float, formatFloat), nil

	// SS
	case reflect.String:
		return func(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
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
		}, nil

	// BS
	case reflect.Slice:
		if rt.Elem().Elem().Kind() == reflect.Uint8 {
			return func(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
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
			}, nil
		}
	}

	return nil, fmt.Errorf("dynamo: invalid type for set: %v", rt)
}

func encodeMapM(rt reflect.Type, flags encodeFlags) (encodeFunc, error) {
	var keyString func(k reflect.Value) (string, error)
	if ktype := rt.Key(); ktype.Implements(rtypeTextMarshaler) {
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
		return nil, fmt.Errorf("dynamo marshal: map key must be string, have %v", rt)
	}

	subflags := flagNone
	if flags&flagAllowEmptyElem != 0 {
		subflags |= flagAllowEmpty | flagNull
		// child containers of a map also have the allowEmptyElem behavior
		// i.e. lists inside a map or maps inside a map
		subflags |= flagAllowEmptyElem
	} else if flags&flagOmitEmptyElem != 0 {
		subflags |= flagOmitEmpty
	}

	valueEnc, err := encoderFor(rt.Elem(), subflags)
	if err != nil {
		return nil, err
	}

	return func(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
		if rv.IsNil() {
			if flags&flagAllowEmpty != 0 {
				return &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{}}, nil
			}
			if flags&flagNull != 0 {
				return &dynamodb.AttributeValue{NULL: aws.Bool(true)}, nil
			}
			return nil, nil
		}

		avs := make(map[string]*dynamodb.AttributeValue, rv.Len())

		iter := rv.MapRange()
		for iter.Next() {
			v, err := valueEnc(iter.Value(), subflags)
			if err != nil {
				return nil, err
			}
			if v == nil {
				return nil, nil
			}

			kstr, err := keyString(iter.Key())
			if err != nil {
				return nil, err
			}

			avs[kstr] = v
		}

		if flags&flagOmitEmpty != 0 && len(avs) == 0 {
			return nil, nil
		}

		return &dynamodb.AttributeValue{M: avs}, nil
	}, nil
}

func encodeMapSet(rt /* map[T]bool | map[T]struct{} */ reflect.Type, flags encodeFlags) (encodeFunc, error) {
	truthy := truthy(rt)
	useBool := truthy.Kind() == reflect.Bool
	if !truthy.IsValid() {
		return nil, fmt.Errorf("dynamo: cannot marshal type %v into a set (value type of map must be ~bool or ~struct{})", rt)
	}

	if rt.Key().Implements(rtypeTextMarshaler) {
		return func(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
			length := rv.Len()
			ss := make([]*string, 0, length)
			// strs := make([]string, length)
			iter := rv.MapRange()
			for iter.Next() {
				if useBool && !iter.Value().Equal(truthy) {
					continue
				}
				text, err := iter.Key().Interface().(encoding.TextMarshaler).MarshalText()
				if err != nil {
					return nil, err
				}
				if flags&flagOmitEmptyElem != 0 && len(text) == 0 {
					continue
				}
				str := string(text)
				ss = append(ss, &str)
			}
			return &dynamodb.AttributeValue{SS: ss}, nil
		}, nil
	}

	switch rt.Key().Kind() {
	// NS
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		return encodeMapNS[int64](truthy, (reflect.Value).Int, strconv.FormatInt), nil
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		return encodeMapNS[uint64](truthy, (reflect.Value).Uint, strconv.FormatUint), nil
	case reflect.Float32, reflect.Float64:
		return encodeMapNS[float64](truthy, (reflect.Value).Float, formatFloat), nil

	// SS
	case reflect.String:
		return func(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
			ss := make([]*string, 0, rv.Len())
			iter := rv.MapRange()
			for iter.Next() {
				if useBool && !iter.Value().Equal(truthy) {
					continue
				}
				s := iter.Key().String()
				if flags&flagOmitEmptyElem != 0 && s == "" {
					continue
				}
				ss = append(ss, aws.String(s))
			}
			if len(ss) == 0 {
				return nil, nil
			}
			return &dynamodb.AttributeValue{SS: ss}, nil
		}, nil

	// BS
	case reflect.Array:
		if rt.Key().Elem().Kind() == reflect.Uint8 {
			size := rt.Key().Len()
			return func(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
				bs := make([][]byte, 0, rv.Len())
				key := make([]byte, size)
				keyv := reflect.ValueOf(key)
				iter := rv.MapRange()
				for iter.Next() {
					if useBool && !iter.Value().Equal(truthy) {
						continue
					}
					reflect.Copy(keyv, iter.Key())
					bs = append(bs, key)
				}
				if len(bs) == 0 {
					return nil, nil
				}
				return &dynamodb.AttributeValue{BS: bs}, nil
			}, nil
		}
	}

	return nil, fmt.Errorf("dynamo: invalid type for set: %v", rt)
}

func encodeN[T constraints.Integer | constraints.Float](get func(reflect.Value) T, format func(T, int) string) encodeFunc {
	return func(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
		str := format(get(rv), 10)
		return &dynamodb.AttributeValue{N: &str}, nil
	}
}

func encodeSliceNS[T constraints.Integer | constraints.Float](get func(reflect.Value) T, format func(T, int) string) encodeFunc {
	return func(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
		ns := make([]*string, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			n := get(rv.Index(i))
			if flags&flagOmitEmptyElem != 0 && n == 0 {
				continue
			}
			str := format(n, 10)
			ns = append(ns, &str)
		}
		if len(ns) == 0 {
			return nil, nil
		}
		return &dynamodb.AttributeValue{NS: ns}, nil
	}
}

func encodeMapNS[T constraints.Integer | constraints.Float](truthy reflect.Value, get func(reflect.Value) T, format func(T, int) string) encodeFunc {
	useBool := truthy.Kind() == reflect.Bool
	return func(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
		ns := make([]*string, 0, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			if useBool && !iter.Value().Equal(truthy) {
				continue
			}
			n := get(iter.Key())
			if flags&flagOmitEmptyElem != 0 && n == 0 {
				continue
			}
			str := format(n, 10)
			ns = append(ns, &str)
		}
		if len(ns) == 0 {
			return nil, nil
		}
		return &dynamodb.AttributeValue{NS: ns}, nil
	}
}

func encodeSet(rt /* []T | map[T]bool | map[T]struct{} */ reflect.Type, flags encodeFlags) (encodeFunc, error) {
	switch rt.Kind() {
	case reflect.Slice:
		return encodeSliceSet(rt, flags)
	case reflect.Map:
		return encodeMapSet(rt, flags)
	}

	return nil, fmt.Errorf("dynamo: marshal: invalid type for set %s", rt.String())
}

func encodeList(rt reflect.Type, flags encodeFlags) (encodeFunc, error) {
	// lists CAN be empty
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

	valueEnc, err := encoderFor(rt.Elem(), subflags)
	if err != nil {
		return nil, err
	}

	return func(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
		avs := make([]*dynamodb.AttributeValue, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			innerVal := rv.Index(i)
			av, err := valueEnc(innerVal, flags|subflags)
			if err != nil {
				return nil, err
			}
			if av == nil {
				if flags&flagOmitEmptyElem != 0 {
					continue
				}
				av = nullAV
			}
			if av != nil {
				avs = append(avs, av)
			}
		}
		if flags&flagOmitEmpty != 0 && len(avs) == 0 {
			return nil, nil
		}
		return &dynamodb.AttributeValue{L: avs}, nil
	}, nil
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

func encodeUnixTime(rt reflect.Type) encodeFunc {
	switch rt {
	case rtypeTimePtr:
		return encode2[*time.Time](func(t *time.Time, flags encodeFlags) (*dynamodb.AttributeValue, error) {
			if t == nil || t.IsZero() {
				return nil, nil
			}
			str := strconv.FormatInt(t.Unix(), 10)
			return &dynamodb.AttributeValue{N: &str}, nil
		})
	case rtypeTime:
		return encode2[time.Time](func(t time.Time, flags encodeFlags) (*dynamodb.AttributeValue, error) {
			if t.IsZero() {
				return nil, nil
			}
			str := strconv.FormatInt(t.Unix(), 10)
			return &dynamodb.AttributeValue{N: &str}, nil
		})
	}
	panic(fmt.Errorf("not time type: %v", rt))
}

func encode2[T any](fn func(T, encodeFlags) (*dynamodb.AttributeValue, error)) func(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
	return func(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
		if !rv.IsValid() || !rv.CanInterface() {
			return nil, nil
		}

		if (rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface) && rv.IsNil() {
			if flags&flagNull != 0 {
				return nullAV, nil
			}
			return nil, nil
		}

		v := rv.Interface().(T)
		return fn(v, flags)
	}
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
