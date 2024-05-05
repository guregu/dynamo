package dynamo

import (
	"encoding"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type encodeFunc func(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error)

func (def *typedef) encodeType(rt reflect.Type, flags encodeFlags, info *structInfo) (encodeFunc, error) {
	encKey := encodeKey{rt, flags}
	if fn := info.findEncoder(encKey); fn != nil {
		return fn, nil
	}

	try := rt
	for {
		switch try {
		case rtypeAttr:
			return encode2(func(av *dynamodb.AttributeValue, _ encodeFlags) (*dynamodb.AttributeValue, error) {
				if av == nil {
					return nil, nil
				}
				return av, nil
			}), nil
		case rtypeTimePtr, rtypeTime:
			if flags&flagUnixTime != 0 {
				return encodeUnixTime(try), nil
			}
		}
		switch {
		case try.Implements(rtypeMarshaler):
			return encode2(func(x Marshaler, _ encodeFlags) (*dynamodb.AttributeValue, error) {
				return x.MarshalDynamo()
			}), nil
		case try.Implements(rtypeAWSMarshaler):
			return encode2(func(x dynamodbattribute.Marshaler, _ encodeFlags) (*dynamodb.AttributeValue, error) {
				var av dynamodb.AttributeValue
				err := x.MarshalDynamoDBAttributeValue(&av)
				return &av, err
			}), nil
		case try.Implements(rtypeTextMarshaler):
			return encodeTextMarshaler, nil
		}
		if try.Kind() == reflect.Pointer {
			try = try.Elem()
			continue
		}
		break
	}

	switch rt.Kind() {
	case reflect.Pointer:
		return def.encodePtr(rt, flags, info)

	// BOOL
	case reflect.Bool:
		return func(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
			return &dynamodb.AttributeValue{BOOL: aws.Bool(rv.Bool())}, nil
		}, nil

	// N
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		return encodeN((reflect.Value).Int, strconv.FormatInt), nil
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		return encodeN((reflect.Value).Uint, strconv.FormatUint), nil
	case reflect.Float32, reflect.Float64:
		return encodeN((reflect.Value).Float, formatFloat), nil

	// S
	case reflect.String:
		return encodeString, nil

	case reflect.Slice, reflect.Array:
		// byte slices are B
		if rt.Elem().Kind() == reflect.Uint8 {
			return encodeBytes(rt, flags), nil
		}
		// sets (NS, SS, BS)
		if flags&flagSet != 0 {
			return encodeSet(rt, flags)
		}
		// lists (L)
		return def.encodeList(rt, flags, info)

	case reflect.Map:
		// sets (NS, SS, BS)
		if flags&flagSet != 0 {
			return encodeSet(rt, flags)
		}
		// M
		return def.encodeMapM(rt, flags, info)

	// M
	case reflect.Struct:
		return def.encodeStruct(rt, flags, info)

	case reflect.Interface:
		if rt.NumMethod() == 0 {
			return def.encodeAny, nil
		}
	}
	return nil, fmt.Errorf("dynamo marshal: unsupported type %s", rt.String())
}

func (def *typedef) encodePtr(rt reflect.Type, flags encodeFlags, info *structInfo) (encodeFunc, error) {
	elem, err := def.encodeType(rt.Elem(), flags, info)
	if err != nil {
		return nil, err
	}
	return func(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
		if rv.IsNil() {
			if flags&flagNull != 0 {
				return nullAV, nil
			}
			return nil, nil
		}
		return elem(rv.Elem(), flags)
	}, nil
}

func encode2[T any](fn func(T, encodeFlags) (*dynamodb.AttributeValue, error)) func(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
	target := reflect.TypeOf((*T)(nil)).Elem()
	interfacing := target.Kind() == reflect.Interface
	return func(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
		if !rv.IsValid() || !rv.CanInterface() {
			return nil, nil
		}

		// emit null if:
		//	- T is not an interface, concrete type is (*X)(nil)
		//	- T is an interface implemented by X, but we have (*X)(nil) and calling its methods would panic
		if rv.Kind() == reflect.Pointer && rv.IsNil() && (!interfacing || rv.Type().Elem().Implements(target)) {
			if flags&flagNull != 0 {
				return nullAV, nil
			}
			return nil, nil
		}

		v := rv.Interface().(T)
		return fn(v, flags)
	}
}

func encodeString(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
	s := rv.String()
	if len(s) == 0 {
		if flags&flagAllowEmpty != 0 {
			return emptyS, nil
		}
		if flags&flagNull != 0 {
			return nullAV, nil
		}
		return nil, nil
	}
	return &dynamodb.AttributeValue{S: &s}, nil
}

var encodeTextMarshaler = encode2[encoding.TextMarshaler](func(x encoding.TextMarshaler, flags encodeFlags) (*dynamodb.AttributeValue, error) {
	text, err := x.MarshalText()
	switch {
	case err != nil:
		return nil, err
	case len(text) == 0:
		if flags&flagAllowEmpty != 0 {
			return &dynamodb.AttributeValue{S: new(string)}, nil
		}
		return nil, nil
	}
	str := string(text)
	return &dynamodb.AttributeValue{S: &str}, nil
})

func encodeBytes(rt reflect.Type, flags encodeFlags) encodeFunc {
	if rt.Kind() == reflect.Array {
		size := rt.Len()
		return func(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
			if rv.IsZero() {
				switch {
				case flags&flagNull != 0:
					return nullAV, nil
				case flags&flagAllowEmpty != 0:
					return emptyB, nil
				}
				return nil, nil
			}
			data := make([]byte, size)
			reflect.Copy(reflect.ValueOf(data), rv)
			return &dynamodb.AttributeValue{B: data}, nil
		}
	}

	return func(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
		if rv.IsNil() {
			if flags&flagNull != 0 {
				return nullAV, nil
			}
			return nil, nil
		}
		if rv.Len() == 0 {
			if flags&flagAllowEmpty != 0 {
				return emptyB, nil
			}
			return nil, nil
		}
		return &dynamodb.AttributeValue{B: rv.Bytes()}, nil
	}
}

func (def *typedef) encodeStruct(rt reflect.Type, flags encodeFlags, info *structInfo) (encodeFunc, error) {
	info2, err := def.structInfo(rt, info)
	if err != nil {
		return nil, err
	}

	var fields []structField
	for _, field := range info2.fields {
		fields = append(fields, *field)
	}

	return func(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
		item, err := encodeItem(fields, rv)
		if err != nil {
			return nil, err
		}
		return &dynamodb.AttributeValue{M: item}, nil
	}, nil
}

func encodeSliceSet(rt /* []T */ reflect.Type, flags encodeFlags) (encodeFunc, error) {
	switch {
	// SS
	case rt.Elem().Implements(rtypeTextMarshaler):
		return encodeSliceTMSS, nil
	}

	switch rt.Elem().Kind() {
	// NS
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		return encodeSliceNS((reflect.Value).Int, strconv.FormatInt), nil
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		return encodeSliceNS((reflect.Value).Uint, strconv.FormatUint), nil
	case reflect.Float64, reflect.Float32:
		return encodeSliceNS((reflect.Value).Float, formatFloat), nil

	// SS
	case reflect.String:
		return encodeSliceSS, nil

	// BS
	case reflect.Slice:
		if rt.Elem().Elem().Kind() == reflect.Uint8 {
			return encodeSliceBS, nil
		}
	}

	return nil, fmt.Errorf("dynamo: invalid type for set: %v", rt)
}

func encodeSliceTMSS(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
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

func encodeSliceSS(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
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
}

func encodeSliceBS(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
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

func (def *typedef) encodeMapM(rt reflect.Type, flags encodeFlags, info *structInfo) (encodeFunc, error) {
	keyString := encodeMapKeyFunc(rt)
	if keyString == nil {
		return nil, fmt.Errorf("dynamo marshal: map key type must be string or encoding.TextMarshaler, have %v", rt)
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

	valueEnc, err := def.encodeType(rt.Elem(), subflags, info)
	if err != nil {
		return nil, err
	}

	return func(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
		if rv.IsNil() {
			if flags&flagAllowEmpty != 0 {
				return &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{}}, nil
			}
			if flags&flagNull != 0 {
				return nullAV, nil
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
				continue
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
			if len(ss) == 0 {
				return nil, nil
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

type numberType interface {
	~int64 | ~uint64 | ~float64
}

func encodeN[T numberType](get func(reflect.Value) T, format func(T, int) string) encodeFunc {
	return func(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
		str := format(get(rv), 10)
		return &dynamodb.AttributeValue{N: &str}, nil
	}
}

func encodeSliceNS[T numberType](get func(reflect.Value) T, format func(T, int) string) encodeFunc {
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

func encodeMapNS[T numberType](truthy reflect.Value, get func(reflect.Value) T, format func(T, int) string) encodeFunc {
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

func (def *typedef) encodeList(rt reflect.Type, flags encodeFlags, info *structInfo) (encodeFunc, error) {
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

	valueEnc, err := def.encodeType(rt.Elem(), subflags, info)
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

func (def *typedef) encodeAny(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error) {
	if !rv.CanInterface() || rv.IsNil() {
		if flags&flagNull != 0 {
			return nullAV, nil
		}
		return nil, nil
	}
	enc, err := def.encodeType(rv.Elem().Type(), flags, nil)
	if err != nil {
		return nil, err
	}
	return enc(rv.Elem(), flags)
}

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
