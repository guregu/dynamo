package dynamo

import (
	"encoding"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

var planCache sync.Map

// var decoderCache sync.Map // map[unmarshalKey]decodeFunc

type unmarshalKey struct {
	gotype reflect.Type
	shape  shapekey
}

func (key unmarshalKey) GoString() string {
	return fmt.Sprintf("%s:%v", key.shape.GoString(), key.gotype)
}

type decodeFunc func(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error

func registerDecodePlan(gotype reflect.Type, r *decodePlan) *decodePlan {
	plan, _ := planCache.LoadOrStore(gotype, r)
	return plan.(*decodePlan)
}

func getDecodePlan(rv reflect.Value) *decodePlan {
	rt := rv.Type()
	v, ok := planCache.Load(rv.Type())
	if ok {
		return v.(*decodePlan)
	}
	r := newDecodePlan(rv)
	r = registerDecodePlan(rt, r)
	return r
}

type decodePlan struct {
	decoders map[unmarshalKey]decodeFunc
}

func newDecodePlan(rv reflect.Value) *decodePlan {
	plan := &decodePlan{
		decoders: make(map[unmarshalKey]decodeFunc),
	}

	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			rv = reflect.Zero(rv.Type().Elem())
		} else {
			rv = rv.Elem()
		}
	}

	err := plan.learn(rv)
	if err != nil {
		panic(err) // TODO
	}
	return plan
}

func (plan *decodePlan) seen(gotype reflect.Type) bool {
	_, ok := plan.decoders[unmarshalKey{gotype: gotype, shape: '0'}]
	return ok
}

func (plan *decodePlan) handle(key unmarshalKey, fn decodeFunc) {
	if _, ok := plan.decoders[key]; ok {
		return
	}
	plan.decoders[key] = fn
	// plan.seen[key.gotype] = struct{}{}
	// debugf("handle %#v -> %s", key, runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name())
}

func (plan *decodePlan) decodeItemDirect(item map[string]*dynamodb.AttributeValue, out any) (bool, error) {
	// debugf("direct? %T(%v)", out, out)
	switch x := out.(type) {
	case *map[string]*dynamodb.AttributeValue:
		*x = item
		return true, nil
	case awsEncoder:
		return true, dynamodbattribute.UnmarshalMap(item, x.iface)
	case ItemUnmarshaler:
		// debugf("direct!! %T(%v)", out, out)
		return true, x.UnmarshalDynamoItem(item)
	}
	return false, nil
}

func (plan *decodePlan) decodeItem(item map[string]*dynamodb.AttributeValue, out any) error {
	outv := indirectPtr(reflect.ValueOf(out))
	if ok, err := plan.decodeItemDirect(item, outv.Interface()); ok {
		return err
	}
	outv = indirect(outv)
	if ok, err := plan.decodeItemDirect(item, outv.Interface()); ok {
		return err
	}

	// plan := *plan0
	// plan.fields = make(map[string]struct{})
	// outp := indirectPtr(reflect.ValueOf(out))
	// outp := reflect.ValueOf(out)
	// outv := indirect(outp)
	if !outv.CanSet() {
		return nil
	}
	// outv.SetZero()
	// outv, alt1, alt2, alt3 := indirect3[*map[string]*dynamodb.AttributeValue, awsEncoder, ItemUnmarshaler](outp)
	// switch {
	// case alt1 != nil:
	// 	*alt1 = item
	// 	return nil
	// case alt2.iface != nil:
	// 	return dynamodbattribute.UnmarshalMap(item, alt2.iface)
	// case alt3 != nil:
	// 	return alt3.UnmarshalDynamoItem(item)
	// }
	// debugf("decode item: %v -> %T(%v)", item, out, out)
	switch outv.Kind() {
	case reflect.Struct:
		err := visitFields(item, outv, nil, func(av *dynamodb.AttributeValue, flags encodeFlags, v reflect.Value) error {
			if av == nil {
				if v.CanSet() && !nullish(v) {
					v.SetZero()
				}
				return nil
			}
			return plan.decodeAttr(flags, av, v)
		})
		return err
	case reflect.Map:
		if err := plan.learn(outv); err != nil {
			return err
		}
		return plan.decodeAttr(flagNone, &dynamodb.AttributeValue{M: item}, outv)
	}
	return fmt.Errorf("dynamo: unmarshal: unsupported type: %T", out)
}

func (plan *decodePlan) decodeAttr(flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
	if !rv.IsValid() || av == nil {
		return nil
	}

	// debugf("decodeAttr: %v(%v) <- %v", rv.Type(), rv, av)

	if av.NULL != nil {
		return decodeNull(plan, flags, av, rv)
		// return nil
	}

	rv = indirectPtr(rv)

retry:
	gotype := rv.Type()
	ok, err := plan.decodeType(unmarshalKey{gotype: gotype, shape: shapeOf(av)}, flags, av, rv)
	if err != nil {
		return err
	}
	if ok {
		// debugf("lookup1 %#v -> %v", unmarshalKey{gotype: gotype, shape: shapeOf(av)}, rv)
		return nil
	}
	ok, err = plan.decodeType(unmarshalKey{gotype: gotype, shape: '_'}, flags, av, rv)
	if err != nil {
		return err
	}
	if ok {
		// debugf("lookup2 %#v -> %v", unmarshalKey{gotype: gotype, shape: '_'}, rv)
		return nil
	}

	if rv.Kind() == reflect.Pointer {
		rv = indirect(rv)
		goto retry
	}

	// debugf("lookup fail %#v.", unmarshalKey{gotype: gotype, shape: shapeOf(av)})
	return fmt.Errorf("no unmarshaler found for %v", rv.Type())
}

func (plan *decodePlan) decodeType(key unmarshalKey, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) (bool, error) {
	do, ok := plan.decoders[key]
	if !ok {
		return false, nil
	}
	err := do(plan, flags, av, rv)
	return true, err
}

func (plan *decodePlan) learn(rv reflect.Value) error {
	// if plan.seen == nil {
	// 	plan.seen = make(map[reflect.Type]struct{})
	// }
	if plan.decoders == nil {
		plan.decoders = make(map[unmarshalKey]decodeFunc)
	}
	// if plan.fields == nil {
	// 	plan.fields = make(map[string]struct{})
	// }

	// first try interface unmarshal stuff
	this := func(db shapekey) unmarshalKey {
		return unmarshalKey{gotype: rv.Type(), shape: db}
	}

	if !rv.IsValid() {
		return nil
	}

	if plan.seen(rv.Type()) {
		return nil
	}

	plan.handle(this('0'), decodeNull)

	try := rv.Type()
	if try.Kind() != reflect.Pointer {
		try = reflect.PointerTo(try)
	}
	for {
		switch try {
		case rtypeAttr:
			plan.handle(this('_'), decodeIface[*dynamodb.AttributeValue](func(dst *dynamodb.AttributeValue, src *dynamodb.AttributeValue) error {
				*dst = *src
				return nil
			}))
			return nil
		case rtypeAWSBypass:
			plan.handle(this('_'), decodeIface[awsEncoder](func(dst awsEncoder, src *dynamodb.AttributeValue) error {
				return dynamodbattribute.Unmarshal(src, dst.iface)
			}))
			return nil
		case rtypeTimePtr, rtypeTime:
			plan.handle(this('N'), decodeUnixTime)
			plan.handle(this('S'), decodeIface[encoding.TextUnmarshaler](func(t encoding.TextUnmarshaler, av *dynamodb.AttributeValue) error {
				return t.UnmarshalText([]byte(*av.S))
			}))
			return nil
		}
		switch {
		case try.Implements(rtypeUnmarshaler):
			plan.handle(this('_'), decodeIface[Unmarshaler](func(t Unmarshaler, av *dynamodb.AttributeValue) error {
				return t.UnmarshalDynamo(av)
			}))
			return nil
		case try.Implements(rtypeAWSUnmarshaler):
			plan.handle(this('_'), decodeIface[dynamodbattribute.Unmarshaler](func(t dynamodbattribute.Unmarshaler, av *dynamodb.AttributeValue) error {
				return t.UnmarshalDynamoDBAttributeValue(av)
			}))
			return nil
		case try.Implements(rtypeTextUnmarshaler):
			plan.handle(this('S'), decodeIface[encoding.TextUnmarshaler](func(t encoding.TextUnmarshaler, av *dynamodb.AttributeValue) error {
				return t.UnmarshalText([]byte(*av.S))
			}))
			plan.handle(this('_'), decodeInvalid("encoding.TextUnmarshaler"))
			return nil
		}

		if try.Kind() == reflect.Pointer {
			try = try.Elem()
			continue
		}
		break
	}

	switch rv.Kind() {
	case reflect.Ptr:
		if err := plan.learn(reflect.Zero(rv.Type().Elem())); err != nil {
			return err
		}
		plan.handle(this('_'), decodePtr)
		return nil
	case reflect.Bool:
		plan.handle(this('T'), decodeBool2)
		plan.handle(this('_'), decodeInvalid("bool"))
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		plan.handle(this('N'), decodeInt2)
		plan.handle(this('_'), decodeInvalid("int"))
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		plan.handle(this('N'), decodeUint2)
		plan.handle(this('_'), decodeInvalid("uint"))
	case reflect.Float64, reflect.Float32:
		plan.handle(this('N'), decodeFloat2)
		plan.handle(this('_'), decodeInvalid("float"))
	case reflect.String:
		plan.handle(this('S'), decodeString2)
		plan.handle(this('_'), decodeInvalid("string"))
	case reflect.Struct:
		err := visitFields(nil, rv, nil, func(av *dynamodb.AttributeValue, flags encodeFlags, v reflect.Value) error {
			return plan.learn(v)
		})
		if err != nil {
			return err
		}
		plan.handle(this('M'), func(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
			return visitFields(av.M, rv, nil, func(av *dynamodb.AttributeValue, flags encodeFlags, v reflect.Value) error {
				return plan.decodeAttr(flags, av, v)
			})
		})
	case reflect.Map:
		var truthy reflect.Value
		switch {
		case rv.Type().Elem().Kind() == reflect.Bool:
			truthy = reflect.ValueOf(true)
		case rv.Type().Elem() == emptyStructType:
			fallthrough
		case rv.Type().Elem().Kind() == reflect.Struct && rv.Type().Elem().NumField() == 0:
			truthy = reflect.ValueOf(struct{}{})
		}

		if err := plan.learn(reflect.Zero(rv.Type().Key())); err != nil {
			return err
		}
		if err := plan.learn(reflect.Zero(rv.Type().Elem())); err != nil {
			return err
		}

		decodeKey := decodeMapKeyFunc(rv)
		plan.handle(this('M'), decodeMap(decodeKey))

		if !truthy.IsValid() {
			bad := func(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
				return fmt.Errorf("dynamo: unmarshal map set: value type must be struct{} or bool, got %v", rv.Type())
			}
			plan.handle(this('s'), bad)
			plan.handle(this('n'), bad)
			plan.handle(this('b'), bad)
			return nil
		}

		plan.handle(this('s'), func(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
			if rv.IsNil() {
				if !rv.CanSet() {
					return nil
				}
				rv.Set(reflect.MakeMapWithSize(rv.Type(), rv.Len()))
			}
			kp := reflect.New(rv.Type().Key())
			for _, s := range av.SS {
				if err := decodeKey(kp, *s); err != nil {
					return err
				}
				rv.SetMapIndex(kp.Elem(), truthy)
			}
			return nil
		})
		plan.handle(this('n'), func(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
			if rv.IsNil() {
				if !rv.CanSet() {
					return nil
				}
				rv.Set(reflect.MakeMapWithSize(rv.Type(), rv.Len()))
			}
			kv := reflect.New(rv.Type().Key()).Elem()
			for _, n := range av.NS {
				if err := plan.decodeAttr(flagNone, &dynamodb.AttributeValue{N: n}, kv); err != nil {
					return err
				}
				rv.SetMapIndex(kv, truthy)
			}
			return nil
		})
		plan.handle(this('b'), func(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
			if rv.IsNil() {
				if !rv.CanSet() {
					return nil
				}
				rv.Set(reflect.MakeMapWithSize(rv.Type(), rv.Len()))
			}
			for _, bb := range av.BS {
				kv := reflect.New(rv.Type().Key()).Elem()
				reflect.Copy(kv, reflect.ValueOf(bb))
				rv.SetMapIndex(kv, truthy)
			}
			return nil
		})
		plan.handle(this('_'), decodeInvalid("map"))
	case reflect.Slice:
		if err := plan.learn(reflect.New(rv.Type().Elem())); err != nil {
			return err
		}
		plan.handle(this('B'), decodeBytes)
		plan.handle(this('L'), decodeList)
		plan.handle(this('b'), decodeSliceBS)
		plan.handle(this('s'), decodeSliceSS)
		plan.handle(this('n'), decodeSliceNS)
		plan.handle(this('_'), decodeInvalid("slice"))
	case reflect.Array:
		if err := plan.learn(reflect.Zero(rv.Type().Elem())); err != nil {
			return err
		}
		plan.handle(this('B'), decodeArrayB)
		plan.handle(this('L'), decodeArrayL)
		plan.handle(this('_'), decodeInvalid("array"))
	case reflect.Interface:
		// interface{}
		if rv.NumMethod() == 0 {
			plan.handle(this('_'), decodeAny)
		} else {
			plan.handle(this('_'), func(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
				return fmt.Errorf("cannot unmarshal to type: %v", rv.Type())
			})
		}
	}

	return nil
}

func decodeInvalid(fieldType string) func(plan *decodePlan, _ encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	return func(plan *decodePlan, _ encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
		return fmt.Errorf("dynamo: cannot unmarshal %s data into %s", avTypeName(av), fieldType)
	}
}

func decodePtr(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
	var elem reflect.Value
	if rv.IsNil() {
		if rv.CanSet() {
			elem = reflect.New(rv.Type().Elem())
			rv.Set(elem)
		} else {
			return nil
		}
	} else {
		elem = rv.Elem()
	}
	if err := plan.decodeAttr(flags, av, elem); err != nil {
		return err
	}
	return nil
}

func decodeString2(plan *decodePlan, state encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	v.SetString(*av.S)
	return nil
}

func decodeInt2(plan *decodePlan, state encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	n, err := strconv.ParseInt(*av.N, 10, 64)
	if err != nil {
		return err
	}
	v.SetInt(n)
	return nil
}

func decodeUint2(plan *decodePlan, state encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	n, err := strconv.ParseUint(*av.N, 10, 64)
	if err != nil {
		return err
	}
	v.SetUint(n)
	return nil
}

func decodeFloat2(plan *decodePlan, state encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	f, err := strconv.ParseFloat(*av.N, 64)
	if err != nil {
		return err
	}
	v.SetFloat(f)
	return nil
}

func decodeBool2(plan *decodePlan, state encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	v.SetBool(*av.BOOL)
	return nil
}

func decodeAny(plan *decodePlan, state encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	iface, err := av2iface(av)
	if err != nil {
		return err
	}
	if iface == nil {
		v.Set(reflect.Zero(v.Type()))
	} else {
		v.Set(reflect.ValueOf(iface))
	}
	return nil
}

func decodeNull(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
	if !rv.IsValid() {
		return nil
	}
	if rv.CanSet() {
		rv.SetZero()
		return nil
	}
	return nil
}

func decodeBytes(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	v.SetBytes(av.B)
	return nil
}

func decodeList(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	slicev := reflect.MakeSlice(v.Type(), len(av.L), len(av.L))
	for i, innerAV := range av.L {
		innerRV := slicev.Index(i).Addr()
		if err := plan.decodeAttr(flags, innerAV, innerRV); err != nil {
			return err
		}
		// debugf("slice[i=%d] %#v <- %v", i, slicev.Index(i).Interface(), innerAV)
	}
	v.Set(slicev)
	return nil
}

func decodeSliceBS(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	slicev := reflect.MakeSlice(v.Type(), len(av.BS), len(av.BS))
	for i, b := range av.BS {
		innerRV := slicev.Index(i).Addr()
		if err := plan.decodeAttr(flags, &dynamodb.AttributeValue{B: b}, innerRV); err != nil {
			return err
		}
	}
	v.Set(slicev)
	return nil
}

func decodeSliceSS(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	slicev := reflect.MakeSlice(v.Type(), len(av.SS), len(av.SS))
	for i, s := range av.SS {
		innerRV := slicev.Index(i).Addr()
		if err := plan.decodeAttr(flags, &dynamodb.AttributeValue{S: s}, innerRV); err != nil {
			return err
		}
	}
	v.Set(slicev)
	return nil
}

func decodeSliceNS(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	slicev := reflect.MakeSlice(v.Type(), len(av.NS), len(av.NS))
	for i, n := range av.NS {
		innerRV := slicev.Index(i).Addr()
		if err := plan.decodeAttr(flags, &dynamodb.AttributeValue{N: n}, innerRV); err != nil {
			return err
		}
	}
	v.Set(slicev)
	return nil
}

func decodeArrayB(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	if len(av.B) > v.Len() {
		return fmt.Errorf("dynamo: cannot marshal %s into %s; too small (dst len: %d, src len: %d)", avTypeName(av), v.Type().String(), v.Len(), len(av.B))
	}
	reflect.Copy(v, reflect.ValueOf(av.B))
	return nil
}

func decodeArrayL(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	if len(av.L) > v.Len() {
		return fmt.Errorf("dynamo: cannot marshal %s into %s; too small (dst len: %d, src len: %d)", avTypeName(av), v.Type().String(), v.Len(), len(av.L))
	}
	for i, innerAV := range av.L {
		if err := plan.decodeAttr(flags, innerAV, v.Index(i)); err != nil {
			return err
		}
	}
	return nil
}

func decodeUnixTime(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
	rv = indirect(rv)

	ts, err := strconv.ParseInt(*av.N, 10, 64)
	if err != nil {
		return err
	}

	rv.Set(reflect.ValueOf(time.Unix(ts, 0).UTC()))
	return nil
}

func decodeMap(decodeKey func(reflect.Value, string) error) func(plan *decodePlan, _ encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	/*
		Something like:

			if out == nil {
					out = make(map[K]V, len(item))
			}
			kp := new(K)
			for name, av := range item {
				vp := new(V)
				decodeKey(kp, name)
				decodeAttr(av, vp) // TODO fix order
				out[*kp] = *vp
			}
	*/
	return func(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
		if rv.IsNil() || rv.Len() > 0 {
			rv.Set(reflect.MakeMapWithSize(rv.Type(), len(av.M)))
		}
		kp := reflect.New(rv.Type().Key())
		for name, v := range av.M {
			if err := decodeKey(kp, name); err != nil {
				return fmt.Errorf("error decoding key %q into %v", name, kp.Type().Elem())
			}
			innerRV := reflect.New(rv.Type().Elem())
			if err := plan.decodeAttr(flags, v, innerRV.Elem()); err != nil {
				return fmt.Errorf("error decoding key %q into %v", name, kp.Type().Elem())
			}
			rv.SetMapIndex(kp.Elem(), innerRV.Elem())
		}
		return nil
	}
}

func decodeMapKeyFunc(rv reflect.Value) func(keyv reflect.Value, value string) error {
	if reflect.PtrTo(rv.Type().Key()).Implements(rtypeTextUnmarshaler) {
		return func(kv reflect.Value, s string) error {
			tm := kv.Interface().(encoding.TextUnmarshaler)
			if err := tm.UnmarshalText([]byte(s)); err != nil {
				return fmt.Errorf("dynamo: unmarshal map: key error: %w", err)
			}
			return nil
		}
	}
	return func(kv reflect.Value, s string) error {
		kv.Elem().SetString(s)
		return nil
	}
}

type shapekey byte

func (sk shapekey) String() string   { return string(rune(sk)) }
func (sk shapekey) GoString() string { return fmt.Sprintf("shapekey(%s)", sk.String()) }

func shapeOf(av *dynamodb.AttributeValue) shapekey {
	if av == nil {
		return 0
	}
	switch {
	case av.B != nil:
		return 'B'
	case av.BS != nil:
		return 'b'
	case av.BOOL != nil:
		return 'T'
	case av.N != nil:
		return 'N'
	case av.S != nil:
		return 'S'
	case av.L != nil:
		return 'L'
	case av.NS != nil:
		return 'n'
	case av.SS != nil:
		return 's'
	case av.M != nil:
		return 'M'
	case av.NULL != nil:
		return '0'
	}
	return '_'
}

func decodeIface[T any](fn func(t T, av *dynamodb.AttributeValue) error) func(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	return func(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
		if !rv.CanInterface() {
			return nil
		}
		var iface interface{}
		if rv.Kind() != reflect.Pointer && rv.CanAddr() {
			iface = rv.Addr().Interface()
		} else {
			if rv.IsNil() {
				if rv.CanSet() {
					rv.Set(reflect.New(rv.Type().Elem()))
				} else {
					return nil
				}
			}
			iface = rv.Interface()
		}
		return fn(iface.(T), av)
	}
}

func indirect(rv reflect.Value) reflect.Value {
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			if !rv.CanSet() {
				return rv
			}
			rv.Set(reflect.New(rv.Type().Elem()))
		}
		rv = rv.Elem()
	}
	return rv
}

func indirectPtr(rv reflect.Value) reflect.Value {
	// rv0 := rv
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			if !rv.CanSet() {
				return rv
			}
			rv.Set(reflect.New(rv.Type().Elem()))
		}
		if rv.Type().Elem().Kind() != reflect.Pointer {
			return rv
		}
		rv = rv.Elem()
	}
	return rv
}

var (
	rtypeAttr            = reflect.TypeOf((*dynamodb.AttributeValue)(nil))
	rtypeTimePtr         = reflect.TypeOf((*time.Time)(nil))
	rtypeTime            = reflect.TypeOf(time.Time{})
	rtypeUnmarshaler     = reflect.TypeOf((*Unmarshaler)(nil)).Elem()
	rtypeAWSBypass       = reflect.TypeOf(awsEncoder{})
	rtypeAWSUnmarshaler  = reflect.TypeOf((*dynamodbattribute.Unmarshaler)(nil)).Elem()
	rtypeTextUnmarshaler = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
)

func nullish(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		return v.IsNil()
	case reflect.Slice:
		return v.IsNil() || v.Len() == 0
	case reflect.Map:
		return v.IsNil() || v.Len() == 0
	}
	return false
}

// const logging = false

// func debugf(format string, args ...any) {
// 	if !logging {
// 		return
// 	}
// 	fmt.Println("・", fmt.Sprintf(format, args...))
// }
