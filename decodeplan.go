package dynamo

import (
	"encoding"
	"fmt"
	"reflect"
	"sync"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

var planCache sync.Map // unmarshalKey → *decodePlan

type unmarshalKey struct {
	gotype reflect.Type
	shape  shapeKey
}

func (key unmarshalKey) GoString() string {
	return fmt.Sprintf("%s:%v", key.shape.GoString(), key.gotype)
}

type decodePlan struct {
	decoders map[unmarshalKey]decodeFunc
}

func newDecodePlan(rv reflect.Value) (*decodePlan, error) {
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
	return plan, err
}

func registerDecodePlan(gotype reflect.Type, r *decodePlan) *decodePlan {
	plan, _ := planCache.LoadOrStore(gotype, r)
	return plan.(*decodePlan)
}

func getDecodePlan(rv reflect.Value) (*decodePlan, error) {
	rt := rv.Type()
	v, ok := planCache.Load(rv.Type())
	if ok {
		return v.(*decodePlan), nil
	}
	plan, err := newDecodePlan(rv)
	if err != nil {
		return nil, err
	}
	plan = registerDecodePlan(rt, plan)
	return plan, nil
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

	if !outv.CanSet() {
		return nil
	}

	// debugf("decode item: %v -> %T(%v)", item, out, out)
	switch outv.Kind() {
	case reflect.Struct:
		return decodeStruct(plan, flagNone, &dynamodb.AttributeValue{M: item}, outv)
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
	if plan.decoders == nil {
		plan.decoders = make(map[unmarshalKey]decodeFunc)
	}

	this := func(db shapeKey) unmarshalKey {
		return unmarshalKey{gotype: rv.Type(), shape: db}
	}

	switch {
	case !rv.IsValid():
		return nil
	case plan.seen(rv.Type()):
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
		plan.handle(this('T'), decodeBool)
		plan.handle(this('_'), decodeInvalid("bool"))
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		plan.handle(this('N'), decodeInt)
		plan.handle(this('_'), decodeInvalid("int"))
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		plan.handle(this('N'), decodeUint)
		plan.handle(this('_'), decodeInvalid("uint"))
	case reflect.Float64, reflect.Float32:
		plan.handle(this('N'), decodeFloat)
		plan.handle(this('_'), decodeInvalid("float"))
	case reflect.String:
		plan.handle(this('S'), decodeString)
		plan.handle(this('_'), decodeInvalid("string"))
	case reflect.Struct:
		err := visitFields(nil, rv, nil, func(av *dynamodb.AttributeValue, flags encodeFlags, v reflect.Value) error {
			return plan.learn(v)
		})
		if err != nil {
			return err
		}
		plan.handle(this('M'), decodeStruct)
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

// const logging = false

// func debugf(format string, args ...any) {
// 	if !logging {
// 		return
// 	}
// 	fmt.Println("・", fmt.Sprintf(format, args...))
// }
