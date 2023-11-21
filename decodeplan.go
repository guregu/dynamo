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

func newDecodePlan(rt reflect.Type) (*decodePlan, error) {
	plan := &decodePlan{
		decoders: make(map[unmarshalKey]decodeFunc),
	}

	for rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}

	plan.learn(rt)
	return plan, nil
}

func registerDecodePlan(gotype reflect.Type, r *decodePlan) *decodePlan {
	plan, _ := planCache.LoadOrStore(gotype, r)
	return plan.(*decodePlan)
}

func getDecodePlan(rt reflect.Type) (*decodePlan, error) {
	v, ok := planCache.Load(rt)
	if ok {
		return v.(*decodePlan), nil
	}
	plan, err := newDecodePlan(rt)
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

func (plan *decodePlan) decodeItemBypass(item map[string]*dynamodb.AttributeValue, out any) error {
	switch x := out.(type) {
	case *map[string]*dynamodb.AttributeValue:
		*x = item
		return nil
	case awsEncoder:
		return dynamodbattribute.UnmarshalMap(item, x.iface)
	case ItemUnmarshaler:
		return x.UnmarshalDynamoItem(item)
	}
	return nil
}

func (plan *decodePlan) decodeItem(item map[string]*dynamodb.AttributeValue, outv reflect.Value) error {
	out := outv
	outv = indirectPtr(outv)
	if shouldBypassDecodeItem(outv.Type()) {
		return plan.decodeItemBypass(item, outv.Interface())
	}
	outv = indirect(outv)
	if shouldBypassDecodeItem(outv.Type()) {
		return plan.decodeItemBypass(item, outv.Interface())
	}

	if !outv.CanSet() {
		goto bad
	}

	// debugf("decode item: %v -> %T(%v)", item, out, out)
	switch outv.Kind() {
	case reflect.Struct:
		return decodeStruct(plan, flagNone, &dynamodb.AttributeValue{M: item}, outv)
	case reflect.Map:
		return plan.decodeAttr(flagNone, &dynamodb.AttributeValue{M: item}, outv)
	}

bad:
	return fmt.Errorf("dynamo: cannot unmarshal item into type %v (must be a pointer to a map or struct, or a supported interface)", out.Type())
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

func (plan *decodePlan) learn(rt reflect.Type) {
	if plan.decoders == nil {
		plan.decoders = make(map[unmarshalKey]decodeFunc)
	}

	this := func(db shapeKey) unmarshalKey {
		return unmarshalKey{gotype: rt, shape: db}
	}

	switch {
	case plan.seen(rt):
		return
	}

	plan.handle(this('0'), decodeNull)

	try := rt
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
			return
		case rtypeTimePtr, rtypeTime:
			plan.handle(this('N'), decodeUnixTime)
			plan.handle(this('S'), decodeIface[encoding.TextUnmarshaler](func(t encoding.TextUnmarshaler, av *dynamodb.AttributeValue) error {
				return t.UnmarshalText([]byte(*av.S))
			}))
			return
		}
		switch {
		case try.Implements(rtypeUnmarshaler):
			plan.handle(this('_'), decodeIface[Unmarshaler](func(t Unmarshaler, av *dynamodb.AttributeValue) error {
				return t.UnmarshalDynamo(av)
			}))
			return
		case try.Implements(rtypeAWSUnmarshaler):
			plan.handle(this('_'), decodeIface[dynamodbattribute.Unmarshaler](func(t dynamodbattribute.Unmarshaler, av *dynamodb.AttributeValue) error {
				return t.UnmarshalDynamoDBAttributeValue(av)
			}))
			return
		case try.Implements(rtypeTextUnmarshaler):
			plan.handle(this('S'), decodeIface[encoding.TextUnmarshaler](func(t encoding.TextUnmarshaler, av *dynamodb.AttributeValue) error {
				return t.UnmarshalText([]byte(*av.S))
			}))
			plan.handle(this('_'), decodeInvalid("encoding.TextUnmarshaler"))
			return
		}

		if try.Kind() == reflect.Pointer {
			try = try.Elem()
			continue
		}
		break
	}

	switch rt.Kind() {
	case reflect.Ptr:
		plan.learn(rt.Elem())
		plan.handle(this('_'), decodePtr)
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
		visitTypeFields(rt, nil, func(flags encodeFlags, vt reflect.Type) error {
			plan.learn(vt)
			return nil
		})
		plan.handle(this('M'), decodeStruct)
	case reflect.Map:
		var truthy reflect.Value
		switch {
		case rt.Elem().Kind() == reflect.Bool:
			truthy = reflect.ValueOf(true)
		case rt.Elem() == emptyStructType:
			fallthrough
		case rt.Elem().Kind() == reflect.Struct && rt.Elem().NumField() == 0:
			truthy = reflect.ValueOf(struct{}{})
		}

		plan.learn(rt.Key())
		plan.learn(rt.Elem())

		decodeKey := decodeMapKeyFunc(rt)
		plan.handle(this('M'), decodeMap(decodeKey))
		plan.handle(this('_'), decodeInvalid("map"))

		if !truthy.IsValid() {
			bad := func(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
				return fmt.Errorf("dynamo: unmarshal map set: value type must be struct{} or bool, got %v", rt)
			}
			plan.handle(this('s'), bad)
			plan.handle(this('n'), bad)
			plan.handle(this('b'), bad)
			return
		}

		plan.handle(this('s'), decodeMapSS(decodeKey, truthy))
		plan.handle(this('n'), decodeMapNS(decodeKey, truthy))
		plan.handle(this('b'), decodeMapBS(decodeKey, truthy))
	case reflect.Slice:
		plan.learn(rt.Elem())
		plan.handle(this('B'), decodeBytes)
		plan.handle(this('L'), decodeList)
		plan.handle(this('b'), decodeSliceBS)
		plan.handle(this('s'), decodeSliceSS)
		plan.handle(this('n'), decodeSliceNS)
		plan.handle(this('_'), decodeInvalid("slice"))
	case reflect.Array:
		plan.learn(rt.Elem())
		plan.handle(this('B'), decodeArrayB)
		plan.handle(this('L'), decodeArrayL)
		plan.handle(this('_'), decodeInvalid("array"))
	case reflect.Interface:
		// interface{}
		if rt.NumMethod() == 0 {
			plan.handle(this('_'), decodeAny)
		} else {
			// plan.handle(this('_'), func(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
			// 	return fmt.Errorf("cannot unmarshal to type: %v", rt)
			// })
		}
	}
}

func shouldBypassDecodeItem(rt reflect.Type) bool {
	switch {
	case rt == rtypeItem, rt == rtypeAWSBypass:
		return true
	case rt.Implements(rtypeItemUnmarshaler):
		return true
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
