package dynamo

import (
	"encoding"
	"fmt"
	"reflect"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

var planCache sync.Map // unmarshalKey → *decodePlan

type unmarshalKey struct {
	gotype reflect.Type
	shape  shapeKey
}

func (key unmarshalKey) GoString() string {
	return fmt.Sprintf("%s:%v", key.shape.GoString(), key.gotype.String())
}

func (key unmarshalKey) Less(other unmarshalKey) bool {
	if key.gotype == other.gotype {
		return key.shape < other.shape
	}
	return key.gotype.String() < other.gotype.String()
}

type typedef struct {
	decoders map[unmarshalKey]decodeFunc
	fields   []fieldMeta
}

type fieldMeta struct {
	index  []int
	name   string
	flags  encodeFlags
	enc    encodeFunc
	isZero func(reflect.Value) bool
}

func (def *typedef) analyze(rt reflect.Type) {
	for rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}

	def.learn(rt)

	if rt.Kind() != reflect.Struct {
		return
	}

	var err error
	def.fields, err = structFields(rt)
	if err != nil {
		panic(err) // TODO
	}
}

func structFields(rt reflect.Type) ([]fieldMeta, error) {
	var fields []fieldMeta
	err := visitTypeFields(rt, nil, nil, func(name string, index []int, flags encodeFlags, vt reflect.Type) error {
		enc, err := encodeType(vt, flags)
		if err != nil {
			return err
		}
		field := fieldMeta{
			index:  index,
			name:   name,
			flags:  flags,
			enc:    enc,
			isZero: isZeroFunc(vt),
		}
		// if flags&flagOmitEmpty != 0 {
		// 	field.isZero = isZeroFunc(rt)
		// }
		fields = append(fields, field)
		return nil
	})
	return fields, err
}

func newTypedef(rt reflect.Type) (*typedef, error) {
	plan := &typedef{
		decoders: make(map[unmarshalKey]decodeFunc),
	}

	plan.analyze(rt)

	return plan, nil
}

func registerTypedef(gotype reflect.Type, r *typedef) *typedef {
	plan, _ := planCache.LoadOrStore(gotype, r)
	return plan.(*typedef)
}

func typedefOf(rt reflect.Type) (*typedef, error) {
	v, ok := planCache.Load(rt)
	if ok {
		return v.(*typedef), nil
	}
	plan, err := newTypedef(rt)
	if err != nil {
		return nil, err
	}
	plan = registerTypedef(rt, plan)
	return plan, nil
}

func (plan *typedef) seen(gotype reflect.Type) bool {
	_, ok := plan.decoders[unmarshalKey{gotype: gotype, shape: '0'}]
	return ok
}

func (plan *typedef) handle(key unmarshalKey, fn decodeFunc) {
	if _, ok := plan.decoders[key]; ok {
		return
	}
	plan.decoders[key] = fn
	// debugf("handle %#v -> %s", key, runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name())
}

func (def *typedef) encodeItem(rv reflect.Value) (Item, error) {
	// out := rv
	rv = indirectPtrNoAlloc(rv)
	if shouldBypassEncodeItem(rv.Type()) {
		return def.encodeItemBypass(rv.Interface())
	}
	rv = indirectNoAlloc(rv)
	if shouldBypassEncodeItem(rv.Type()) {
		return def.encodeItemBypass(rv.Interface())
	}

	rv = indirectNoAlloc(rv)
	switch rv.Kind() {
	case reflect.Struct:
		return encodeItem(def.fields, rv)
	case reflect.Map:
		enc, err := encodeMapM(rv.Type(), flagNone)
		if err != nil {
			return nil, err
		}
		av, err := enc(rv, flagNone)
		if err != nil {
			return nil, err
		}
		return av.M, err
	}
	return encodeItem(def.fields, rv)
}

func (plan *typedef) encodeItemBypass(in any) (item map[string]*dynamodb.AttributeValue, err error) {
	switch x := in.(type) {
	case map[string]*dynamodb.AttributeValue:
		item = x
	case *map[string]*dynamodb.AttributeValue:
		if x == nil {
			return nil, fmt.Errorf("item to encode is nil")
		}
		item = *x
	case awsEncoder:
		item, err = dynamodbattribute.MarshalMap(x.iface)
	case ItemMarshaler:
		item, err = x.MarshalDynamoItem()
	}
	return
}

func (plan *typedef) decodeItem(item map[string]*dynamodb.AttributeValue, outv reflect.Value) error {
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

func (plan *typedef) decodeItemBypass(item map[string]*dynamodb.AttributeValue, out any) error {
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

func (plan *typedef) decodeAttr(flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
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
	return fmt.Errorf("dynamo: cannot unmarshal %s attribute value into type %s", avTypeName(av), rv.Type().String())
}

func (plan *typedef) decodeType(key unmarshalKey, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) (bool, error) {
	do, ok := plan.decoders[key]
	if !ok {
		return false, nil
	}
	err := do(plan, flags, av, rv)
	return true, err
}

func (plan *typedef) learn(rt reflect.Type) {
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

	plan.handle(this(shapeNULL), decodeNull)

	try := rt
	if try.Kind() != reflect.Pointer {
		try = reflect.PointerTo(try)
	}
	for {
		switch try {
		case rtypeAttr:
			plan.handle(this(shapeAny), decode2(func(dst *dynamodb.AttributeValue, src *dynamodb.AttributeValue) error {
				*dst = *src
				return nil
			}))
			return
		case rtypeTimePtr, rtypeTime:
			plan.handle(this(shapeN), decodeUnixTime)
			plan.handle(this(shapeS), decode2(func(t encoding.TextUnmarshaler, av *dynamodb.AttributeValue) error {
				return t.UnmarshalText([]byte(*av.S))
			}))
			return
		}
		switch {
		case try.Implements(rtypeUnmarshaler):
			plan.handle(this(shapeAny), decode2(func(t Unmarshaler, av *dynamodb.AttributeValue) error {
				return t.UnmarshalDynamo(av)
			}))
			return
		case try.Implements(rtypeAWSUnmarshaler):
			plan.handle(this(shapeAny), decode2(func(t dynamodbattribute.Unmarshaler, av *dynamodb.AttributeValue) error {
				return t.UnmarshalDynamoDBAttributeValue(av)
			}))
			return
		case try.Implements(rtypeTextUnmarshaler):
			plan.handle(this(shapeS), decode2(func(t encoding.TextUnmarshaler, av *dynamodb.AttributeValue) error {
				return t.UnmarshalText([]byte(*av.S))
			}))
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
		plan.handle(this(shapeAny), decodePtr)

	case reflect.Bool:
		plan.handle(this(shapeBOOL), decodeBool)

	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		plan.handle(this(shapeN), decodeInt)

	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		plan.handle(this(shapeN), decodeUint)

	case reflect.Float64, reflect.Float32:
		plan.handle(this(shapeN), decodeFloat)

	case reflect.String:
		plan.handle(this(shapeS), decodeString)

	case reflect.Struct:
		visitTypeFields(rt, nil, nil, func(_ string, _ []int, flags encodeFlags, vt reflect.Type) error {
			plan.learn(vt)
			return nil
		})
		plan.handle(this(shapeM), decodeStruct)

	case reflect.Map:
		plan.learn(rt.Key())
		plan.learn(rt.Elem())

		decodeKey := decodeMapKeyFunc(rt)
		plan.handle(this(shapeM), decodeMap(decodeKey))

		truthy := truthy(rt)
		if !truthy.IsValid() {
			bad := func(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
				return fmt.Errorf("dynamo: unmarshal map set: value type must be struct{} or bool, got %v", rt)
			}
			plan.handle(this(shapeSS), bad)
			plan.handle(this(shapeNS), bad)
			plan.handle(this(shapeBS), bad)
			return
		}

		plan.handle(this(shapeSS), decodeMapSS(decodeKey, truthy))
		plan.handle(this(shapeNS), decodeMapNS(decodeKey, truthy))
		plan.handle(this(shapeBS), decodeMapBS(decodeKey, truthy))
	case reflect.Slice:
		plan.learn(rt.Elem())
		if rt.Elem().Kind() == reflect.Uint8 {
			plan.handle(this(shapeB), decodeBytes)
		}
		/*
			else {
				plan.handle(this(shapeB), decodeSliceB)
			}
		*/
		plan.handle(this(shapeL), decodeSliceL)
		plan.handle(this(shapeBS), decodeSliceBS)
		plan.handle(this(shapeSS), decodeSliceSS)
		plan.handle(this(shapeNS), decodeSliceNS)
	case reflect.Array:
		plan.learn(rt.Elem())
		plan.handle(this(shapeB), decodeArrayB)
		plan.handle(this(shapeL), decodeArrayL)
	case reflect.Interface:
		// interface{}
		if rt.NumMethod() == 0 {
			plan.handle(this(shapeAny), decodeAny)
		}
	}
}

func shouldBypassDecodeItem(rt reflect.Type) bool {
	switch {
	case rt == rtypeItemPtr, rt == rtypeAWSBypass:
		return true
	case rt.Implements(rtypeItemUnmarshaler):
		return true
	}
	return false
}

func shouldBypassEncodeItem(rt reflect.Type) bool {
	switch rt {
	case rtypeItem, rtypeItemPtr, rtypeAWSBypass:
		return true
	}
	switch {
	case rt.Implements(rtypeItemMarshaler):
		return true
	}
	return false
}

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

var (
	nullAV = &dynamodb.AttributeValue{NULL: aws.Bool(true)}
	emptyB = &dynamodb.AttributeValue{B: []byte("")}
)
