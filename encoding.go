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

var typeCache sync.Map // unmarshalKey → *typedef

type typedef struct {
	decoders map[unmarshalKey]decodeFunc
	fields   []structField
}

func newTypedef(rt reflect.Type) (*typedef, error) {
	def := &typedef{
		decoders: make(map[unmarshalKey]decodeFunc),
	}
	err := def.init(rt)
	return def, err
}

func (def *typedef) init(rt reflect.Type) error {
	for rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}

	def.learn(rt)

	if rt.Kind() != reflect.Struct {
		return nil
	}

	var err error
	def.fields, err = structFields(rt)
	return err
}

func registerTypedef(gotype reflect.Type, def *typedef) *typedef {
	canon, _ := typeCache.LoadOrStore(gotype, def)
	return canon.(*typedef)
}

func typedefOf(rt reflect.Type) (*typedef, error) {
	v, ok := typeCache.Load(rt)
	if ok {
		return v.(*typedef), nil
	}
	def, err := newTypedef(rt)
	if err != nil {
		return nil, err
	}
	def = registerTypedef(rt, def)
	return def, nil
}

func (def *typedef) seen(gotype reflect.Type) bool {
	_, ok := def.decoders[unmarshalKey{gotype: gotype, shape: shapeNULL}]
	return ok
}

func (def *typedef) handle(key unmarshalKey, fn decodeFunc) {
	if _, ok := def.decoders[key]; ok {
		return
	}
	def.decoders[key] = fn
	// debugf("handle %#v -> %s", key, runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name())
}

func (def *typedef) encodeItem(rv reflect.Value) (Item, error) {
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

func (def *typedef) encodeItemBypass(in any) (item map[string]*dynamodb.AttributeValue, err error) {
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

func (def *typedef) decodeItem(item map[string]*dynamodb.AttributeValue, outv reflect.Value) error {
	out := outv
	outv = indirectPtr(outv)
	if shouldBypassDecodeItem(outv.Type()) {
		return def.decodeItemBypass(item, outv.Interface())
	}
	outv = indirect(outv)
	if shouldBypassDecodeItem(outv.Type()) {
		return def.decodeItemBypass(item, outv.Interface())
	}

	if !outv.CanSet() {
		goto bad
	}

	// debugf("decode item: %v -> %T(%v)", item, out, out)
	switch outv.Kind() {
	case reflect.Struct:
		return decodeStruct(def, flagNone, &dynamodb.AttributeValue{M: item}, outv)
	case reflect.Map:
		return def.decodeAttr(flagNone, &dynamodb.AttributeValue{M: item}, outv)
	}

bad:
	return fmt.Errorf("dynamo: cannot unmarshal item into type %v (must be a pointer to a map or struct, or a supported interface)", out.Type())
}

func (def *typedef) decodeItemBypass(item map[string]*dynamodb.AttributeValue, out any) error {
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

func (def *typedef) decodeAttr(flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
	if !rv.IsValid() || av == nil {
		return nil
	}

	// debugf("decodeAttr: %v(%v) <- %v", rv.Type(), rv, av)

	if av.NULL != nil {
		return decodeNull(def, flags, av, rv)
	}

	rv = indirectPtr(rv)

retry:
	gotype := rv.Type()
	ok, err := def.decodeType(unmarshalKey{gotype: gotype, shape: shapeOf(av)}, flags, av, rv)
	if err != nil {
		return err
	}
	if ok {
		// debugf("lookup1 %#v -> %v", unmarshalKey{gotype: gotype, shape: shapeOf(av)}, rv)
		return nil
	}
	ok, err = def.decodeType(unmarshalKey{gotype: gotype, shape: shapeAny}, flags, av, rv)
	if err != nil {
		return err
	}
	if ok {
		// debugf("lookup2 %#v -> %v", unmarshalKey{gotype: gotype, shape: shapeAny}, rv)
		return nil
	}

	if rv.Kind() == reflect.Pointer {
		rv = indirect(rv)
		goto retry
	}

	// debugf("lookup fail %#v.", unmarshalKey{gotype: gotype, shape: shapeOf(av)})
	return fmt.Errorf("dynamo: cannot unmarshal %s attribute value into type %s", avTypeName(av), rv.Type().String())
}

func (def *typedef) decodeType(key unmarshalKey, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) (bool, error) {
	do, ok := def.decoders[key]
	if !ok {
		return false, nil
	}
	err := do(def, flags, av, rv)
	return true, err
}

func (def *typedef) learn(rt reflect.Type) {
	if def.decoders == nil {
		def.decoders = make(map[unmarshalKey]decodeFunc)
	}

	this := func(db shapeKey) unmarshalKey {
		return unmarshalKey{gotype: rt, shape: db}
	}

	switch {
	case def.seen(rt):
		return
	}

	def.handle(this(shapeNULL), decodeNull)

	try := rt
	if try.Kind() != reflect.Pointer {
		try = reflect.PointerTo(try)
	}
	for {
		switch try {
		case rtypeAttr:
			def.handle(this(shapeAny), decode2(func(dst *dynamodb.AttributeValue, src *dynamodb.AttributeValue) error {
				*dst = *src
				return nil
			}))
			return
		case rtypeTimePtr, rtypeTime:
			def.handle(this(shapeN), decodeUnixTime)
			def.handle(this(shapeS), decode2(func(t encoding.TextUnmarshaler, av *dynamodb.AttributeValue) error {
				return t.UnmarshalText([]byte(*av.S))
			}))
			return
		}
		switch {
		case try.Implements(rtypeUnmarshaler):
			def.handle(this(shapeAny), decode2(func(t Unmarshaler, av *dynamodb.AttributeValue) error {
				return t.UnmarshalDynamo(av)
			}))
			return
		case try.Implements(rtypeAWSUnmarshaler):
			def.handle(this(shapeAny), decode2(func(t dynamodbattribute.Unmarshaler, av *dynamodb.AttributeValue) error {
				return t.UnmarshalDynamoDBAttributeValue(av)
			}))
			return
		case try.Implements(rtypeTextUnmarshaler):
			def.handle(this(shapeS), decode2(func(t encoding.TextUnmarshaler, av *dynamodb.AttributeValue) error {
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
		def.learn(rt.Elem())
		def.handle(this(shapeAny), decodePtr)

	case reflect.Bool:
		def.handle(this(shapeBOOL), decodeBool)

	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		def.handle(this(shapeN), decodeInt)

	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		def.handle(this(shapeN), decodeUint)

	case reflect.Float64, reflect.Float32:
		def.handle(this(shapeN), decodeFloat)

	case reflect.String:
		def.handle(this(shapeS), decodeString)

	case reflect.Struct:
		visitTypeFields(rt, nil, nil, func(_ string, _ []int, flags encodeFlags, vt reflect.Type) error {
			def.learn(vt)
			return nil
		})
		def.handle(this(shapeM), decodeStruct)

	case reflect.Map:
		def.learn(rt.Key())
		def.learn(rt.Elem())

		decodeKey := decodeMapKeyFunc(rt)
		def.handle(this(shapeM), decodeMap(decodeKey))

		truthy := truthy(rt)
		if !truthy.IsValid() {
			bad := func(_ *typedef, _ encodeFlags, _ *dynamodb.AttributeValue, _ reflect.Value) error {
				return fmt.Errorf("dynamo: unmarshal map set: value type must be struct{} or bool, got %v", rt)
			}
			def.handle(this(shapeSS), bad)
			def.handle(this(shapeNS), bad)
			def.handle(this(shapeBS), bad)
			return
		}

		def.handle(this(shapeSS), decodeMapSS(decodeKey, truthy))
		def.handle(this(shapeNS), decodeMapNS(decodeKey, truthy))
		def.handle(this(shapeBS), decodeMapBS(decodeKey, truthy))
	case reflect.Slice:
		def.learn(rt.Elem())
		if rt.Elem().Kind() == reflect.Uint8 {
			def.handle(this(shapeB), decodeBytes)
		}
		/*
			else {
				def.handle(this(shapeB), decodeSliceB)
			}
		*/
		def.handle(this(shapeL), decodeSliceL)
		def.handle(this(shapeBS), decodeSliceBS)
		def.handle(this(shapeSS), decodeSliceSS)
		def.handle(this(shapeNS), decodeSliceNS)
	case reflect.Array:
		def.learn(rt.Elem())
		def.handle(this(shapeB), decodeArrayB)
		def.handle(this(shapeL), decodeArrayL)
	case reflect.Interface:
		// interface{}
		if rt.NumMethod() == 0 {
			def.handle(this(shapeAny), decodeAny)
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

type structField struct {
	index  []int
	name   string
	flags  encodeFlags
	enc    encodeFunc
	isZero func(reflect.Value) bool
}

func structFields(rt reflect.Type) ([]structField, error) {
	var fields []structField
	err := visitTypeFields(rt, nil, nil, func(name string, index []int, flags encodeFlags, vt reflect.Type) error {
		enc, err := encodeType(vt, flags)
		if err != nil {
			return err
		}
		field := structField{
			index:  index,
			name:   name,
			flags:  flags,
			enc:    enc,
			isZero: isZeroFunc(vt),
		}
		fields = append(fields, field)
		return nil
	})
	return fields, err
}

var (
	nullAV = &dynamodb.AttributeValue{NULL: aws.Bool(true)}
	emptyB = &dynamodb.AttributeValue{B: []byte("")}
	emptyS = &dynamodb.AttributeValue{S: new(string)}
)
