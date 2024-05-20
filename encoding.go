package dynamo

import (
	"encoding"
	"fmt"
	"reflect"
	"sync"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var typeCache sync.Map // unmarshalKey → *typedef

type typedef struct {
	decoders map[unmarshalKey]decodeFunc
	fields   []structField
	info     *structInfo
}

func newTypedef(rt reflect.Type) (*typedef, error) {
	def := &typedef{
		decoders: make(map[unmarshalKey]decodeFunc),
		// encoders: make(map[encodeKey]encodeFunc),
	}
	err := def.init(rt)
	return def, err
}

func (def *typedef) init(rt reflect.Type) error {
	rt0 := rt
	for rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}

	def.learn(rt)

	if rt.Kind() != reflect.Struct {
		return nil
	}

	// skip visiting struct fields if encoding will be bypassed by a custom marshaler
	if shouldBypassEncodeItem(rt0) || shouldBypassEncodeItem(rt) {
		return nil
	}

	info, err := def.structInfo(rt, nil)
	if err != nil {
		return err
	}
	for _, field := range info.fields {
		def.fields = append(def.fields, *field)
	}
	def.info = info
	return nil
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
		enc, err := def.encodeMapM(rv.Type(), flagNone, def.info)
		if err != nil {
			return nil, err
		}
		av, err := enc(rv, flagNone)
		if err != nil {
			return nil, err
		}
		return av.(*types.AttributeValueMemberM).Value, err
	}
	return encodeItem(def.fields, rv)
}

func (def *typedef) encodeItemBypass(in any) (item map[string]types.AttributeValue, err error) {
	switch x := in.(type) {
	case map[string]types.AttributeValue:
		item = x
	case *map[string]types.AttributeValue:
		if x == nil {
			return nil, fmt.Errorf("item to encode is nil")
		}
		item = *x
	case awsEncoder:
		item, err = attributevalue.MarshalMap(x.iface)
	case ItemMarshaler:
		item, err = x.MarshalDynamoItem()
	}
	return
}

func (def *typedef) decodeItem(item map[string]types.AttributeValue, outv reflect.Value) error {
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
		return decodeStruct(def, flagNone, &types.AttributeValueMemberM{Value: item}, outv)
	case reflect.Map:
		return def.decodeAttr(flagNone, &types.AttributeValueMemberM{Value: item}, outv)
	}

bad:
	return fmt.Errorf("dynamo: cannot unmarshal item into type %v (must be a pointer to a map or struct, or a supported interface)", out.Type())
}

func (def *typedef) decodeItemBypass(item Item, out any) error {
	switch x := out.(type) {
	case *Item:
		*x = item
		return nil
	case awsEncoder:
		return attributevalue.UnmarshalMap(item, x.iface)
	case ItemUnmarshaler:
		return x.UnmarshalDynamoItem(item)
	}
	return nil
}

func (def *typedef) decodeAttr(flags encodeFlags, av types.AttributeValue, rv reflect.Value) error {
	if !rv.IsValid() || av == nil {
		return nil
	}

	// debugf("decodeAttr: %v(%v) <- %v", rv.Type(), rv, av)

	if _, isNull := av.(*types.AttributeValueMemberNULL); isNull {
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

func (def *typedef) decodeType(key unmarshalKey, flags encodeFlags, av types.AttributeValue, rv reflect.Value) (bool, error) {
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
		case rtypeAttrB:
			def.handle(this(shapeB), decode2(func(dst *types.AttributeValueMemberB, src types.AttributeValue) error {
				*dst = *src.(*types.AttributeValueMemberB)
				return nil
			}))
		case rtypeAttrBS:
			def.handle(this(shapeBS), decode2(func(dst *types.AttributeValueMemberBS, src types.AttributeValue) error {
				*dst = *src.(*types.AttributeValueMemberBS)
				return nil
			}))
		case rtypeAttrBOOL:
			def.handle(this(shapeBOOL), decode2(func(dst *types.AttributeValueMemberBOOL, src types.AttributeValue) error {
				*dst = *src.(*types.AttributeValueMemberBOOL)
				return nil
			}))
		case rtypeAttrN:
			def.handle(this(shapeN), decode2(func(dst *types.AttributeValueMemberN, src types.AttributeValue) error {
				*dst = *src.(*types.AttributeValueMemberN)
				return nil
			}))
		case rtypeAttrS:
			def.handle(this(shapeS), decode2(func(dst *types.AttributeValueMemberS, src types.AttributeValue) error {
				*dst = *src.(*types.AttributeValueMemberS)
				return nil
			}))
		case rtypeAttrL:
			def.handle(this(shapeL), decode2(func(dst *types.AttributeValueMemberL, src types.AttributeValue) error {
				*dst = *src.(*types.AttributeValueMemberL)
				return nil
			}))
		case rtypeAttrNS:
			def.handle(this(shapeNS), decode2(func(dst *types.AttributeValueMemberNS, src types.AttributeValue) error {
				*dst = *src.(*types.AttributeValueMemberNS)
				return nil
			}))
		case rtypeAttrSS:
			def.handle(this(shapeSS), decode2(func(dst *types.AttributeValueMemberSS, src types.AttributeValue) error {
				*dst = *src.(*types.AttributeValueMemberSS)
				return nil
			}))
		case rtypeAttrM:
			def.handle(this(shapeM), decode2(func(dst *types.AttributeValueMemberM, src types.AttributeValue) error {
				*dst = *src.(*types.AttributeValueMemberM)
				return nil
			}))
		case rtypeAttrNULL:
			def.handle(this(shapeNULL), decode2(func(dst *types.AttributeValueMemberNULL, src types.AttributeValue) error {
				*dst = *src.(*types.AttributeValueMemberNULL)
				return nil
			}))

		case rtypeTimePtr, rtypeTime:
			def.handle(this(shapeN), decodeUnixTime)
			def.handle(this(shapeS), decode2(func(t encoding.TextUnmarshaler, av types.AttributeValue) error {
				return t.UnmarshalText([]byte(av.(*types.AttributeValueMemberS).Value))
			}))
			return
		}
		switch {
		// case try.Implements(rtypeAttr):
		// 	def.handle(this(shapeAny), decode2(func(dst types.AttributeValue, src types.AttributeValue) error {
		// 		*dst = src
		// 		return nil
		// 	}))
		case try.Implements(rtypeUnmarshaler):
			def.handle(this(shapeAny), decode2(func(t Unmarshaler, av types.AttributeValue) error {
				return t.UnmarshalDynamo(av)
			}))
			return
		case try.Implements(rtypeAWSUnmarshaler):
			def.handle(this(shapeAny), decode2(func(t attributevalue.Unmarshaler, av types.AttributeValue) error {
				return t.UnmarshalDynamoDBAttributeValue(av)
			}))
			return
		case try.Implements(rtypeTextUnmarshaler):
			def.handle(this(shapeS), decode2(func(t encoding.TextUnmarshaler, av types.AttributeValue) error {
				return t.UnmarshalText([]byte(av.(*types.AttributeValueMemberS).Value))
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
			bad := func(_ *typedef, _ encodeFlags, _ types.AttributeValue, _ reflect.Value) error {
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

var (
	nullAV = &types.AttributeValueMemberNULL{Value: true}
	emptyB = &types.AttributeValueMemberB{Value: []byte("")}
	emptyS = &types.AttributeValueMemberS{Value: ""}
)
