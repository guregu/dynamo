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
	// n1, n2 := key.gotype.String(), other.gotype.String()
	return key.gotype.String() < other.gotype.String()
}

type decodePlan struct {
	decoders map[unmarshalKey]decodeFunc
	fields   []fieldMeta
}

type fieldMeta struct {
	index []int
	name  string
	flags encodeFlags
	enc   encodeFunc
}

type encodeFunc func(rv reflect.Value, flags encodeFlags) (*dynamodb.AttributeValue, error)

func (plan *decodePlan) analyze(rt reflect.Type) {
	for rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}

	plan.learn(rt)

	if rt.Kind() != reflect.Struct {
		return
	}

	var err error
	plan.fields, err = structFields(rt)
	if err != nil {
		panic(err) // TODO
	}
}

func structFields(rt reflect.Type) ([]fieldMeta, error) {
	var fields []fieldMeta
	err := visitTypeFields(rt, nil, nil, func(name string, index []int, flags encodeFlags, vt reflect.Type) error {
		enc, err := encoderFor(vt, flags)
		if err != nil {
			return err
		}
		field := fieldMeta{
			index: index,
			name:  name,
			flags: flags,
			enc:   enc,
		}
		fields = append(fields, field)
		return nil
	})
	return fields, err
}

func encodeItem(fields []fieldMeta, rv reflect.Value) (Item, error) {
	item := make(Item, len(fields))
	for _, field := range fields {
		fv := dig(rv, field.index)
		if !fv.IsValid() {
			// TODO: encode NULL?
			continue
		}

		if field.flags&flagOmitEmpty != 0 && isZero(fv) {
			continue
		}

		av, err := field.enc(fv, field.flags)
		if err != nil {
			return nil, err
		}
		if av == nil {
			if field.flags&flagNull != 0 {
				null := true
				item[field.name] = &dynamodb.AttributeValue{NULL: &null}
			}
			continue
		}
		item[field.name] = av
	}
	return item, nil
}

func (plan *decodePlan) encodeItem(rv reflect.Value) (Item, error) {
	// out := rv
	rv = indirectPtrNoAlloc(rv)
	if shouldBypassEncodeItem(rv.Type()) {
		return plan.encodeItemBypass(rv.Interface())
	}
	rv = indirectNoAlloc(rv)
	if shouldBypassEncodeItem(rv.Type()) {
		return plan.encodeItemBypass(rv.Interface())
	}

	rv = indirectNoAlloc(rv)
	switch rv.Kind() {
	case reflect.Struct:
		return encodeItem(plan.fields, rv)
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
	return encodeItem(plan.fields, rv)
}

func newDecodePlan(rt reflect.Type) (*decodePlan, error) {
	plan := &decodePlan{
		decoders: make(map[unmarshalKey]decodeFunc),
	}

	plan.analyze(rt)

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

func (plan *decodePlan) encodeItemBypass(in any) (item map[string]*dynamodb.AttributeValue, err error) {
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
	return fmt.Errorf("dynamo: cannot unmarshal %s attribute value into type %s", avTypeName(av), rv.Type().String())
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

	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		plan.handle(this('N'), decodeInt)

	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		plan.handle(this('N'), decodeUint)

	case reflect.Float64, reflect.Float32:
		plan.handle(this('N'), decodeFloat)

	case reflect.String:
		plan.handle(this('S'), decodeString)

	case reflect.Struct:
		visitTypeFields(rt, nil, nil, func(_ string, _ []int, flags encodeFlags, vt reflect.Type) error {
			plan.learn(vt)
			return nil
		})
		plan.handle(this('M'), decodeStruct)

	case reflect.Map:
		plan.learn(rt.Key())
		plan.learn(rt.Elem())

		decodeKey := decodeMapKeyFunc(rt)
		plan.handle(this('M'), decodeMap(decodeKey))

		truthy := truthy(rt)
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
		if rt.Elem().Kind() == reflect.Uint8 {
			plan.handle(this('B'), decodeBytes)
		}
		/*
			else {
				plan.handle(this('B'), decodeSliceB)
			}
		*/
		plan.handle(this('L'), decodeSliceL)
		plan.handle(this('b'), decodeSliceBS)
		plan.handle(this('s'), decodeSliceSS)
		plan.handle(this('n'), decodeSliceNS)
	case reflect.Array:
		plan.learn(rt.Elem())
		plan.handle(this('B'), decodeArrayB)
		plan.handle(this('L'), decodeArrayL)
	case reflect.Interface:
		// interface{}
		if rt.NumMethod() == 0 {
			plan.handle(this('_'), decodeAny)
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
