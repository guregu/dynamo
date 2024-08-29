package dynamo

import (
	"encoding"
	"fmt"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// special attribute encoders
var (
	// types.AttributeValue
	rtypeAttr     = reflect.TypeOf((*types.AttributeValue)(nil)).Elem()
	rtypeAttrB    = reflect.TypeOf((*types.AttributeValueMemberB)(nil))
	rtypeAttrBS   = reflect.TypeOf((*types.AttributeValueMemberBS)(nil))
	rtypeAttrBOOL = reflect.TypeOf((*types.AttributeValueMemberBOOL)(nil))
	rtypeAttrN    = reflect.TypeOf((*types.AttributeValueMemberN)(nil))
	rtypeAttrS    = reflect.TypeOf((*types.AttributeValueMemberS)(nil))
	rtypeAttrL    = reflect.TypeOf((*types.AttributeValueMemberL)(nil))
	rtypeAttrNS   = reflect.TypeOf((*types.AttributeValueMemberNS)(nil))
	rtypeAttrSS   = reflect.TypeOf((*types.AttributeValueMemberSS)(nil))
	rtypeAttrM    = reflect.TypeOf((*types.AttributeValueMemberM)(nil))
	rtypeAttrNULL = reflect.TypeOf((*types.AttributeValueMemberNULL)(nil))

	// *time.Time
	rtypeTimePtr = reflect.TypeOf((*time.Time)(nil))
	// time.Time
	rtypeTime = reflect.TypeOf(time.Time{})

	// Unmarshaler
	rtypeUnmarshaler = reflect.TypeOf((*Unmarshaler)(nil)).Elem()
	// dynamodbattribute.Unmarshaler
	rtypeAWSUnmarshaler = reflect.TypeOf((*attributevalue.Unmarshaler)(nil)).Elem()
	// encoding.TextUnmarshaler
	rtypeTextUnmarshaler = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()

	// Marshaler
	rtypeMarshaler = reflect.TypeOf((*Marshaler)(nil)).Elem()
	// attributevalue.Marshaler
	rtypeAWSMarshaler = reflect.TypeOf((*attributevalue.Marshaler)(nil)).Elem()
	// encoding.TextMarshaler
	rtypeTextMarshaler = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()

	// interface{ IsZero() bool } (time.Time, etc.)
	rtypeIsZeroer = reflect.TypeOf((*isZeroer)(nil)).Elem()
	// struct{}
	rtypeEmptyStruct = reflect.TypeOf(struct{}{})
)

// special item encoders
var (
	rtypeItemPtr         = reflect.TypeOf((*map[string]types.AttributeValue)(nil))
	rtypeItem            = rtypeItemPtr.Elem()
	rtypeItemUnmarshaler = reflect.TypeOf((*ItemUnmarshaler)(nil)).Elem()
	rtypeItemMarshaler   = reflect.TypeOf((*ItemMarshaler)(nil)).Elem()
	rtypeAWSBypass       = reflect.TypeOf(awsEncoder{})
)

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

func indirectNoAlloc(rv reflect.Value) reflect.Value {
	if !rv.IsValid() {
		return rv
	}
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return reflect.Value{}
		}
		rv = rv.Elem()
	}
	return rv
}

func indirectPtrNoAlloc(rv reflect.Value) reflect.Value {
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return rv
		}
		if rv.Type().Elem().Kind() != reflect.Pointer {
			return rv
		}
		rv = rv.Elem()
	}
	return rv
}

func dig(rv reflect.Value, index []int) reflect.Value {
	rv = indirectNoAlloc(rv)
	for i, idx := range index {
		if !rv.IsValid() {
			break
		}
		if i == len(index)-1 {
			rv = indirectPtrNoAlloc(rv.Field(idx))
		} else {
			rv = indirectNoAlloc(rv.Field(idx))
		}
	}
	return rv
}

func visitFields(item map[string]types.AttributeValue, rv reflect.Value, seen map[string]struct{}, fn func(av types.AttributeValue, flags encodeFlags, v reflect.Value) error) error {
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			if !rv.CanSet() {
				return nil
			}
			rv.Set(reflect.New(rv.Type().Elem()))
		}
		rv = rv.Elem()
	}

	if rv.Kind() != reflect.Struct {
		panic("not a struct")
	}

	if seen == nil {
		seen = make(map[string]struct{})
	}

	// fields := make(map[string]reflect.Value)
	for i := 0; i < rv.Type().NumField(); i++ {
		field := rv.Type().Field(i)
		fv := rv.Field(i)
		isPtr := fv.Type().Kind() == reflect.Ptr

		name, flags := fieldInfo(field)
		if name == "-" {
			// skip
			continue
		}

		if _, ok := seen[name]; ok {
			continue
		}

		// embed anonymous structs, they could be pointers so test that too
		if (fv.Type().Kind() == reflect.Struct || isPtr && fv.Type().Elem().Kind() == reflect.Struct) && field.Anonymous {
			if isPtr {
				fv = indirect(fv)
			}

			if !fv.IsValid() {
				// inaccessible
				continue
			}

			if err := visitFields(item, fv, seen, fn); err != nil {
				return err
			}
			continue
		}

		if !field.IsExported() {
			continue
		}

		if seen != nil {
			seen[name] = struct{}{}
		}
		av := item[name] // might be nil
		// debugf("visit: %s --> %s[%s](%v, %v, %v)", name, runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name(), field.Type, av, flags, fv)
		if err := fn(av, flags, fv); err != nil {
			return err
		}
	}
	return nil
}

type encodeKey struct {
	rt    reflect.Type
	flags encodeFlags
}

type structInfo struct {
	root   reflect.Type
	parent *structInfo
	fields map[string]*structField // by name
	refs   map[encodeKey][]*structField
	types  map[encodeKey]encodeFunc
	zeros  map[reflect.Type]func(reflect.Value) bool

	seen  map[encodeKey]struct{}
	queue []encodeKey
}

func (info *structInfo) encode(rv reflect.Value, flags encodeFlags) (types.AttributeValue, error) {
	item := make(Item, len(info.fields))
	for _, field := range info.fields {
		fv := dig(rv, field.index)
		if !fv.IsValid() {
			// TODO: encode NULL?
			continue
		}

		if field.flags&flagOmitEmpty != 0 && field.isZero != nil {
			if field.isZero(fv) {
				continue
			}
		}

		av, err := field.enc(fv, field.flags)
		if err != nil {
			return nil, err
		}
		if av == nil {
			if field.flags&flagNull != 0 {
				item[field.name] = nullAV
			}
			continue
		}
		item[field.name] = av
	}
	return &types.AttributeValueMemberM{Value: item}, nil
}

func (info *structInfo) isZero(rv reflect.Value) bool {
	if info == nil {
		return false
	}
	for _, field := range info.fields {
		fv := dig(rv, field.index)
		if !fv.IsValid() /* field doesn't exist */ {
			continue
		}
		if field.isZero == nil {
			// TODO: https://github.com/guregu/dynamo/issues/247
			// need to give child structs an isZero
			continue
		}
		if !field.isZero(fv) {
			return false
		}
	}
	return true
}

func (info *structInfo) findEncoder(key encodeKey) encodeFunc {
	if info == nil {
		return nil
	}
	if key.rt == info.root {
		return info.encode
	}
	if enc, ok := info.types[key]; ok {
		return enc
	}
	return info.parent.findEncoder(key)
}

func (info *structInfo) findZero(rt reflect.Type) func(reflect.Value) bool {
	if info == nil {
		return nil
	}
	if rt == info.root {
		return info.isZero
	}
	if isZero, ok := info.zeros[rt]; ok {
		return isZero
	}
	return info.parent.findZero(rt)
}

func (def *typedef) structInfo(rt reflect.Type, parent *structInfo) (*structInfo, error) {
	rti := rt
	for rti.Kind() == reflect.Pointer {
		rti = rti.Elem()
	}
	if rti.Kind() != reflect.Struct {
		return nil, nil
	}

	info := &structInfo{
		root:   rt,
		parent: parent,
		fields: make(map[string]*structField),
		refs:   make(map[encodeKey][]*structField),
		types:  make(map[encodeKey]encodeFunc),
		zeros:  make(map[reflect.Type]func(reflect.Value) bool),
		seen:   make(map[encodeKey]struct{}),
	}

	collectTypes(rt, info, nil)

	for _, key := range info.queue {
		fn, err := def.encodeType(key.rt, key.flags, info)
		if err != nil {
			return info, err
		}
		isZero := info.findZero(key.rt)
		if isZero == nil {
			isZero = def.isZeroFunc(key.rt)
		}
		for _, sf := range info.refs[key] {
			sf.enc = fn
			sf.isZero = isZero
		}
		info.types[key] = fn
		info.zeros[key.rt] = isZero
	}

	// don't need these anymore
	info.queue = nil
	info.seen = nil

	return info, nil
}

func collectTypes(rt reflect.Type, info *structInfo, trail []int) *structInfo {
	for rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	if rt.Kind() != reflect.Struct {
		panic("not a struct")
	}

	// fields := make(map[string]reflect.Value)
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		ft := field.Type
		isPtr := ft.Kind() == reflect.Ptr

		name, flags := fieldInfo(field)
		if name == "-" {
			// skip
			continue
		}

		key := encodeKey{
			rt:    ft,
			flags: flags,
		}

		idx := field.Index
		if len(trail) > 0 {
			idx = append(trail, idx...)
		}

		sf := &structField{
			index: idx,
			name:  name,
			flags: flags,
		}
		public := field.IsExported()
		if _, ok := info.fields[name]; !ok {
			if public {
				info.fields[name] = sf
			}
			info.refs[key] = append(info.refs[key], sf)
		}

		// embed anonymous structs, they could be pointers so test that too
		if (ft.Kind() == reflect.Struct || isPtr && ft.Elem().Kind() == reflect.Struct) && field.Anonymous {
			collectTypes(ft, info, idx)
			continue
		}

		if !public {
			continue
		}
		if _, ok := info.seen[key]; ok {
			continue
		}
		info.queue = append(info.queue, key)
	}
	return info
}

func visitTypeFields(rt reflect.Type, seen map[string]struct{}, trail []int, fn func(name string, index []int, flags encodeFlags, vt reflect.Type) error) error {
	for rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	if rt.Kind() != reflect.Struct {
		panic("not a struct")
	}

	if seen == nil {
		seen = make(map[string]struct{})
	}

	// fields := make(map[string]reflect.Value)
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		ft := field.Type
		isPtr := ft.Kind() == reflect.Ptr

		name, flags := fieldInfo(field)
		if name == "-" {
			// skip
			continue
		}

		if _, ok := seen[name]; ok {
			continue
		}

		// embed anonymous structs, they could be pointers so test that too
		if (ft.Kind() == reflect.Struct || isPtr && ft.Elem().Kind() == reflect.Struct) && field.Anonymous {
			index := field.Index
			if len(trail) > 0 {
				index = append(trail, field.Index...)
			}
			if err := visitTypeFields(ft, seen, index, fn); err != nil {
				return err
			}
			continue
		}

		if !field.IsExported() {
			continue
		}

		if seen != nil {
			seen[name] = struct{}{}
		}
		index := field.Index
		if len(trail) > 0 {
			index = append(trail, field.Index...)
		}
		if err := fn(name, index, flags, ft); err != nil {
			return err
		}
	}
	return nil
}

func reallocSlice(v reflect.Value, size int) {
	v.Set(reflect.MakeSlice(v.Type(), size, size))
}

func reallocMap(v reflect.Value, size int) {
	v.Set(reflect.MakeMapWithSize(v.Type(), size))
}

type decodeKeyFunc func(reflect.Value, string) error

func decodeMapKeyFunc(rt reflect.Type) decodeKeyFunc {
	if reflect.PointerTo(rt.Key()).Implements(rtypeTextUnmarshaler) {
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

type encodeKeyFunc func(k reflect.Value) (string, error)

func encodeMapKeyFunc(rt reflect.Type) encodeKeyFunc {
	keyt := rt.Key()
	if keyt.Implements(rtypeTextMarshaler) {
		return func(rv reflect.Value) (string, error) {
			tm := rv.Interface().(encoding.TextMarshaler)
			txt, err := tm.MarshalText()
			if err != nil {
				return "", fmt.Errorf("dynamo: marshal map: key error: %v", err)
			}
			return string(txt), nil
		}
	}
	if keyt.Kind() == reflect.String {
		return func(rv reflect.Value) (string, error) {
			return rv.String(), nil
		}
	}
	return nil
}

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

func emptylike(rt reflect.Type) bool {
	if rt == rtypeEmptyStruct {
		return true
	}
	return rt.Kind() == reflect.Struct && rt.NumField() == 0
}

func truthy(rt reflect.Type) reflect.Value {
	elemt := rt.Elem()
	switch {
	case elemt.Kind() == reflect.Bool:
		return reflect.ValueOf(true).Convert(elemt)
	case emptylike(elemt):
		return reflect.ValueOf(struct{}{}).Convert(elemt)
	}
	return reflect.Value{}
}

// func deref(rv reflect.Value, depth int) reflect.Value {
// 	switch {
// 	case depth < 0:
// 		for i := 0; i >= depth; i-- {
// 			if !rv.CanAddr() {
// 				return rv
// 			}
// 			rv = rv.Addr()
// 		}
// 		return rv
// 	case depth > 0:
// 		for i := 0; i < depth; i++ {
// 			if !rv.IsValid() || rv.IsNil() {
// 				return rv
// 			}
// 			rv = rv.Elem()
// 		}
// 	}
// 	return rv
// }
