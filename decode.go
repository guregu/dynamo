package dynamo

import (
	"encoding"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// Unmarshaler is the interface implemented by objects that can unmarshal
// an AttributeValue into themselves.
type Unmarshaler interface {
	UnmarshalDynamo(av *dynamodb.AttributeValue) error
}

// ItemUnmarshaler is the interface implemented by objects that can unmarshal
// an Item (a map of strings to AttributeValues) into themselves.
type ItemUnmarshaler interface {
	UnmarshalDynamoItem(item map[string]*dynamodb.AttributeValue) error
}

// Unmarshal decodes a DynamoDB item into out, which must be a pointer.
func UnmarshalItem(item map[string]*dynamodb.AttributeValue, out interface{}) error {
	return unmarshalItem(item, out)
}

// Unmarshal decodes a DynamoDB value into out, which must be a pointer.
func Unmarshal(av *dynamodb.AttributeValue, out interface{}) error {
	switch out := out.(type) {
	case awsEncoder:
		return dynamodbattribute.Unmarshal(av, out.iface)
	}

	rv := reflect.ValueOf(out)
	plan, err := getDecodePlan(rv.Type())
	if err != nil {
		return err
	}
	return plan.decodeAttr(flagNone, av, rv)
}

// used in iterators for unmarshaling one item
type unmarshalFunc func(map[string]*dynamodb.AttributeValue, interface{}) error

func unmarshalItem(item map[string]*dynamodb.AttributeValue, out interface{}) error {
	rv := reflect.ValueOf(out)
	plan, err := getDecodePlan(rv.Type())
	if err != nil {
		return err
	}
	return plan.decodeItem(item, rv)
}

func unmarshalAppend(item map[string]*dynamodb.AttributeValue, out interface{}) error {
	if awsenc, ok := out.(awsEncoder); ok {
		return unmarshalAppendAWS(item, awsenc.iface)
	}

	rv := reflect.ValueOf(out)
	if rv.Kind() != reflect.Ptr || rv.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("dynamo: unmarshal append: result argument must be a slice pointer")
	}

	slicev := rv.Elem()
	innerRV := reflect.New(slicev.Type().Elem())
	if err := unmarshalItem(item, innerRV.Interface()); err != nil {
		return err
	}
	slicev = reflect.Append(slicev, innerRV.Elem())

	rv.Elem().Set(slicev)
	return nil
}

func unmarshalAppendTo(out interface{}) func(item map[string]*dynamodb.AttributeValue, out interface{}) error {
	if awsenc, ok := out.(awsEncoder); ok {
		return func(item map[string]*dynamodb.AttributeValue, _ any) error {
			return unmarshalAppendAWS(item, awsenc.iface)
		}
	}

	ptr := reflect.ValueOf(out)
	slicet := ptr.Type().Elem()
	membert := slicet.Elem()
	if ptr.Kind() != reflect.Ptr || slicet.Kind() != reflect.Slice {
		return func(item map[string]*dynamodb.AttributeValue, _ any) error {
			return fmt.Errorf("dynamo: unmarshal append: result argument must be a slice pointer")
		}
	}

	plan, err := getDecodePlan(membert)
	if err != nil {
		return func(item map[string]*dynamodb.AttributeValue, _ any) error {
			return err
		}
	}

	/*
		Like:
			member := new(T)
			return func(item, ...) {
				decode(item, member)
				*slice = append(*slice, *member)
			}
	*/
	member := reflect.New(membert) // *T of *[]T
	return func(item map[string]*dynamodb.AttributeValue, _ any) error {
		if err := plan.decodeItem(item, member); err != nil {
			return err
		}
		slice := ptr.Elem()
		slice = reflect.Append(slice, member.Elem())
		ptr.Elem().Set(slice)
		return nil
	}
}

type decodeFunc func(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error

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

func decodeString(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	v.SetString(*av.S)
	return nil
}

func decodeInt(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	n, err := strconv.ParseInt(*av.N, 10, 64)
	if err != nil {
		return err
	}
	v.SetInt(n)
	return nil
}

func decodeUint(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	n, err := strconv.ParseUint(*av.N, 10, 64)
	if err != nil {
		return err
	}
	v.SetUint(n)
	return nil
}

func decodeFloat(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	f, err := strconv.ParseFloat(*av.N, 64)
	if err != nil {
		return err
	}
	v.SetFloat(f)
	return nil
}

func decodeBool(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	v.SetBool(*av.BOOL)
	return nil
}

func decodeBytes(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	v.SetBytes(av.B)
	return nil
}

func decodeSliceL(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	reallocSlice(v, len(av.L))
	for i, innerAV := range av.L {
		innerRV := v.Index(i).Addr()
		if err := plan.decodeAttr(flags, innerAV, innerRV); err != nil {
			return err
		}
		// debugf("slice[i=%d] %#v <- %v", i, v.Index(i).Interface(), innerAV)
	}
	return nil
}

// func decodeSliceB(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
// 	reallocSlice(v, len(av.B))
// 	for i, b := range av.B {
// 		innerB := reflect.ValueOf(b).Convert(v.Type().Elem())
// 		innerRV := v.Index(i)
// 		innerRV.Set(innerB)
// 	}
// 	return nil
// }

func decodeSliceBS(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	reallocSlice(v, len(av.BS))
	for i, b := range av.BS {
		innerRV := v.Index(i).Addr()
		if err := plan.decodeAttr(flags, &dynamodb.AttributeValue{B: b}, innerRV); err != nil {
			return err
		}
	}
	return nil
}

func decodeSliceSS(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	reallocSlice(v, len(av.SS))
	for i, s := range av.SS {
		innerRV := v.Index(i).Addr()
		if err := plan.decodeAttr(flags, &dynamodb.AttributeValue{S: s}, innerRV); err != nil {
			return err
		}
	}
	return nil
}

func decodeSliceNS(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	reallocSlice(v, len(av.NS))
	for i, n := range av.NS {
		innerRV := v.Index(i).Addr()
		if err := plan.decodeAttr(flags, &dynamodb.AttributeValue{N: n}, innerRV); err != nil {
			return err
		}
	}
	return nil
}

func decodeArrayB(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	if len(av.B) > v.Len() {
		return fmt.Errorf("dynamo: cannot marshal %s into %s; too small (dst len: %d, src len: %d)", avTypeName(av), v.Type().String(), v.Len(), len(av.B))
	}
	vt := v.Type()
	array := reflect.ValueOf(av.B)
	reflect.Copy(v, array.Convert(vt))
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

func decodeStruct(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
	return visitFields(av.M, rv, nil, func(av *dynamodb.AttributeValue, flags encodeFlags, v reflect.Value) error {
		if av == nil {
			if v.CanSet() && !nullish(v) {
				v.SetZero()
			}
			return nil
		}
		return plan.decodeAttr(flags, av, v)
	})
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
		reallocMap(rv, len(av.M))
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

type decodeKeyFunc func(reflect.Value, string) error

func decodeMapKeyFunc(rt reflect.Type) decodeKeyFunc {
	if reflect.PtrTo(rt.Key()).Implements(rtypeTextUnmarshaler) {
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

func decodeMapSS(decodeKey decodeKeyFunc, truthy reflect.Value) func(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
	return func(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
		reallocMap(rv, len(av.SS))
		kp := reflect.New(rv.Type().Key())
		for _, s := range av.SS {
			if err := decodeKey(kp, *s); err != nil {
				return err
			}
			rv.SetMapIndex(kp.Elem(), truthy)
		}
		return nil
	}
}

func decodeMapNS(decodeKey decodeKeyFunc, truthy reflect.Value) func(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
	return func(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
		reallocMap(rv, len(av.NS))
		kv := reflect.New(rv.Type().Key()).Elem()
		for _, n := range av.NS {
			if err := plan.decodeAttr(flagNone, &dynamodb.AttributeValue{N: n}, kv); err != nil {
				return err
			}
			rv.SetMapIndex(kv, truthy)
		}
		return nil
	}
}
func decodeMapBS(decodeKey decodeKeyFunc, truthy reflect.Value) func(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
	return func(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
		reallocMap(rv, len(av.BS))
		kv := reflect.New(rv.Type().Key()).Elem()
		for _, bb := range av.BS {
			reflect.Copy(kv, reflect.ValueOf(bb))
			rv.SetMapIndex(kv, truthy)
		}
		return nil
	}
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
					rv.Set(reflect.New(rv.Type()).Elem())
				} else {
					return nil
				}
			}
			iface = rv.Interface()
		}
		return fn(iface.(T), av)
	}
}

func decodeAny(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	iface, err := av2iface(av)
	if err != nil {
		return err
	}
	if iface == nil {
		v.SetZero()
	} else {
		v.Set(reflect.ValueOf(iface))
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
