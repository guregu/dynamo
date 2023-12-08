package dynamo

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type decodeFunc func(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error

func decodePtr(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
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

func decodeNull(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
	if !rv.IsValid() {
		return nil
	}
	if rv.CanSet() {
		rv.SetZero()
		return nil
	}
	return nil
}

func decodeString(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	v.SetString(*av.S)
	return nil
}

func decodeInt(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	n, err := strconv.ParseInt(*av.N, 10, 64)
	if err != nil {
		return err
	}
	v.SetInt(n)
	return nil
}

func decodeUint(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	n, err := strconv.ParseUint(*av.N, 10, 64)
	if err != nil {
		return err
	}
	v.SetUint(n)
	return nil
}

func decodeFloat(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	f, err := strconv.ParseFloat(*av.N, 64)
	if err != nil {
		return err
	}
	v.SetFloat(f)
	return nil
}

func decodeBool(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	v.SetBool(*av.BOOL)
	return nil
}

func decodeBytes(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	v.SetBytes(av.B)
	return nil
}

func decodeSliceL(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
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

func decodeSliceBS(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	reallocSlice(v, len(av.BS))
	for i, b := range av.BS {
		innerRV := v.Index(i).Addr()
		if err := plan.decodeAttr(flags, &dynamodb.AttributeValue{B: b}, innerRV); err != nil {
			return err
		}
	}
	return nil
}

func decodeSliceSS(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	reallocSlice(v, len(av.SS))
	for i, s := range av.SS {
		innerRV := v.Index(i).Addr()
		if err := plan.decodeAttr(flags, &dynamodb.AttributeValue{S: s}, innerRV); err != nil {
			return err
		}
	}
	return nil
}

func decodeSliceNS(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	reallocSlice(v, len(av.NS))
	for i, n := range av.NS {
		innerRV := v.Index(i).Addr()
		if err := plan.decodeAttr(flags, &dynamodb.AttributeValue{N: n}, innerRV); err != nil {
			return err
		}
	}
	return nil
}

func decodeArrayB(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	if len(av.B) > v.Len() {
		return fmt.Errorf("dynamo: cannot marshal %s into %s; too small (dst len: %d, src len: %d)", avTypeName(av), v.Type().String(), v.Len(), len(av.B))
	}
	vt := v.Type()
	array := reflect.ValueOf(av.B)
	reflect.Copy(v, array.Convert(vt))
	return nil
}

func decodeArrayL(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
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

func decodeStruct(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
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

func decodeMap(decodeKey func(reflect.Value, string) error) func(plan *typedef, _ encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	/*
		Something like:

			if out == nil {
					out = make(map[K]V, len(item))
			}
			kp := new(K)
			for name, av := range item {
				vp := new(V)
				decodeKey(kp, name)
				decodeAttr(av, vp) // TODO: make argument order consistent
				out[*kp] = *vp
			}
	*/
	return func(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
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

func decodeMapSS(decodeKey decodeKeyFunc, truthy reflect.Value) func(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
	return func(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
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

func decodeMapNS(decodeKey decodeKeyFunc, truthy reflect.Value) func(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
	return func(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
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
func decodeMapBS(decodeKey decodeKeyFunc, truthy reflect.Value) func(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
	return func(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
		reallocMap(rv, len(av.BS))
		kv := reflect.New(rv.Type().Key()).Elem()
		for _, bb := range av.BS {
			reflect.Copy(kv, reflect.ValueOf(bb))
			rv.SetMapIndex(kv, truthy)
		}
		return nil
	}
}

func decode2[T any](fn func(t T, av *dynamodb.AttributeValue) error) func(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	return func(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
		if !rv.CanInterface() {
			return nil
		}
		var value interface{}
		if rv.Kind() != reflect.Pointer && rv.CanAddr() {
			value = rv.Addr().Interface()
		} else {
			if rv.IsNil() {
				if rv.CanSet() {
					rv.Set(reflect.New(rv.Type()).Elem())
				} else {
					return nil
				}
			}
			value = rv.Interface()
		}
		return fn(value.(T), av)
	}
}

func decodeAny(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
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

func decodeUnixTime(plan *typedef, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
	rv = indirect(rv)

	ts, err := strconv.ParseInt(*av.N, 10, 64)
	if err != nil {
		return err
	}

	rv.Set(reflect.ValueOf(time.Unix(ts, 0).UTC()))
	return nil
}
