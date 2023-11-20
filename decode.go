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
	plan, err := getDecodePlan(reflect.ValueOf(out))
	if err != nil {
		return err
	}
	return plan.decodeAttr(flagNone, av, rv)
}

// used in iterators for unmarshaling one item
type unmarshalFunc func(map[string]*dynamodb.AttributeValue, interface{}) error

func unmarshalItem(item map[string]*dynamodb.AttributeValue, out interface{}) error {
	switch out := out.(type) {
	case awsEncoder:
		return dynamodbattribute.UnmarshalMap(item, out.iface)
	}

	rv := reflect.ValueOf(out)
	if rv.Kind() != reflect.Ptr {
		return fmt.Errorf("dynamo: unmarshal: not a pointer: %T", out)
	}
	plan, err := getDecodePlan(reflect.ValueOf(out))
	if err != nil {
		return err
	}
	return plan.decodeItem(item, rv.Interface())
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

type decodeFunc func(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error

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

func decodeString(plan *decodePlan, state encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	v.SetString(*av.S)
	return nil
}

func decodeInt(plan *decodePlan, state encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	n, err := strconv.ParseInt(*av.N, 10, 64)
	if err != nil {
		return err
	}
	v.SetInt(n)
	return nil
}

func decodeUint(plan *decodePlan, state encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	n, err := strconv.ParseUint(*av.N, 10, 64)
	if err != nil {
		return err
	}
	v.SetUint(n)
	return nil
}

func decodeFloat(plan *decodePlan, state encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
	f, err := strconv.ParseFloat(*av.N, 64)
	if err != nil {
		return err
	}
	v.SetFloat(f)
	return nil
}

func decodeBool(plan *decodePlan, state encodeFlags, av *dynamodb.AttributeValue, v reflect.Value) error {
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

func decodeStruct(plan *decodePlan, flags encodeFlags, av *dynamodb.AttributeValue, rv reflect.Value) error {
	err := visitFields(av.M, rv, nil, func(av *dynamodb.AttributeValue, flags encodeFlags, v reflect.Value) error {
		if av == nil {
			if v.CanSet() && !nullish(v) {
				v.SetZero()
			}
			return nil
		}
		return plan.decodeAttr(flags, av, v)
	})
	return err
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
