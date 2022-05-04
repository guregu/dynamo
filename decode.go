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
	rv := reflect.ValueOf(out)
	return unmarshalReflect(av, rv)
}

// used in iterators for unmarshaling one item
type unmarshalFunc func(map[string]*dynamodb.AttributeValue, interface{}) error

var nilTum encoding.TextUnmarshaler
var tumType = reflect.TypeOf(&nilTum).Elem()

// unmarshals one value
func unmarshalReflect(av *dynamodb.AttributeValue, rv reflect.Value) error {
	// first try interface unmarshal stuff
	if rv.CanInterface() {
		var iface interface{}
		if rv.CanAddr() {
			iface = rv.Addr().Interface()
		} else {
			iface = rv.Interface()
		}

		if x, ok := iface.(*time.Time); ok && av.N != nil {
			// implicit unixtime
			// TODO(guregu): have unixtime unmarshal explicitly check struct tags
			ts, err := strconv.ParseInt(*av.N, 10, 64)
			if err != nil {
				return err
			}

			*x = time.Unix(ts, 0).UTC()
			return nil
		}

		switch x := iface.(type) {
		case *dynamodb.AttributeValue:
			*x = *av
			return nil
		case Unmarshaler:
			return x.UnmarshalDynamo(av)
		case dynamodbattribute.Unmarshaler:
			return x.UnmarshalDynamoDBAttributeValue(av)
		case encoding.TextUnmarshaler:
			if av.S != nil {
				return x.UnmarshalText([]byte(*av.S))
			}
		}
	}

	if !rv.CanSet() {
		return nil
	}

	if av.NULL != nil {
		rv.Set(reflect.Zero(rv.Type()))
		return nil
	}

	switch rv.Kind() {
	case reflect.Ptr:
		pt := reflect.New(rv.Type().Elem())
		rv.Set(pt)
		if av.NULL == nil || !(*av.NULL) {
			return unmarshalReflect(av, rv.Elem())
		}
		return nil
	case reflect.Bool:
		if av.BOOL == nil {
			return fmt.Errorf("dynamo: cannot unmarshal %s data into bool", avTypeName(av))
		}
		rv.SetBool(*av.BOOL)
		return nil
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		if av.N == nil {
			return fmt.Errorf("dynamo: cannot unmarshal %s data into int", avTypeName(av))
		}
		n, err := strconv.ParseInt(*av.N, 10, 64)
		if err != nil {
			return err
		}
		rv.SetInt(n)
		return nil
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		if av.N == nil {
			return fmt.Errorf("dynamo: cannot unmarshal %s data into uint", avTypeName(av))
		}
		n, err := strconv.ParseUint(*av.N, 10, 64)
		if err != nil {
			return err
		}
		rv.SetUint(n)
		return nil
	case reflect.Float64, reflect.Float32:
		if av.N == nil {
			return fmt.Errorf("dynamo: cannot unmarshal %s data into float", avTypeName(av))
		}
		n, err := strconv.ParseFloat(*av.N, 64)
		if err != nil {
			return err
		}
		rv.SetFloat(n)
		return nil
	case reflect.String:
		if av.S == nil {
			return fmt.Errorf("dynamo: cannot unmarshal %s data into string", avTypeName(av))
		}
		rv.SetString(*av.S)
		return nil
	case reflect.Struct:
		if av.M == nil {
			return fmt.Errorf("dynamo: cannot unmarshal %s data into struct", avTypeName(av))
		}
		if err := unmarshalItem(av.M, rv.Addr().Interface()); err != nil {
			return err
		}
		return nil
	case reflect.Map:
		if rv.IsNil() {
			// TODO: maybe always remake this?
			// I think the JSON library doesn't...
			rv.Set(reflect.MakeMap(rv.Type()))
		}

		var truthy reflect.Value
		switch {
		case rv.Type().Elem().Kind() == reflect.Bool:
			truthy = reflect.ValueOf(true)
		case rv.Type().Elem() == emptyStructType:
			fallthrough
		case rv.Type().Elem().Kind() == reflect.Struct && rv.Type().Elem().NumField() == 0:
			truthy = reflect.ValueOf(struct{}{})
		default:
			if av.M == nil {
				return fmt.Errorf("dynamo: unmarshal map set: value type must be struct{} or bool, got %v", rv.Type())
			}
		}

		switch {
		case av.M != nil:
			// TODO: this is probably slow
			kp := reflect.New(rv.Type().Key())
			kv := kp.Elem()
			for k, v := range av.M {
				innerRV := reflect.New(rv.Type().Elem())
				if err := unmarshalReflect(v, innerRV.Elem()); err != nil {
					return err
				}
				if kp.Type().Implements(tumType) {
					tm := kp.Interface().(encoding.TextUnmarshaler)
					if err := tm.UnmarshalText([]byte(k)); err != nil {
						return fmt.Errorf("dynamo: unmarshal map: key error: %v", err)
					}
				} else {
					kv.SetString(k)
				}
				rv.SetMapIndex(kv, innerRV.Elem())
			}
			return nil
		case av.SS != nil:
			kp := reflect.New(rv.Type().Key())
			kv := kp.Elem()
			for _, s := range av.SS {
				if kp.Type().Implements(tumType) {
					tm := kp.Interface().(encoding.TextUnmarshaler)
					if err := tm.UnmarshalText([]byte(*s)); err != nil {
						return fmt.Errorf("dynamo: unmarshal map (SS): key error: %v", err)
					}
				} else {
					kv.SetString(*s)
				}
				rv.SetMapIndex(kv, truthy)
			}
			return nil
		case av.NS != nil:
			kv := reflect.New(rv.Type().Key()).Elem()
			for _, n := range av.NS {
				if err := unmarshalReflect(&dynamodb.AttributeValue{N: n}, kv); err != nil {
					return err
				}
				rv.SetMapIndex(kv, truthy)
			}
			return nil
		case av.BS != nil:
			for _, bb := range av.BS {
				kv := reflect.New(rv.Type().Key()).Elem()
				reflect.Copy(kv, reflect.ValueOf(bb))
				rv.SetMapIndex(kv, truthy)
			}
			return nil
		}
		return fmt.Errorf("dynamo: cannot unmarshal %s data into map", avTypeName(av))
	case reflect.Slice:
		return unmarshalSlice(av, rv)
	case reflect.Array:
		arr := reflect.New(rv.Type()).Elem()
		elemtype := arr.Type().Elem()
		switch {
		case av.B != nil:
			if len(av.B) > arr.Len() {
				return fmt.Errorf("dynamo: cannot marshal %s into %s; too small (dst len: %d, src len: %d)", avTypeName(av), arr.Type().String(), arr.Len(), len(av.B))
			}
			reflect.Copy(arr, reflect.ValueOf(av.B))
			rv.Set(arr)
			return nil
		case av.L != nil:
			if len(av.L) > arr.Len() {
				return fmt.Errorf("dynamo: cannot marshal %s into %s; too small (dst len: %d, src len: %d)", avTypeName(av), arr.Type().String(), arr.Len(), len(av.L))
			}
			for i, innerAV := range av.L {
				innerRV := reflect.New(elemtype).Elem()
				if err := unmarshalReflect(innerAV, innerRV); err != nil {
					return err
				}
				arr.Index(i).Set(innerRV)
			}
			rv.Set(arr)
			return nil
		}
	case reflect.Interface:
		// interface{}
		if rv.NumMethod() == 0 {
			iface, err := av2iface(av)
			if err != nil {
				return err
			}
			if iface == nil {
				rv.Set(reflect.Zero(rv.Type()))
			} else {
				rv.Set(reflect.ValueOf(iface))
			}
			return nil
		}
	}

	iface := rv.Interface()
	return fmt.Errorf("dynamo: cannot unmarshal to type: %T (%+v)", iface, iface)
}

// unmarshal for when rv's Kind is Slice
func unmarshalSlice(av *dynamodb.AttributeValue, rv reflect.Value) error {
	switch {
	case av.B != nil:
		rv.SetBytes(av.B)
		return nil

	case av.L != nil:
		slicev := reflect.MakeSlice(rv.Type(), 0, len(av.L))
		for _, innerAV := range av.L {
			innerRV := reflect.New(rv.Type().Elem()).Elem()
			if err := unmarshalReflect(innerAV, innerRV); err != nil {
				return err
			}
			slicev = reflect.Append(slicev, innerRV)
		}
		rv.Set(slicev)
		return nil

	// there's probably a better way to do these
	case av.BS != nil:
		slicev := reflect.MakeSlice(rv.Type(), 0, len(av.L))
		for _, b := range av.BS {
			innerRV := reflect.New(rv.Type().Elem()).Elem()
			if err := unmarshalReflect(&dynamodb.AttributeValue{B: b}, innerRV); err != nil {
				return err
			}
			slicev = reflect.Append(slicev, innerRV)
		}
		rv.Set(slicev)
		return nil
	case av.SS != nil:
		slicev := reflect.MakeSlice(rv.Type(), 0, len(av.L))
		for _, str := range av.SS {
			innerRV := reflect.New(rv.Type().Elem()).Elem()
			if err := unmarshalReflect(&dynamodb.AttributeValue{S: str}, innerRV); err != nil {
				return err
			}
			slicev = reflect.Append(slicev, innerRV)
		}
		rv.Set(slicev)
		return nil
	case av.NS != nil:
		slicev := reflect.MakeSlice(rv.Type(), 0, len(av.L))
		for _, n := range av.NS {
			innerRV := reflect.New(rv.Type().Elem()).Elem()
			if err := unmarshalReflect(&dynamodb.AttributeValue{N: n}, innerRV); err != nil {
				return err
			}
			slicev = reflect.Append(slicev, innerRV)
		}
		rv.Set(slicev)
		return nil
	}
	return fmt.Errorf("dynamo: cannot unmarshal %s data into slice", avTypeName(av))
}

func fieldsInStruct(rv reflect.Value) map[string]reflect.Value {
	if rv.Kind() == reflect.Ptr {
		return fieldsInStruct(rv.Elem())
	}

	fields := make(map[string]reflect.Value)
	for i := 0; i < rv.Type().NumField(); i++ {
		field := rv.Type().Field(i)
		fv := rv.Field(i)
		isPtr := fv.Type().Kind() == reflect.Ptr

		name, _ := fieldInfo(field)
		if name == "-" {
			// skip
			continue
		}

		// embed anonymous structs, they could be pointers so test that too
		if (fv.Type().Kind() == reflect.Struct || isPtr && fv.Type().Elem().Kind() == reflect.Struct) && field.Anonymous {
			if isPtr {
				if fv.CanSet() {
					// set zero value for pointer
					zero := reflect.New(fv.Type().Elem())
					fv.Set(zero)
					fv = zero
				} else {
					fv = reflect.Indirect(fv)
				}
			}

			if !fv.IsValid() {
				// inaccessible
				continue
			}

			innerFields := fieldsInStruct(fv)
			for k, v := range innerFields {
				// don't clobber top-level fields
				if _, exists := fields[k]; exists {
					continue
				}
				fields[k] = v
			}
			continue
		}

		fields[name] = fv
	}
	return fields
}

// unmarshals a struct
func unmarshalItem(item map[string]*dynamodb.AttributeValue, out interface{}) error {
	switch x := out.(type) {
	case *map[string]*dynamodb.AttributeValue:
		*x = item
		return nil
	case awsEncoder:
		// special case for AWSEncoding
		return dynamodbattribute.UnmarshalMap(item, x.iface)
	case ItemUnmarshaler:
		return x.UnmarshalDynamoItem(item)
	}

	rv := reflect.ValueOf(out)
	if rv.Kind() != reflect.Ptr {
		return fmt.Errorf("dynamo: unmarshal: not a pointer: %T", out)
	}

	switch rv.Elem().Kind() {
	case reflect.Ptr:
		rv.Elem().Set(reflect.New(rv.Elem().Type().Elem()))
		return unmarshalItem(item, rv.Elem().Interface())
	case reflect.Struct:
		var err error
		fields := fieldsInStruct(rv.Elem())
		for name, fv := range fields {
			// we need to zero-out all fields to avoid weird data sticking around
			// when iterating by unmarshaling to the same object over and over
			if fv.CanSet() {
				fv.Set(reflect.Zero(fv.Type()))
			}

			if av, ok := item[name]; ok {
				if innerErr := unmarshalReflect(av, fv); innerErr != nil {
					err = innerErr
				}
			}
		}
		return err
	case reflect.Map:
		mapv := rv.Elem()
		if mapv.Type().Key().Kind() != reflect.String {
			return fmt.Errorf("dynamo: unmarshal: map key must be a string: %T", mapv.Interface())
		}
		if mapv.IsNil() {
			mapv.Set(reflect.MakeMap(mapv.Type()))
		}

		for k, av := range item {
			innerRV := reflect.New(mapv.Type().Elem()).Elem()
			if err := unmarshalReflect(av, innerRV); err != nil {
				return err
			}
			mapv.SetMapIndex(reflect.ValueOf(k), innerRV)
		}
		return nil
	}
	return fmt.Errorf("dynamo: unmarshal: unsupported type: %T", out)
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

// av2iface converts an av into interface{}.
func av2iface(av *dynamodb.AttributeValue) (interface{}, error) {
	switch {
	case av.B != nil:
		return av.B, nil
	case av.BS != nil:
		return av.BS, nil
	case av.BOOL != nil:
		return *av.BOOL, nil
	case av.N != nil:
		return strconv.ParseFloat(*av.N, 64)
	case av.S != nil:
		return *av.S, nil
	case av.L != nil:
		list := make([]interface{}, 0, len(av.L))
		for _, item := range av.L {
			iface, err := av2iface(item)
			if err != nil {
				return nil, err
			}
			list = append(list, iface)
		}
		return list, nil
	case av.NS != nil:
		set := make([]float64, 0, len(av.NS))
		for _, n := range av.NS {
			f, err := strconv.ParseFloat(*n, 64)
			if err != nil {
				return nil, err
			}
			set = append(set, f)
		}
		return set, nil
	case av.SS != nil:
		set := make([]string, 0, len(av.SS))
		for _, s := range av.SS {
			set = append(set, *s)
		}
		return set, nil
	case av.M != nil:
		m := make(map[string]interface{}, len(av.M))
		for k, v := range av.M {
			iface, err := av2iface(v)
			if err != nil {
				return nil, err
			}
			m[k] = iface
		}
		return m, nil
	case av.NULL != nil:
		return nil, nil
	}
	return nil, fmt.Errorf("dynamo: unsupported AV: %#v", *av)
}

func avTypeName(av *dynamodb.AttributeValue) string {
	if av == nil {
		return "<nil>"
	}
	switch {
	case av.B != nil:
		return "binary"
	case av.BS != nil:
		return "binary set"
	case av.BOOL != nil:
		return "boolean"
	case av.N != nil:
		return "number"
	case av.S != nil:
		return "string"
	case av.L != nil:
		return "list"
	case av.NS != nil:
		return "number set"
	case av.SS != nil:
		return "string set"
	case av.M != nil:
		return "map"
	case av.NULL != nil:
		return "null"
	}
	return "<empty>"
}
