package dynamo

import (
	"encoding"
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Unmarshaler interface {
	UnmarshalDynamo(av *dynamodb.AttributeValue) error
}

// unmarshals one value
func unmarshalReflect(av *dynamodb.AttributeValue, rv reflect.Value) error {
	// first try interface unmarshal stuff
	if rv.CanInterface() {
		var iface = rv.Interface()
		// TODO: try non-pointer types too, for compatibility
		if rv.CanAddr() {
			iface = rv.Addr().Interface()
		}
		if u, ok := iface.(Unmarshaler); ok {
			return u.UnmarshalDynamo(av)
		}
		if u, ok := iface.(encoding.TextUnmarshaler); ok {
			if av.S != nil {
				return u.UnmarshalText([]byte(*av.S))
			}
		}
	}

	switch rv.Kind() {
	case reflect.Ptr:
		pt := reflect.New(rv.Type().Elem())
		rv.Set(pt)
		if av.NULL == nil || !(*av.NULL) {
			return unmarshalReflect(av, rv.Elem())
		}
	case reflect.Bool:
		if av.BOOL == nil {
			return errors.New("dynamo: unmarshal bool: expected BOOL to be non-nil")
		}
		rv.SetBool(*av.BOOL)
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		if av.N == nil {
			return errors.New("dynamo: unmarshal int: expected N to be non-nil")
		}
		n, err := strconv.ParseInt(*av.N, 10, 64)
		if err != nil {
			return err
		}
		rv.SetInt(n)
	case reflect.String:
		if av.S == nil {
			return errors.New("dynamo: unmarshal string: expected S to be non-nil")
		}
		rv.SetString(*av.S)
	case reflect.Struct:
		if av.M == nil {
			return errors.New("dynamo: unmarshal struct: expected M to be non-nil")
		}
		if err := unmarshalItem(av.M, rv.Addr().Interface()); err != nil {
			return err
		}
	case reflect.Map:
		if av.M == nil {
			return errors.New("dynamo: unmarshal map: expected M to be non-nil")
		}

		if rv.IsNil() {
			// TODO: maybe always remake this?
			// I think the JSON library doesn't...
			rv.Set(reflect.MakeMap(rv.Type()))
		}

		// TODO: this is probably slow
		for k, v := range av.M {
			innerRV := reflect.New(rv.Type().Elem())
			if err := unmarshalReflect(v, innerRV.Elem()); err != nil {
				return err
			}
			rv.SetMapIndex(reflect.ValueOf(k), innerRV.Elem())
		}
	case reflect.Slice:
		return unmarshalSlice(av, rv)
	default:
		iface := rv.Interface()
		return fmt.Errorf("dynamo: can't unmarshal to type: %T (%+v)", iface, iface)
	}
	return nil
}

// unmarshal for when rv's Kind is Slice
func unmarshalSlice(av *dynamodb.AttributeValue, rv reflect.Value) error {
	switch {
	case av.L != nil:
		slicev := reflect.MakeSlice(rv.Type(), 0, len(av.L))
		for _, innerAV := range av.L {
			innerRV := reflect.New(rv.Type().Elem())
			if err := unmarshalReflect(innerAV, innerRV.Elem()); err != nil {
				return err
			}
			slicev = reflect.Append(slicev, innerRV.Elem())
		}
		rv.Set(slicev)

	// there's probably a better way to do these
	case av.BS != nil:
		slicev := reflect.MakeSlice(rv.Type(), 0, len(av.L))
		for _, b := range av.BS {
			innerRV := reflect.New(rv.Type().Elem())
			if err := unmarshalReflect(&dynamodb.AttributeValue{B: b}, innerRV.Elem()); err != nil {
				return err
			}
			slicev = reflect.Append(slicev, innerRV.Elem())
		}
		rv.Set(slicev)
	case av.SS != nil:
		slicev := reflect.MakeSlice(rv.Type(), 0, len(av.L))
		for _, str := range av.SS {
			innerRV := reflect.New(rv.Type().Elem())
			if err := unmarshalReflect(&dynamodb.AttributeValue{S: str}, innerRV.Elem()); err != nil {
				return err
			}
			slicev = reflect.Append(slicev, innerRV.Elem())
		}
		rv.Set(slicev)
	case av.NS != nil:
		slicev := reflect.MakeSlice(rv.Type(), 0, len(av.L))
		for _, n := range av.NS {
			innerRV := reflect.New(rv.Type().Elem())
			if err := unmarshalReflect(&dynamodb.AttributeValue{N: n}, innerRV.Elem()); err != nil {
				return err
			}
			slicev = reflect.Append(slicev, innerRV.Elem())
		}
		rv.Set(slicev)

	}
	return errors.New("dynamo: unmarshal slice: int slice but L, NS, SS are nil")
}

func fieldsInStruct(rv reflect.Value) map[string]reflect.Value {
	if rv.Kind() == reflect.Ptr {
		return fieldsInStruct(rv.Elem())
	}

	fields := make(map[string]reflect.Value)
	for i := 0; i < rv.Type().NumField(); i++ {
		field := rv.Type().Field(i)
		fv := rv.Field(i)

		name, _, _ := fieldInfo(field)
		if name == "-" {
			// skip
			continue
		}

		// embed anonymous structs
		if fv.Type().Kind() == reflect.Struct && field.Anonymous {
			innerFields := fieldsInStruct(fv)
			for k, v := range innerFields {
				fields[k] = v
			}
			continue
		}

		fields[name] = fv
	}
	return fields
}

// unmarshals a struct
// TODO: unmarshal to map[string]interface{} too
func unmarshalItem(item map[string]*dynamodb.AttributeValue, out interface{}) error {
	rv := reflect.ValueOf(out)

	if rv.Kind() != reflect.Ptr {
		return fmt.Errorf("not a pointer: %T", out)
	}

	var err error
	switch rv.Elem().Kind() {
	case reflect.Struct:
		fields := fieldsInStruct(rv.Elem())
		for name, fv := range fields {
			if av, ok := item[name]; ok {
				if innerErr := unmarshalReflect(av, fv); innerErr != nil {
					err = innerErr
				}
			}
		}
	default:
		// TODO: support map[string]interface{}
		return fmt.Errorf("dynamo: unmarshal: unsupported type: %T", out)
	}

	return err
}

// unmarshals to a slice
func unmarshalAll(items []map[string]*dynamodb.AttributeValue, out interface{}) error {
	rv := reflect.ValueOf(out)
	if rv.Kind() != reflect.Ptr || rv.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("result argument must be a slice address")
	}
	rv = rv.Elem()

	slicev := reflect.MakeSlice(rv.Type(), 0, len(items))
	for _, av := range items {
		innerRV := reflect.New(rv.Type().Elem())
		if err := unmarshalItem(av, innerRV); err != nil {
			return err
		}
		slicev = reflect.Append(slicev, innerRV.Elem())
	}
	rv.Set(slicev)
	return nil
}

func unmarshalAppend(item map[string]*dynamodb.AttributeValue, out interface{}) error {
	resultv := reflect.ValueOf(out)
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Slice {
		panic("result argument must be a slice address")
	}

	slicev := resultv.Elem()
	elemt := slicev.Type().Elem()
	elemp := reflect.New(elemt)
	if err := unmarshalItem(item, elemp.Interface()); err != nil {
		return err
	}
	slicev = reflect.Append(slicev, elemp.Elem())

	resultv.Elem().Set(slicev)
	return nil
}
