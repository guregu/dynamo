package dynamo

import (
	"encoding"
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

/*
types
B 	 []byte
BOOL bool
BS 	 [][]byte
L 	 []any
M 	 map[string]any
N 	 int64 or float64
NULL bool?
S 	 string
SS 	 []string
*/

type Unmarshaler interface {
	UnmarshalDynamo(av *dynamodb.AttributeValue) error
}

func unmarshal(av *dynamodb.AttributeValue, out interface{}) error {
	return nil
}

// unmarshals one value
func unmarshalReflect(av *dynamodb.AttributeValue, rv reflect.Value) error {
	// TODO: fix fix fix

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
		// TODO: rewrite this it sucks
		switch rv.Type().Elem().Kind() {
		// []string
		case reflect.String:
			switch {
			case av.SS != nil:
				slicev := reflect.MakeSlice(rv.Type(), len(av.SS), len(av.SS))
				// slicev := rv.Slice(0, rv.Cap())
				for i, sptr := range av.SS {
					slicev = reflectAppend(i, *sptr, slicev)
				}
				rv.Set(slicev)
			case av.L != nil:
				slicev := reflect.MakeSlice(rv.Type(), len(av.L), len(av.L))
				for i, listAV := range av.L {
					slicev = reflectAppend(i, *listAV.S, slicev)
				}
				rv.Set(slicev)
			case av.NULL != nil && *av.NULL:
				rv.Set(reflect.Zero(rv.Type()))
			default:
				return errors.New("dynamo: unmarshal slice: string slice but SS and L are nil")
			}
		// []int family
		case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
			switch {
			case av.NS != nil:
				slicev := reflect.MakeSlice(rv.Type(), len(av.NS), len(av.NS))
				for i, nptr := range av.NS {
					n, err := strconv.ParseInt(*nptr, 10, 64)
					if err != nil {
						return err
					}
					slicev = reflectAppend(i, n, slicev)
				}
				rv.Set(slicev)
			case av.L != nil:
				slicev := reflect.MakeSlice(rv.Type(), len(av.L), len(av.L))
				for i, listAV := range av.L {
					n, err := strconv.ParseInt(*listAV.N, 10, 64)
					if err != nil {
						return err
					}
					slicev = reflectAppend(i, n, slicev)
				}
				rv.Set(slicev)
			default:
				return errors.New("dynamo: unmarshal slice: int slice but NS and L are nil")
			}
		}
	default:
		iface := rv.Interface()
		return fmt.Errorf("dynamo: can't unmarshal to type: %T (%+v)", iface, iface)
	}
	return nil
}

func reflectAppend(i int, iface interface{}, slicev reflect.Value) reflect.Value {
	iv := reflect.ValueOf(iface)
	if slicev.Len() == i {
		slicev = reflect.Append(slicev, iv)
		slicev = slicev.Slice(0, slicev.Cap())
	} else {
		slicev.Index(i).Set(iv)
	}
	return slicev
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

	// fmt.Printf("unmarshal item %T %v %v\n", out, rv.Type().Kind(), rv.Kind())

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
		return fmt.Errorf("unknown type: %T", out)
	}

	return err
}

// unmarshals to a slice
func unmarshalAll(items []map[string]*dynamodb.AttributeValue, out interface{}) error {
	// cribbed from mgo
	resultv := reflect.ValueOf(out)
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Slice {
		panic("result argument must be a slice address")
	}
	slicev := resultv.Elem()
	slicev = slicev.Slice(0, slicev.Cap())
	elemt := slicev.Type().Elem()
	for i, item := range items {
		if slicev.Len() == i {
			elemp := reflect.New(elemt)
			if err := unmarshalItem(item, elemp.Interface()); err != nil {
				return err
			}
			slicev = reflect.Append(slicev, elemp.Elem())
			slicev = slicev.Slice(0, slicev.Cap())
		} else {
			if err := unmarshalItem(item, slicev.Index(i).Addr().Interface()); err != nil {
				return err
			}
		}
	}
	resultv.Elem().Set(slicev.Slice(0, len(items)))
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
