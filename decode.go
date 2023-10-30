package dynamo

import (
	"fmt"
	"reflect"
	"strconv"

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
	plan := getDecodePlan(reflect.ValueOf(out))
	return plan.decodeAttr(flagNone, av, rv)
}

// used in iterators for unmarshaling one item
type unmarshalFunc func(map[string]*dynamodb.AttributeValue, interface{}) error

func visitFields(item map[string]*dynamodb.AttributeValue, rv reflect.Value, seen map[string]struct{}, fn func(av *dynamodb.AttributeValue, flags encodeFlags, v reflect.Value) error) error {
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

		if seen != nil {
			if _, ok := seen[name]; ok {
				continue
			}
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

func unmarshalItem(item map[string]*dynamodb.AttributeValue, out interface{}) error {
	switch out := out.(type) {
	case awsEncoder:
		return dynamodbattribute.UnmarshalMap(item, out.iface)
	}

	rv := reflect.ValueOf(out)
	if rv.Kind() != reflect.Ptr {
		return fmt.Errorf("dynamo: unmarshal: not a pointer: %T", out)
	}
	plan := getDecodePlan(reflect.ValueOf(out))
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
