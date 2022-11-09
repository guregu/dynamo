package dynamo

import (
	"encoding"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Unmarshaler is the interface implemented by objects that can unmarshal
// an AttributeValue into themselves.
type Unmarshaler interface {
	UnmarshalDynamo(av types.AttributeValue) error
}

// ItemUnmarshaler is the interface implemented by objects that can unmarshal
// an Item (a map of strings to AttributeValues) into themselves.
type ItemUnmarshaler interface {
	UnmarshalDynamoItem(item map[string]types.AttributeValue) error
}

// Unmarshal decodes a DynamoDB item into out, which must be a pointer.
func UnmarshalItem(item map[string]types.AttributeValue, out interface{}) error {
	return unmarshalItem(item, out)
}

// Unmarshal decodes a DynamoDB value into out, which must be a pointer.
func Unmarshal(av types.AttributeValue, out interface{}) error {
	rv := reflect.ValueOf(out)
	return unmarshalReflect(av, rv)
}

// used in iterators for unmarshaling one item
type unmarshalFunc func(map[string]types.AttributeValue, interface{}) error

var nilTum encoding.TextUnmarshaler
var tumType = reflect.TypeOf(&nilTum).Elem()

// unmarshals one value
func unmarshalReflect(av types.AttributeValue, rv reflect.Value) error {
	// first try interface unmarshal stuff
	if rv.CanInterface() {
		var iface interface{}
		if rv.CanAddr() {
			iface = rv.Addr().Interface()
		} else {
			iface = rv.Interface()
		}

		if x, ok := iface.(*time.Time); ok {
			if t, ok := av.(*types.AttributeValueMemberN); ok {

				// implicit unixtime
				// TODO(guregu): have unixtime unmarshal explicitly check struct tags
				ts, err := strconv.ParseInt(t.Value, 10, 64)
				if err != nil {
					return err
				}

				*x = time.Unix(ts, 0).UTC()
				return nil
			}
		}

		switch x := iface.(type) {
		case types.AttributeValue:
			switch x.(type) {
			case *types.AttributeValueMemberB:
				res := av.(*types.AttributeValueMemberB)
				*x.(*types.AttributeValueMemberB) = *res
				return nil
			case *types.AttributeValueMemberBOOL:
				res := av.(*types.AttributeValueMemberBOOL)
				*x.(*types.AttributeValueMemberBOOL) = *res
				return nil
			case *types.AttributeValueMemberBS:
				res := av.(*types.AttributeValueMemberBS)
				*x.(*types.AttributeValueMemberBS) = *res
				return nil
			case *types.AttributeValueMemberL:
				res := av.(*types.AttributeValueMemberL)
				*x.(*types.AttributeValueMemberL) = *res
				return nil
			case *types.AttributeValueMemberM:
				res := av.(*types.AttributeValueMemberM)
				*x.(*types.AttributeValueMemberM) = *res
				return nil
			case *types.AttributeValueMemberN:
				res := av.(*types.AttributeValueMemberN)
				*x.(*types.AttributeValueMemberN) = *res
				return nil
			case *types.AttributeValueMemberNS:
				res := av.(*types.AttributeValueMemberNS)
				*x.(*types.AttributeValueMemberNS) = *res
				return nil
			case *types.AttributeValueMemberNULL:
				res := av.(*types.AttributeValueMemberNULL)
				*x.(*types.AttributeValueMemberNULL) = *res
				return nil
			case *types.AttributeValueMemberS:
				res := av.(*types.AttributeValueMemberS)
				*x.(*types.AttributeValueMemberS) = *res
				return nil
			case *types.AttributeValueMemberSS:
				res := av.(*types.AttributeValueMemberSS)
				*x.(*types.AttributeValueMemberSS) = *res
				return nil
			}

		case Unmarshaler:
			return x.UnmarshalDynamo(av)
		case attributevalue.Unmarshaler:
			return x.UnmarshalDynamoDBAttributeValue(av)
		case encoding.TextUnmarshaler:
			if value, ok := av.(*types.AttributeValueMemberS); ok && value != nil {
				return x.UnmarshalText([]byte(value.Value))
			}
		}
	}

	if !rv.CanSet() {
		return nil
	}
	nullValue, valueIsNull := av.(*types.AttributeValueMemberNULL)
	if valueIsNull {
		rv.Set(reflect.Zero(rv.Type()))
		return nil
	}

	switch rv.Kind() {
	case reflect.Ptr:
		pt := reflect.New(rv.Type().Elem())
		rv.Set(pt)
		if !valueIsNull || (valueIsNull && !nullValue.Value) {
			return unmarshalReflect(av, rv.Elem())
		}
		return nil
	case reflect.Bool:
		boolValue, valueIsBool := av.(*types.AttributeValueMemberBOOL)
		if !valueIsBool {
			return fmt.Errorf("dynamo: cannot unmarshal %s data into bool", avTypeName(boolValue))
		}
		rv.SetBool(boolValue.Value)
		return nil
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		nValue, valueIsN := av.(*types.AttributeValueMemberN)
		if !valueIsN {
			return fmt.Errorf("dynamo: cannot unmarshal %s data into int", avTypeName(av))
		}
		n, err := strconv.ParseInt(nValue.Value, 10, 64)
		if err != nil {
			return err
		}
		rv.SetInt(n)
		return nil
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		nValue, valueIsN := av.(*types.AttributeValueMemberN)
		if !valueIsN {
			return fmt.Errorf("dynamo: cannot unmarshal %s data into uint", avTypeName(av))
		}
		n, err := strconv.ParseUint(nValue.Value, 10, 64)
		if err != nil {
			return err
		}
		rv.SetUint(n)
		return nil
	case reflect.Float64, reflect.Float32:
		nValue, valueIsN := av.(*types.AttributeValueMemberN)
		if !valueIsN {
			return fmt.Errorf("dynamo: cannot unmarshal %s data into float", avTypeName(av))
		}
		n, err := strconv.ParseFloat(nValue.Value, 64)
		if err != nil {
			return err
		}
		rv.SetFloat(n)
		return nil
	case reflect.String:
		sValue, valueIsS := av.(*types.AttributeValueMemberS)
		if !valueIsS {
			return fmt.Errorf("dynamo: cannot unmarshal %s data into string", avTypeName(av))
		}
		rv.SetString(sValue.Value)
		return nil
	case reflect.Struct:
		mValue, valueIsM := av.(*types.AttributeValueMemberM)
		if !valueIsM {
			return fmt.Errorf("dynamo: cannot unmarshal %s data into struct", avTypeName(av))
		}
		if err := unmarshalItem(mValue.Value, rv.Addr().Interface()); err != nil {
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
			_, valueIsM := av.(*types.AttributeValueMemberM)
			if !valueIsM {
				return fmt.Errorf("dynamo: unmarshal map set: value type must be struct{} or bool, got %v", rv.Type())
			}
		}

		switch item := av.(type) {
		case *types.AttributeValueMemberM:

			// TODO: this is probably slow
			kp := reflect.New(rv.Type().Key())
			kv := kp.Elem()
			for k, v := range item.Value {
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
		case *types.AttributeValueMemberSS:
			kp := reflect.New(rv.Type().Key())
			kv := kp.Elem()
			for _, s := range item.Value {
				if kp.Type().Implements(tumType) {
					tm := kp.Interface().(encoding.TextUnmarshaler)
					if err := tm.UnmarshalText([]byte(s)); err != nil {
						return fmt.Errorf("dynamo: unmarshal map (SS): key error: %v", err)
					}
				} else {
					kv.SetString(s)
				}
				rv.SetMapIndex(kv, truthy)
			}
			return nil
		case *types.AttributeValueMemberNS:
			kv := reflect.New(rv.Type().Key()).Elem()
			for _, n := range item.Value {
				if err := unmarshalReflect(&types.AttributeValueMemberN{Value: n}, kv); err != nil {
					return err
				}
				rv.SetMapIndex(kv, truthy)
			}
			return nil
		case *types.AttributeValueMemberBS:
			for _, bb := range item.Value {
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
		switch t := av.(type) {
		case *types.AttributeValueMemberB:
			if len(t.Value) > arr.Len() {
				return fmt.Errorf("dynamo: cannot marshal %s into %s; too small (dst len: %d, src len: %d)", avTypeName(av), arr.Type().String(), arr.Len(), len(t.Value))
			}
			reflect.Copy(arr, reflect.ValueOf(t.Value))
			rv.Set(arr)
			return nil
		case *types.AttributeValueMemberL:
			if len(t.Value) > arr.Len() {
				return fmt.Errorf("dynamo: cannot marshal %s into %s; too small (dst len: %d, src len: %d)", avTypeName(av), arr.Type().String(), arr.Len(), len(t.Value))
			}
			for i, innerAV := range t.Value {
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
func unmarshalSlice(av types.AttributeValue, rv reflect.Value) error {
	switch t := av.(type) {
	case *types.AttributeValueMemberB:
		rv.SetBytes(t.Value)
		return nil

	case *types.AttributeValueMemberL:
		slicev := reflect.MakeSlice(rv.Type(), 0, len(t.Value))
		for _, innerAV := range t.Value {
			innerRV := reflect.New(rv.Type().Elem()).Elem()
			if err := unmarshalReflect(innerAV, innerRV); err != nil {
				return err
			}
			slicev = reflect.Append(slicev, innerRV)
		}
		rv.Set(slicev)
		return nil

	// there's probably a better way to do these
	case *types.AttributeValueMemberBS:
		slicev := reflect.MakeSlice(rv.Type(), 0, len(t.Value))
		for _, b := range t.Value {
			innerRV := reflect.New(rv.Type().Elem()).Elem()
			if err := unmarshalReflect(&types.AttributeValueMemberB{Value: b}, innerRV); err != nil {
				return err
			}
			slicev = reflect.Append(slicev, innerRV)
		}
		rv.Set(slicev)
		return nil
	case *types.AttributeValueMemberSS:
		slicev := reflect.MakeSlice(rv.Type(), 0, len(t.Value))
		for _, str := range t.Value {
			innerRV := reflect.New(rv.Type().Elem()).Elem()
			if err := unmarshalReflect(&types.AttributeValueMemberS{Value: str}, innerRV); err != nil {
				return err
			}
			slicev = reflect.Append(slicev, innerRV)
		}
		rv.Set(slicev)
		return nil
	case *types.AttributeValueMemberNS:
		slicev := reflect.MakeSlice(rv.Type(), 0, len(t.Value))
		for _, n := range t.Value {
			innerRV := reflect.New(rv.Type().Elem()).Elem()
			if err := unmarshalReflect(&types.AttributeValueMemberN{Value: n}, innerRV); err != nil {
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
func unmarshalItem(item map[string]types.AttributeValue, out interface{}) error {
	switch x := out.(type) {
	case *map[string]types.AttributeValue:
		*x = item
		return nil
	case awsEncoder:
		// special case for AWSEncoding
		return attributevalue.UnmarshalMap(item, x.iface)
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

func unmarshalAppend(item map[string]types.AttributeValue, out interface{}) error {
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
func av2iface(av types.AttributeValue) (interface{}, error) {
	switch v := av.(type) {
	case *types.AttributeValueMemberB:
		return v.Value, nil
	case *types.AttributeValueMemberBS:
		return v.Value, nil
	case *types.AttributeValueMemberBOOL:
		return v.Value, nil
	case *types.AttributeValueMemberN:
		return strconv.ParseFloat(v.Value, 64)
	case *types.AttributeValueMemberS:
		return v.Value, nil
	case *types.AttributeValueMemberL:
		list := make([]interface{}, 0, len(v.Value))
		for _, item := range v.Value {
			iface, err := av2iface(item)
			if err != nil {
				return nil, err
			}
			list = append(list, iface)
		}
		return list, nil
	case *types.AttributeValueMemberNS:
		set := make([]float64, 0, len(v.Value))
		for _, n := range v.Value {
			f, err := strconv.ParseFloat(n, 64)
			if err != nil {
				return nil, err
			}
			set = append(set, f)
		}
		return set, nil
	case *types.AttributeValueMemberSS:
		set := make([]string, 0, len(v.Value))
		for _, s := range v.Value {
			set = append(set, s)
		}
		return set, nil
	case *types.AttributeValueMemberM:
		m := make(map[string]interface{}, len(v.Value))
		for k, v := range v.Value {
			iface, err := av2iface(v)
			if err != nil {
				return nil, err
			}
			m[k] = iface
		}
		return m, nil
	case *types.AttributeValueMemberNULL:
		return nil, nil
	}
	return nil, fmt.Errorf("dynamo: unsupported AV: %#v", av)
}

func avTypeName(av types.AttributeValue) string {
	return fmt.Sprintf("%T", av)
}
