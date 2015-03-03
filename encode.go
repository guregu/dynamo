package dynamo

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/gen/dynamodb"
)

type Marshaler interface {
	MarshalDynamo() (dynamodb.AttributeValue, error)
}

func marshalStruct(v interface{}) (item map[string]dynamodb.AttributeValue, err error) {
	item = make(map[string]dynamodb.AttributeValue)
	rv := reflect.ValueOf(v)

	if rv.Type().Kind() != reflect.Struct {
		if rv.Type().Kind() == reflect.Ptr {
			return marshalStruct(rv.Elem().Interface())
		}
		return nil, fmt.Errorf("marshal struct invalid type: %T (%+v)", v, v)
	}

	for i := 0; i < rv.Type().NumField(); i++ {
		field := rv.Type().Field(i)
		fv := rv.Field(i)

		name, special := fieldName(field)
		switch {
		case name == "-":
			// skip
			continue
		case special == "omitempty":
			// skip if empty
			if isZero(fv) {
				continue
			}
		}

		av, err := marshal(fv.Interface())
		if err != nil {
			return nil, err
		}
		item[name] = av
	}
	return
}

func marshal(v interface{}) (av dynamodb.AttributeValue, err error) {
	switch x := v.(type) {
	case Marshaler:
		return x.MarshalDynamo()

	case []byte:
		av.B = x
	case [][]byte:
		av.BS = x

	case bool:
		av.BOOL = aws.Boolean(x)

	case int:
		av.N = aws.String(strconv.Itoa(x))
	case int64:
		av.N = aws.String(strconv.FormatInt(x, 10))
	case int32:
		av.N = aws.String(strconv.FormatInt(int64(x), 10))
	case int16:
		av.N = aws.String(strconv.FormatInt(int64(x), 10))
	case int8:
		av.N = aws.String(strconv.FormatInt(int64(x), 10))
	case byte:
		av.N = aws.String(strconv.FormatInt(int64(x), 10))
	case float64:
		av.N = aws.String(strconv.FormatFloat(x, 'f', -1, 64))
	case float32:
		av.N = aws.String(strconv.FormatFloat(float64(x), 'f', -1, 32))

	case nil:
		av.NULL = aws.Boolean(true)

	case string:
		av.S = aws.String(x)
	case []string:
		av.SS = x
	default:
		// TODO: use reflect
		err = fmt.Errorf("dynamo marshal: unknown type %T", v)
	}
	return
}

func marshalSlice(values []interface{}) ([]dynamodb.AttributeValue, error) {
	avs := make([]dynamodb.AttributeValue, 0, len(values))
	for _, v := range values {
		av, err := marshal(v)
		if err != nil {
			return nil, err
		}
		avs = append(avs, av)
	}
	return avs, nil
}

func fieldName(field reflect.StructField) (name, special string) {
	name = field.Tag.Get("dynamo")
	switch name {
	case "":
		// no tag, use the field name
		name = field.Name
	default:
		if idx := strings.IndexRune(name, ','); idx != -1 {
			special = name[idx+1:]
			if idx > 0 {
				name = name[:idx]
			} else {
				name = field.Name
			}
		}
	}
	return
}

// thanks James Henstridge
// TODO: tweak
// TODO: IsZero() interface support
func isZero(rv reflect.Value) bool {
	switch rv.Kind() {
	case reflect.Func, reflect.Map, reflect.Slice:
		return rv.IsNil()
	case reflect.Array:
		z := true
		for i := 0; i < rv.Len(); i++ {
			z = z && isZero(rv.Index(i))
		}
		return z
	case reflect.Struct:
		z := true
		for i := 0; i < rv.NumField(); i++ {
			z = z && isZero(rv.Field(i))
		}
		return z
	}
	// Compare other types directly:
	z := reflect.Zero(rv.Type())
	return rv.Interface() == z.Interface()
}
