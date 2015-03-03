package dynamo

import (
	"fmt"
	"strconv"

	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/gen/dynamodb"
)

type Marshaler interface {
	MarshalDynamo() (dynamodb.AttributeValue, error)
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

func marshalSlice(values []interface{$}) ([]dynamodb.AttributeValue, error) {
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
