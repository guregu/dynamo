package dynamo

import (
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Item is a type alias for the raw DynamoDB item type.
type Item = map[string]*dynamodb.AttributeValue

type shapeKey byte

func (sk shapeKey) String() string   { return string(rune(sk)) }
func (sk shapeKey) GoString() string { return fmt.Sprintf("shape(%s)", sk.String()) }

func shapeOf(av *dynamodb.AttributeValue) shapeKey {
	if av == nil {
		return 0
	}
	switch {
	case av.B != nil:
		return 'B'
	case av.BS != nil:
		return 'b'
	case av.BOOL != nil:
		return 'T'
	case av.N != nil:
		return 'N'
	case av.S != nil:
		return 'S'
	case av.L != nil:
		return 'L'
	case av.NS != nil:
		return 'n'
	case av.SS != nil:
		return 's'
	case av.M != nil:
		return 'M'
	case av.NULL != nil:
		return '0'
	}
	return '_'
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
