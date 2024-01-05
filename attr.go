package dynamo

import (
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Item is a type alias for the raw DynamoDB item type.
type Item = map[string]types.AttributeValue

type shapeKey byte

func (sk shapeKey) String() string   { return string(rune(sk)) }
func (sk shapeKey) GoString() string { return fmt.Sprintf("shape(%s)", sk.String()) }

const (
	shapeB    shapeKey = 'B'
	shapeBOOL shapeKey = 'T'
	shapeN    shapeKey = 'N'
	shapeS    shapeKey = 'S'
	shapeL    shapeKey = 'L'
	shapeM    shapeKey = 'M'
	shapeBS   shapeKey = 'b'
	shapeNS   shapeKey = 'n'
	shapeSS   shapeKey = 's'
	shapeNULL shapeKey = '0'

	shapeAny     shapeKey = '_'
	shapeInvalid shapeKey = 0
)

func shapeOf(av types.AttributeValue) shapeKey {
	if av == nil {
		return shapeInvalid
	}
	switch av.(type) {
	case *types.AttributeValueMemberB:
		return shapeB
	case *types.AttributeValueMemberBS:
		return shapeBS
	case *types.AttributeValueMemberBOOL:
		return shapeBOOL
	case *types.AttributeValueMemberN:
		return shapeN
	case *types.AttributeValueMemberS:
		return shapeS
	case *types.AttributeValueMemberL:
		return shapeL
	case *types.AttributeValueMemberNS:
		return shapeNS
	case *types.AttributeValueMemberSS:
		return shapeSS
	case *types.AttributeValueMemberM:
		return shapeM
	case *types.AttributeValueMemberNULL:
		return shapeNULL
	}
	return shapeAny
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
		return v.Value, nil
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
	if av == nil {
		return "<nil>"
	}
	switch av.(type) {
	case *types.AttributeValueMemberB:
		return "binary"
	case *types.AttributeValueMemberBS:
		return "binary set"
	case *types.AttributeValueMemberBOOL:
		return "boolean"
	case *types.AttributeValueMemberN:
		return "number"
	case *types.AttributeValueMemberS:
		return "string"
	case *types.AttributeValueMemberL:
		return "list"
	case *types.AttributeValueMemberNS:
		return "number set"
	case *types.AttributeValueMemberSS:
		return "string set"
	case *types.AttributeValueMemberM:
		return "map"
	case *types.AttributeValueMemberNULL:
		return "null"
	}
	return "<empty>"
}
