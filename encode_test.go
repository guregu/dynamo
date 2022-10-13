package dynamo

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var itemEncodeOnlyTests = []struct {
	name string
	in   interface{}
	out  map[string]types.AttributeValue
}{
	{
		name: "omitemptyelem",
		in: struct {
			L     []*string         `dynamo:",omitemptyelem"`
			SS    []string          `dynamo:",omitemptyelem,set"`
			M     map[string]string `dynamo:",omitemptyelem"`
			Other bool
		}{
			L:     []*string{nil, aws.String("")},
			SS:    []string{""},
			M:     map[string]string{"test": ""},
			Other: true,
		},
		out: map[string]types.AttributeValue{
			"L":     &types.AttributeValueMemberL{Value: []types.AttributeValue{}},
			"M":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{}},
			"Other": &types.AttributeValueMemberBOOL{Value: true},
		},
	},
	{
		name: "omitemptyelem + omitempty",
		in: struct {
			L     []*string         `dynamo:",omitemptyelem,omitempty"`
			M     map[string]string `dynamo:",omitemptyelem,omitempty"`
			Other bool
		}{
			L:     []*string{nil, aws.String("")},
			M:     map[string]string{"test": ""},
			Other: true,
		},
		out: map[string]types.AttributeValue{
			"Other": &types.AttributeValueMemberBOOL{Value: (true)},
		},
	},
	{
		name: "allowemptyelem flag on map with a struct element that has a map field",
		in: struct {
			M map[string]interface{} `dynamo:",allowemptyelem"`
		}{
			M: map[string]interface{}{
				"struct": struct {
					InnerMap map[string]interface{} // no struct tags, empty elems not encoded
				}{
					InnerMap: map[string]interface{}{
						"empty": "",
					},
				},
			},
		},
		out: map[string]types.AttributeValue{
			"M": &types.AttributeValueMemberM{
				Value: map[string]types.AttributeValue{
					"struct": &types.AttributeValueMemberM{
						Value: map[string]types.AttributeValue{
							"InnerMap": &types.AttributeValueMemberM{
								Value: map[string]types.AttributeValue{
									// expected empty inside
								},
							},
						},
					},
				},
			},
		},
	},
	{
		name: "unexported field",
		in: struct {
			Public   int
			private  int
			private2 *int
		}{
			Public:   555,
			private:  1337,
			private2: new(int),
		},
		out: map[string]types.AttributeValue{
			"Public": &types.AttributeValueMemberN{Value: ("555")},
		},
	},
}

func TestMarshal(t *testing.T) {
	for _, tc := range encodingTests {
		item, err := marshal(tc.in, flagNone)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", tc.name, err)
		}

		if !reflect.DeepEqual(item, tc.out) {
			t.Errorf("%s: bad result: %#v ≠ %#v", tc.name, item, tc.out)
		}
	}
}

func TestMarshalItem(t *testing.T) {
	for _, tc := range itemEncodingTests {
		item, err := marshalItem(tc.in)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", tc.name, err)
		}

		if !reflect.DeepEqual(item, tc.out) {
			t.Errorf("%s: bad result: %#v ≠ %#v", tc.name, item, tc.out)
		}
	}
}

func TestMarshalItemAsymmetric(t *testing.T) {
	for _, tc := range itemEncodeOnlyTests {
		item, err := marshalItem(tc.in)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", tc.name, err)
		}

		if !reflect.DeepEqual(item, tc.out) {
			t.Errorf("%s: bad result: %#v ≠ %#v", tc.name, item, tc.out)
		}
	}
}
