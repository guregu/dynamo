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
	out  Item
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
		out: Item{
			"L":     &types.AttributeValueMemberL{Value: []types.AttributeValue{}},
			"M":     &types.AttributeValueMemberM{Value: Item{}},
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
		out: Item{
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
		out: Item{
			"M": &types.AttributeValueMemberM{
				Value: Item{
					"struct": &types.AttributeValueMemberM{
						Value: Item{
							"InnerMap": &types.AttributeValueMemberM{
								Value: Item{
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
		out: Item{
			"Public": &types.AttributeValueMemberN{Value: ("555")},
		},
	},
}

func TestMarshal(t *testing.T) {
	for _, tc := range encodingTests {
		t.Run(tc.name, func(t *testing.T) {
			item, err := marshal(tc.in, flagNone)
			if err != nil {
				t.Errorf("%s: unexpected error: %v", tc.name, err)
			}

			if !reflect.DeepEqual(item, tc.out) {
				t.Errorf("%s: bad result: %#v ≠ %#v", tc.name, item, tc.out)
			}
		})
	}
}

func TestMarshalItem(t *testing.T) {
	for _, tc := range itemEncodingTests {
		t.Run(tc.name, func(t *testing.T) {
			item, err := marshalItem(tc.in)
			if err != nil {
				t.Errorf("%s: unexpected error: %v", tc.name, err)
			}

			if !reflect.DeepEqual(item, tc.out) {
				t.Errorf("%s: bad result: %#v ≠ %#v", tc.name, item, tc.out)
			}
		})
	}
}

func TestMarshalItemAsymmetric(t *testing.T) {
	for _, tc := range itemEncodeOnlyTests {
		t.Run(tc.name, func(t *testing.T) {
			item, err := marshalItem(tc.in)
			if err != nil {
				t.Errorf("%s: unexpected error: %v", tc.name, err)
			}

			if !reflect.DeepEqual(item, tc.out) {
				t.Errorf("%s: bad result: %#v ≠ %#v", tc.name, item, tc.out)
			}
		})
	}
}

type isValue_Kind interface {
	isValue_Kind()
}

type myStruct struct {
	OK    bool
	Value isValue_Kind
}

func (ms *myStruct) MarshalDynamoItem() (map[string]types.AttributeValue, error) {
	world := "world"
	return map[string]types.AttributeValue{
		"hello": &types.AttributeValueMemberS{Value: world},
	}, nil
}

func (ms *myStruct) UnmarshalDynamoItem(item map[string]types.AttributeValue) error {
	hello := item["hello"]
	if h, ok := hello.(*types.AttributeValueMemberS); ok && h.Value == "world" {
		ms.OK = true
	} else {
		ms.OK = false
	}
	return nil
}

var _ ItemMarshaler = &myStruct{}
var _ ItemUnmarshaler = &myStruct{}

func TestMarshalItemBypass(t *testing.T) {
	something := &myStruct{}
	got, err := MarshalItem(something)
	if err != nil {
		t.Fatal(err)
	}

	world := "world"
	expect := map[string]types.AttributeValue{
		"hello": &types.AttributeValueMemberS{Value: world},
	}
	if !reflect.DeepEqual(got, expect) {
		t.Error("bad marshal. want:", expect, "got:", got)
	}

	var dec myStruct
	err = UnmarshalItem(got, &dec)
	if err != nil {
		t.Fatal(err)
	}
	if !dec.OK {
		t.Error("bad unmarshal")
	}
}
