package dynamo

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var itemEncodeOnlyTests = []struct {
	name string
	in   interface{}
	out  map[string]*dynamodb.AttributeValue
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
		out: map[string]*dynamodb.AttributeValue{
			"L":     {L: []*dynamodb.AttributeValue{}},
			"M":     {M: map[string]*dynamodb.AttributeValue{}},
			"Other": {BOOL: aws.Bool(true)},
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
		out: map[string]*dynamodb.AttributeValue{
			"Other": {BOOL: aws.Bool(true)},
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
		out: map[string]*dynamodb.AttributeValue{
			"M": {M: map[string]*dynamodb.AttributeValue{
				"struct": {M: map[string]*dynamodb.AttributeValue{
					"InnerMap": {M: map[string]*dynamodb.AttributeValue{
						// expected empty inside
					}},
				}},
			}},
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
		out: map[string]*dynamodb.AttributeValue{
			"Public": {N: aws.String("555")},
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

func (ms *myStruct) MarshalDynamoItem() (map[string]*dynamodb.AttributeValue, error) {
	world := "world"
	return map[string]*dynamodb.AttributeValue{
		"hello": {S: &world},
	}, nil
}

func (ms *myStruct) UnmarshalDynamoItem(item map[string]*dynamodb.AttributeValue) error {
	hello := item["hello"]
	if hello == nil || hello.S == nil || *hello.S != "world" {
		ms.OK = false
	} else {
		ms.OK = true
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
	expect := map[string]*dynamodb.AttributeValue{
		"hello": {S: &world},
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
