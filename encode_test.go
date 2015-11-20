package dynamo

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func TestMarshalStruct(t *testing.T) {
	var testTable = []struct {
		name string
		in   interface{}
		out  map[string]*dynamodb.AttributeValue
	}{
		{
			name: "strings",
			in: struct {
				A string
			}{
				A: "hello",
			},
			out: map[string]*dynamodb.AttributeValue{
				"A": &dynamodb.AttributeValue{S: aws.String("hello")},
			},
		},
		{
			name: "pointer (string)",
			in: &struct {
				A string
			}{
				A: "hello",
			},
			out: map[string]*dynamodb.AttributeValue{
				"A": &dynamodb.AttributeValue{S: aws.String("hello")},
			},
		},
		{
			name: "rename",
			in: struct {
				A string `dynamo:"renamed"`
			}{
				A: "hello",
			},
			out: map[string]*dynamodb.AttributeValue{
				"renamed": &dynamodb.AttributeValue{S: aws.String("hello")},
			},
		},
		{
			name: "skip",
			in: struct {
				A     string `dynamo:"-"`
				Other bool
			}{
				A:     "hello",
				Other: true,
			},
			out: map[string]*dynamodb.AttributeValue{
				"Other": &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
			},
		},
		{
			name: "omitempty",
			in: struct {
				A     bool `dynamo:",omitempty"`
				Other bool
			}{
				Other: true,
			},
			out: map[string]*dynamodb.AttributeValue{
				"Other": &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
			},
		},
		{
			name: "embedded struct",
			in: struct {
				embedded
			}{
				embedded: embedded{
					Embedded: true,
				},
			},
			out: map[string]*dynamodb.AttributeValue{
				"Embedded": &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
			},
		},
		{
			name: "sets",
			in: struct {
				SS1 []string        `dynamo:",set"`
				SS2 []textMarshaler `dynamo:",set"`
				BS  [][]byte        `dynamo:",set"`
				NS1 []int           `dynamo:",set"`
				NS2 []float64       `dynamo:",set"`
			}{
				SS1: []string{"A", "B"},
				SS2: []textMarshaler{textMarshaler(true), textMarshaler(false)},
				BS:  [][]byte{[]byte{'A'}, []byte{'B'}},
				NS1: []int{1, 2},
				NS2: []float64{1, 2},
			},
			out: map[string]*dynamodb.AttributeValue{
				"SS1": &dynamodb.AttributeValue{SS: []*string{aws.String("A"), aws.String("B")}},
				"SS2": &dynamodb.AttributeValue{SS: []*string{aws.String("true"), aws.String("false")}},
				"BS":  &dynamodb.AttributeValue{BS: [][]byte{[]byte{'A'}, []byte{'B'}}},
				"NS1": &dynamodb.AttributeValue{NS: []*string{aws.String("1"), aws.String("2")}},
				"NS2": &dynamodb.AttributeValue{NS: []*string{aws.String("1"), aws.String("2")}},
			},
		},
	}

	for _, tc := range testTable {
		item, err := marshalStruct(tc.in)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", tc.name, err)
		}

		if !reflect.DeepEqual(item, tc.out) {
			t.Errorf("%s: bad result: %#v ≠ %#v", tc.name, item, tc.out)
		}
	}
}

func TestMarshal(t *testing.T) {
	for _, tc := range encodingTests {
		item, err := marshal(tc.in, "")
		if err != nil {
			t.Errorf("%s: unexpected error: %v", tc.name, err)
		}

		if !reflect.DeepEqual(item, tc.out) {
			t.Errorf("%s: bad result: %#v ≠ %#v", tc.name, item, tc.out)
		}
	}
}

type embedded struct {
	Embedded bool
}

type customMarshaler int

func (cm customMarshaler) MarshalDynamo() (*dynamodb.AttributeValue, error) {
	return &dynamodb.AttributeValue{
		BOOL: aws.Bool(cm != 0),
	}, nil
}

func (cm *customMarshaler) UnmarshalDynamo(av *dynamodb.AttributeValue) error {
	if *av.BOOL == true {
		*cm = 1
	}
	return nil
}

var _ Marshaler = new(customMarshaler)
var _ Unmarshaler = new(customMarshaler)

type textMarshaler bool

func (tm textMarshaler) MarshalText() ([]byte, error) {
	if tm {
		return []byte("true"), nil
	}
	return []byte("false"), nil
}

func (tm *textMarshaler) UnmarshalText(text []byte) error {
	*tm = string(text) == "true"
	return nil
}
