package dynamo

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var itemDecodeOnlyTests = []struct {
	name   string
	given  map[string]*dynamodb.AttributeValue
	expect interface{}
}{
	{
		// unexported embedded pointers should be ignored
		name: "embedded unexported pointer",
		given: map[string]*dynamodb.AttributeValue{
			"Embedded": &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
		},
		expect: struct {
			*embedded
		}{},
	},
	{
		// unexported fields should be ignored
		name: "unexported fields",
		given: map[string]*dynamodb.AttributeValue{
			"a": &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
		},
		expect: struct {
			a bool
		}{},
	},
	{
		// embedded pointers shouldn't clobber existing fields
		name: "exported pointer embedded struct clobber",
		given: map[string]*dynamodb.AttributeValue{
			"Embedded": &dynamodb.AttributeValue{S: aws.String("OK")},
		},
		expect: struct {
			Embedded string
			*ExportedEmbedded
		}{
			Embedded:         "OK",
			ExportedEmbedded: &ExportedEmbedded{},
		},
	},
}

func TestUnmarshalAsymmetric(t *testing.T) {
	for _, tc := range itemDecodeOnlyTests {
		rv := reflect.New(reflect.TypeOf(tc.expect))
		expect := rv.Interface()
		err := UnmarshalItem(tc.given, expect)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", tc.name, err)
			continue
		}
		if !reflect.DeepEqual(rv.Elem().Interface(), tc.expect) {
			t.Errorf("%s: bad result: %#v ≠ %#v", tc.name, rv.Elem().Interface(), tc.expect)
		}
	}
}

func TestUnmarshalAppend(t *testing.T) {
	var results []struct {
		User  int `dynamo:"UserID"`
		Page  int
		Limit uint
		Null  interface{}
	}
	id := "12345"
	page := "5"
	limit := "20"
	null := true
	item := map[string]*dynamodb.AttributeValue{
		"UserID": &dynamodb.AttributeValue{N: &id},
		"Page":   &dynamodb.AttributeValue{N: &page},
		"Limit":  &dynamodb.AttributeValue{N: &limit},
		"Null":   &dynamodb.AttributeValue{NULL: &null},
	}

	for range [15]struct{}{} {
		err := unmarshalAppend(item, &results)
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, h := range results {
		if h.User != 12345 || h.Page != 5 || h.Limit != 20 || h.Null != nil {
			t.Error("invalid hit", h)
		}
	}

	var mapResults []map[string]interface{}

	for range [15]struct{}{} {
		err := unmarshalAppend(item, &mapResults)
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, h := range mapResults {
		if h["UserID"] != 12345.0 || h["Page"] != 5.0 || h["Limit"] != 20.0 || h["Null"] != nil {
			t.Error("invalid interface{} hit", h)
		}
	}
}

func TestUnmarshal(t *testing.T) {
	for _, tc := range encodingTests {
		rv := reflect.New(reflect.TypeOf(tc.in))
		err := unmarshalReflect(tc.out, rv.Elem())
		if err != nil {
			t.Errorf("%s: unexpected error: %v", tc.name, err)
		}

		if !reflect.DeepEqual(rv.Elem().Interface(), tc.in) {
			t.Errorf("%s: bad result: %#v ≠ %#v", tc.name, rv.Elem().Interface(), tc.out)
		}
	}
}

func TestUnmarshalItem(t *testing.T) {
	for _, tc := range itemEncodingTests {
		rv := reflect.New(reflect.TypeOf(tc.in))
		err := unmarshalItem(tc.out, rv.Interface())
		if err != nil {
			t.Errorf("%s: unexpected error: %v", tc.name, err)
		}

		if !reflect.DeepEqual(rv.Elem().Interface(), tc.in) {
			t.Errorf("%s: bad result: %#v ≠ %#v", tc.name, rv.Elem().Interface(), tc.in)
		}
	}
}

func TestUnmarshalNULL(t *testing.T) {
	tru := true
	arbitrary := "hello world"
	double := new(*int)
	item := map[string]*dynamodb.AttributeValue{
		"String":    &dynamodb.AttributeValue{NULL: &tru},
		"Slice":     &dynamodb.AttributeValue{NULL: &tru},
		"Array":     &dynamodb.AttributeValue{NULL: &tru},
		"StringPtr": &dynamodb.AttributeValue{NULL: &tru},
		"DoublePtr": &dynamodb.AttributeValue{NULL: &tru},
		"Map":       &dynamodb.AttributeValue{NULL: &tru},
		"Interface": &dynamodb.AttributeValue{NULL: &tru},
	}

	type resultType struct {
		String    string
		Slice     []string
		Array     [2]byte
		StringPtr *string
		DoublePtr **int
		Map       map[string]int
		Interface interface{}
	}

	// dirty result, we want this to be reset
	result := resultType{
		String:    "ABC",
		Slice:     []string{"A", "B"},
		Array:     [2]byte{'A', 'B'},
		StringPtr: &arbitrary,
		DoublePtr: double,
		Map: map[string]int{
			"A": 1,
		},
		Interface: "interface{}",
	}

	if err := UnmarshalItem(item, &result); err != nil {
		t.Error(err)
	}

	if (!reflect.DeepEqual(result, resultType{})) {
		t.Error("unmarshal null: bad result:", result, "≠", resultType{})
	}
}
