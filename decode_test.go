package dynamo

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

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
