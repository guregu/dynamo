package dynamo

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func TestUnmarshalAppend(t *testing.T) {
	var results []struct {
		User int `dynamo:"UserID"`
		Page int
	}
	id := "12345"
	page := "5"
	item := map[string]*dynamodb.AttributeValue{
		"UserID": &dynamodb.AttributeValue{N: &id},
		"Page":   &dynamodb.AttributeValue{N: &page},
	}

	for range [15]struct{}{} {
		err := unmarshalAppend(item, &results)
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, h := range results {
		if h.User != 12345 || h.Page != 5 {
			t.Error("invalid hit", h)
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
