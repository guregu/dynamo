package dynamo

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"testing"
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
