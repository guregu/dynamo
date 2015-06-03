package dynamo

import (
	"github.com/awslabs/aws-sdk-go/service/dynamodb"
	"testing"

	"log"
)

func TestUnmarshalAppend(t *testing.T) {
	var results []hit
	id := "12345"
	page := "5"
	item := map[string]*dynamodb.AttributeValue{
		"UserID": &dynamodb.AttributeValue{N: &id},
		"Page":   &dynamodb.AttributeValue{N: &page},
	}

	for range [15]struct{}{} {
		unmarshalAppend(item, &results)
	}
	log.Println(results)

	for _, h := range results {
		if h.User != 12345 || h.Page != 5 {
			t.Error("invalid hit", h)
		}
	}
}
