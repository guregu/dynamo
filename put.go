package dynamo

import (
	// "errors"
	// "strings"

	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/gen/dynamodb"
	// "github.com/davecgh/go-spew/spew"
)

// Put creates a new item or replaces an existing item with the given struct.
// TODO: support putting map[string]interface{}
func (table Table) Put(item interface{}) error {
	encoded, err := marshalStruct(item)
	if err != nil {
		return err
	}

	req := &dynamodb.PutItemInput{
		TableName: aws.String(table.Name),
		Item:      encoded,
	}

	_, err = table.db.client.PutItem(req)
	return err
}
