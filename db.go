package dynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DB struct {
	client *dynamodb.DynamoDB
}

func New(p client.ConfigProvider, cfgs ...*aws.Config) *DB {
	db := &DB{
		dynamodb.New(p, cfgs...),
	}
	return db
}

// Iter is an iterator for query results.
type Iter interface {
	Next(out interface{}) bool
	Err() error
}
