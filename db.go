// Package dynamo offers a rich DynamoDB client.
package dynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// DB is a DynamoDB client.
type DB struct {
	client *dynamodb.DynamoDB
}

// New creates a new client with the given configuration.
func New(p client.ConfigProvider, cfgs ...*aws.Config) *DB {
	db := &DB{
		dynamodb.New(p, cfgs...),
	}
	return db
}

// Iter is an iterator for request results.
type Iter interface {
	// Next tries to unmarshal the next result into out.
	// Returns false when it is complete or if it runs into an error.
	Next(out interface{}) bool
	// Err returns the error encountered, if any.
	// You should check this after Next is finished.
	Err() error
}
