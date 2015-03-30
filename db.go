package dynamo

import (
	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/service/dynamodb"
)

type DB struct {
	client *dynamodb.DynamoDB
}

func New(cfg *aws.Config) *DB {
	db := &DB{
		dynamodb.New(cfg),
	}
	return db
}
