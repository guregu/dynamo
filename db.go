package dynamo

import (
	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/gen/dynamodb"
)

type DB struct {
	client *dynamodb.DynamoDB
}

func New(creds aws.CredentialsProvider, region string, client *http.Client) *DB {
	db := &DB{
		client: dynamodb.New(creds, region, client),
	}
	return db
}
