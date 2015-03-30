package dynamo

import (
	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/service/dynamodb"
)

func testDB() *DB {
	creds := aws.DetectCreds("", "", "")
	return New(&aws.Config{
		Credentials: creds,
		Region:      region,
		Client:      client,
	})
}
