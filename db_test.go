package dynamo

import (
	"github.com/aws/aws-sdk-go/aws"
)

func testDB() *DB {
	return New(&aws.Config{
		Credentials: aws.DefaultChainCredentials,
		Region:      aws.String("us-west-2"),
		HTTPClient:  nil,
	})
}
