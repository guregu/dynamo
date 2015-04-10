package dynamo

import (
	"github.com/awslabs/aws-sdk-go/aws"
)

func testDB() *DB {
	creds := aws.DetectCreds("", "", "")
	return New(&aws.Config{
		Credentials: creds,
		Region:      "us-west-2",
		HTTPClient:  nil,
	})
}
