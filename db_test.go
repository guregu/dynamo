package dynamo

import (
	"github.com/awslabs/aws-sdk-go/aws"
)

func testDB() *DB {
	creds := aws.DetectCreds("", "", "")
	return New(&aws.Config{
		Credentials: creds,
		Region:      "ap-southeast-1",
		HTTPClient:  nil,
	})
}
