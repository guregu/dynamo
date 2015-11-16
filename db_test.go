package dynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
)

func testDB() *DB {
	return New(session.New(), &aws.Config{Region: aws.String("us-west-2")})
}
