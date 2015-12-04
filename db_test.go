package dynamo

import (
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
)

var (
	testDB    *DB
	testTable = "TestDB"
)

const offlineSkipMsg = "DYNAMO_TEST_REGION not set"

func init() {
	if region := os.Getenv("DYNAMO_TEST_REGION"); region != "" {
		testDB = New(session.New(), &aws.Config{Region: aws.String(region)})
	}
	if table := os.Getenv("DYNAMO_TEST_TABLE"); table != "" {
		testTable = table
	}
}

// widget is the data structure used for integration tests
type widget struct {
	UserID int       // PK
	Time   time.Time // RK
	Msg    string
	Count  int
}

func isConditionalCheckErr(err error) bool {
	if ae, ok := err.(awserr.RequestFailure); ok {
		return ae.Code() == "ConditionalCheckFailedException"
	}
	return false
}
