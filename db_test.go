package dynamo

import (
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
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
}

// widget is the data structure used for integration tests
type widget struct {
	UserID int       // PK
	Time   time.Time // RK
	Msg    string
}
