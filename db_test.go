package dynamo

import (
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
)

var (
	testDB    *DB
	testTable = "TestDB"
)

var dummyCreds = credentials.NewStaticCredentials("dummy", "dummy", "")

const offlineSkipMsg = "DYNAMO_TEST_REGION not set"

func init() {
	// os.Setenv("DYNAMO_TEST_REGION", "us-west-2")
	if region := os.Getenv("DYNAMO_TEST_REGION"); region != "" {
		var endpoint *string
		if dte := os.Getenv("DYNAMO_TEST_ENDPOINT"); dte != "" {
			endpoint = aws.String(dte)
		}
		testDB = New(session.Must(session.NewSession()), &aws.Config{
			Region:   aws.String(region),
			Endpoint: endpoint,
			// LogLevel: aws.LogLevel(aws.LogDebugWithHTTPBody),
		})
	}
	if table := os.Getenv("DYNAMO_TEST_TABLE"); table != "" {
		testTable = table
	}
}

// widget is the data structure used for integration tests
type widget struct {
	UserID int       `dynamo:",hash"`
	Time   time.Time `dynamo:",range"`
	Msg    string
	Count  int
	Meta   map[string]string
	StrPtr *string `dynamo:",allowempty"`
}

func TestListTables(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}

	tables, err := testDB.ListTables().All()
	if err != nil {
		t.Error(err)
		return
	}

	found := false
	for _, t := range tables {
		if t == testTable {
			found = true
			break
		}
	}

	if !found {
		t.Error("couldn't find testTable", testTable, "in:", tables)
	}
}
