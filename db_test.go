package dynamo

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

var (
	testDB    *DB
	testTable = "TestDB"
)

var dummyCreds = credentials.NewStaticCredentialsProvider("dummy", "dummy", "")

const offlineSkipMsg = "DYNAMO_TEST_REGION not set"

func init() {
	// os.Setenv("DYNAMO_TEST_REGION", "us-west-2")
	if region := os.Getenv("DYNAMO_TEST_REGION"); region != "" {
		var endpoint aws.EndpointResolverWithOptions
		if dte := os.Getenv("DYNAMO_TEST_ENDPOINT"); dte != "" {
			endpoint = aws.EndpointResolverWithOptionsFunc(
				func(service, region string, options ...interface{}) (aws.Endpoint, error) {
					return aws.Endpoint{URL: dte}, nil
				},
			)
		}
		cfg, err := config.LoadDefaultConfig(
			context.Background(),
			config.WithRegion(region),
			config.WithEndpointResolverWithOptions(endpoint),
			config.WithRetryer(nil),
		)
		if err != nil {
			log.Fatal(err)
		}
		testDB = New(cfg)
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

	tables, err := testDB.ListTables().All(context.TODO())
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
