package dynamo

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

var (
	testDB    *DB
	testTable = "TestDB"
)

const offlineSkipMsg = "DYNAMO_TEST_REGION not set"

type endpointResolver struct {
	resolveEndpoint func(service, region string, options ...interface{}) (aws.Endpoint, error)
}

func (e endpointResolver) ResolveEndpoint(service, region string, options ...interface{}) (aws.Endpoint, error) {
	return e.resolveEndpoint(service, region, options...)
}

func init() {
	if region := os.Getenv("DYNAMO_TEST_REGION"); region != "" {
		var endpoint *string
		if dte := os.Getenv("DYNAMO_TEST_ENDPOINT"); dte != "" {
			endpoint = aws.String(dte)
		}

		resolver := endpointResolver{
			resolveEndpoint: func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL: *endpoint,
				}, nil
			},
		}

		conf, err := config.LoadDefaultConfig(context.TODO(), config.WithEndpointResolverWithOptions(resolver),
			config.WithRegion(os.Getenv("AWS_REGION")))
		testDB = New(conf)

		if err != nil {
			return
		}
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
