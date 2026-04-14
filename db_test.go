package dynamo

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/smithy-go"
)

var (
	testDB             *DB
	testTableWidgets   = "TestDB"
	testTableSprockets = "TestDB-Sprockets"
)

var dummyCreds = credentials.NewStaticCredentialsProvider("dummy", "dummy", "")

const offlineSkipMsg = "DYNAMO_TEST_REGION not set"

// widget is the data structure used for integration tests
type widget struct {
	UserID int       `dynamo:",hash"`
	Time   time.Time `dynamo:",range" index:"Msg-Time-index,range"`
	Msg    string    `index:"Msg-Time-index,hash"`
	Count  int
	Meta   map[string]string
	StrPtr *string `dynamo:",allowempty"`
}

func TestMain(m *testing.M) {
	var endpoint, region *string
	if dte := os.Getenv("DYNAMO_TEST_ENDPOINT"); dte != "" {
		endpoint = &dte
	}
	if dtr := os.Getenv("DYNAMO_TEST_REGION"); dtr != "" {
		region = &dtr
	}
	if endpoint != nil && region == nil {
		dtr := "local"
		region = &dtr
	}
	if region != nil {
		var resolv aws.EndpointResolverWithOptions
		if endpoint != nil {
			resolv = aws.EndpointResolverWithOptionsFunc(
				func(service, region string, options ...interface{}) (aws.Endpoint, error) {
					return aws.Endpoint{URL: *endpoint}, nil
				},
			)
		}
		// TransactionCanceledException

		cfg, err := config.LoadDefaultConfig(
			context.Background(),
			config.WithRegion(*region),
			config.WithEndpointResolverWithOptions(resolv),
			config.WithRetryer(func() aws.Retryer {
				return retry.NewStandard(RetryTxConflicts)
			}),
		)
		if err != nil {
			log.Fatal(err)
		}
		testDB = New(cfg)
	}

	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	var offline bool
	if table := os.Getenv("DYNAMO_TEST_TABLE"); table != "" {
		offline = false
		// Test-% --> Test-1707708680863
		table = strings.ReplaceAll(table, "%", timestamp)
		testTableWidgets = table
	}
	if table := os.Getenv("DYNAMO_TEST_TABLE2"); table != "" {
		table = strings.ReplaceAll(table, "%", timestamp)
		testTableSprockets = table
	} else if !offline {
		testTableSprockets = testTableWidgets + "-Sprockets"
	}

	if !offline && testTableWidgets == testTableSprockets {
		panic(fmt.Sprintf("DYNAMO_TEST_TABLE must not equal DYNAMO_TEST_TABLE2. got DYNAMO_TEST_TABLE=%q and DYNAMO_TEST_TABLE2=%q",
			testTableWidgets, testTableSprockets))
	}

	var shouldCreate bool
	switch os.Getenv("DYNAMO_TEST_CREATE_TABLE") {
	case "1", "true", "yes":
		shouldCreate = true
	case "0", "false", "no":
		shouldCreate = false
	default:
		shouldCreate = endpoint != nil
	}
	ctx := context.Background()
	var created []Table
	if testDB != nil {
		for _, name := range []string{testTableWidgets, testTableSprockets} {
			table := testDB.Table(name)
			log.Println("Checking test table:", name)
			_, err := table.Describe().Run(ctx)
			switch {
			case isTableNotExistsErr(err) && shouldCreate:
				log.Println("Creating test table:", name)
				if err := testDB.CreateTable(name, widget{}).Index(Index{
					Name: "UserID-Msg-Time-index",
					HashKeys: []KeySchema{
						{Key: "UserID", Type: NumberType},
					},
					RangeKeys: []KeySchema{
						{Key: "Msg", Type: StringType},
						{Key: "Time", Type: StringType},
					},
				}).Run(ctx); err != nil {
					panic(err)
				}
				created = append(created, testDB.Table(name))
			case err != nil:
				panic(err)
			}
		}
	}

	code := m.Run()
	defer os.Exit(code)

	for _, table := range created {
		log.Println("Deleting test table:", table.Name())
		if err := table.DeleteTable().Run(ctx); err != nil {
			log.Println("Error deleting test table:", table.Name(), err)
		}
	}
}

func isTableNotExistsErr(err error) bool {
	var aerr smithy.APIError
	if errors.As(err, &aerr) {
		return aerr.ErrorCode() == "ResourceNotFoundException"
	}
	return false
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

	if !slices.Contains(tables, testTableWidgets) {
		t.Error("couldn't find testTable", testTableWidgets, "in:", tables)
	}
}
