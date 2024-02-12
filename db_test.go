package dynamo

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
)

var (
	testDB             *DB
	testTableWidgets   = "TestDB"
	testTableSprockets = "TestDB-Sprockets"
)

var dummyCreds = credentials.NewStaticCredentials("dummy", "dummy", "")

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
		testDB = New(session.Must(session.NewSession()), &aws.Config{
			Region:   region,
			Endpoint: endpoint,
			// LogLevel: aws.LogLevel(aws.LogDebugWithHTTPBody),
		})
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

	var created []Table
	if testDB != nil {
		for _, name := range []string{testTableWidgets, testTableSprockets} {
			table := testDB.Table(name)
			log.Println("Checking test table:", name)
			_, err := table.Describe().Run()
			switch {
			case isTableNotExistsErr(err) && shouldCreate:
				log.Println("Creating test table:", name)
				if err := testDB.CreateTable(name, widget{}).Run(); err != nil {
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
		if err := table.DeleteTable().Run(); err != nil {
			log.Println("Error deleting test table:", table.Name(), err)
		}
	}
}

func isTableNotExistsErr(err error) bool {
	var ae awserr.Error
	return errors.As(err, &ae) && ae.Code() == "ResourceNotFoundException"
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
		if t == testTableWidgets {
			found = true
			break
		}
	}

	if !found {
		t.Error("couldn't find testTable", testTableWidgets, "in:", tables)
	}
}
