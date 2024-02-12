package dynamo

import (
	"errors"
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
	testDB    *DB
	testTable = "TestDB"
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
	var endpoint *string
	if region := os.Getenv("DYNAMO_TEST_REGION"); region != "" {
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
		// Test-% --> Test-1707708680863
		table = strings.ReplaceAll(table, "%", strconv.FormatInt(time.Now().UnixMilli(), 10))
		testTable = table
	}

	var created []Table
	if testDB != nil {
		table := testDB.Table(testTable)
		log.Println("Checking test table:", testTable)
		_, err := table.Describe().Run()
		switch {
		case isTableNotExistsErr(err) && endpoint != nil:
			log.Println("Creating test table:", testTable)
			if err := testDB.CreateTable(testTable, widget{}).Run(); err != nil {
				panic(err)
			}
			created = append(created, testDB.Table(testTable))
		case err != nil:
			panic(err)
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
		if t == testTable {
			found = true
			break
		}
	}

	if !found {
		t.Error("couldn't find testTable", testTable, "in:", tables)
	}
}
