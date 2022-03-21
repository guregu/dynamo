package dynamo

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func TestTableLifecycle(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}

	now := time.Now().UTC()
	name := fmt.Sprintf("TestDB-%d", now.UnixNano())

	// example from the docs
	type UserAction struct {
		UserID string    `dynamo:"ID,hash" index:"Seq-ID-index,range"`
		Time   time.Time `dynamo:",range"`
		Seq    int64     `localIndex:"ID-Seq-index,range" index:"Seq-ID-index,hash"`
		UUID   string    `index:"UUID-index,hash"`
	}

	// create & wait
	if err := testDB.CreateTable(name, UserAction{}).Wait(); err != nil {
		t.Fatal(err)
	}

	// make sure it really works
	table := testDB.Table(name)
	if err := table.Put(UserAction{UserID: "test", Time: now, Seq: 1, UUID: "42"}).Run(); err != nil {
		t.Fatal(err)
	}

	// delete & wait
	if err := testDB.Table(name).DeleteTable().Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestAddConsumedCapacity(t *testing.T) {
	raw := &dynamodb.ConsumedCapacity{
		TableName: aws.String("TestTable"),
		Table: &dynamodb.Capacity{
			CapacityUnits:      aws.Float64(9),
			ReadCapacityUnits:  aws.Float64(4),
			WriteCapacityUnits: aws.Float64(5),
		},
		GlobalSecondaryIndexes: map[string]*dynamodb.Capacity{
			"TestGSI": {
				CapacityUnits:      aws.Float64(3),
				ReadCapacityUnits:  aws.Float64(1),
				WriteCapacityUnits: aws.Float64(2),
			},
		},
		LocalSecondaryIndexes: map[string]*dynamodb.Capacity{
			"TestLSI": {
				CapacityUnits:      aws.Float64(30),
				ReadCapacityUnits:  aws.Float64(10),
				WriteCapacityUnits: aws.Float64(20),
			},
		},
		CapacityUnits:      aws.Float64(42),
		ReadCapacityUnits:  aws.Float64(15),
		WriteCapacityUnits: aws.Float64(27),
	}
	expected := ConsumedCapacity{
		TableName:  *raw.TableName,
		Table:      *raw.Table.CapacityUnits,
		TableRead:  *raw.Table.ReadCapacityUnits,
		TableWrite: *raw.Table.WriteCapacityUnits,
		GSI:        map[string]float64{"TestGSI": *raw.GlobalSecondaryIndexes["TestGSI"].CapacityUnits},
		GSIRead:    map[string]float64{"TestGSI": *raw.GlobalSecondaryIndexes["TestGSI"].ReadCapacityUnits},
		GSIWrite:   map[string]float64{"TestGSI": *raw.GlobalSecondaryIndexes["TestGSI"].WriteCapacityUnits},
		LSI:        map[string]float64{"TestLSI": *raw.LocalSecondaryIndexes["TestLSI"].CapacityUnits},
		LSIRead:    map[string]float64{"TestLSI": *raw.LocalSecondaryIndexes["TestLSI"].ReadCapacityUnits},
		LSIWrite:   map[string]float64{"TestLSI": *raw.LocalSecondaryIndexes["TestLSI"].WriteCapacityUnits},
		Total:      *raw.CapacityUnits,
		Read:       *raw.ReadCapacityUnits,
		Write:      *raw.WriteCapacityUnits,
	}

	var cc ConsumedCapacity
	addConsumedCapacity(&cc, raw)

	if !reflect.DeepEqual(cc, expected) {
		t.Error("bad ConsumedCapacity:", cc, "≠", expected)
	}
}
