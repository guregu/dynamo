package dynamo

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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
	if err := testDB.CreateTable(name, UserAction{}).Index(Index{
		Name:         "Foo-Bar-index",
		HashKey:      "Foo",
		HashKeyType:  StringType,
		RangeKey:     "Bar",
		RangeKeyType: NumberType,
	}).Wait(); err != nil {
		t.Fatal(err)
	}

	desc, err := testDB.Table(name).Describe().Run()
	if err != nil {
		t.Fatal(err)
	}
	want := Description{
		Name: name,
		// ARN:    "arn:aws:dynamodb:ddblocal:000000000000:table/TestDB-1665411117776473700",
		Status:       ActiveStatus,
		HashKey:      "ID",
		HashKeyType:  StringType,
		RangeKey:     "Time",
		RangeKeyType: StringType,
		Throughput:   Throughput{Read: 1, Write: 1},
		GSI: []Index{
			{
				Name: "Foo-Bar-index",
				// ARN: "arn:aws:dynamodb:ddblocal:000000000000:table/TestDB-1665411117776473700/index/Foo-Bar-index",
				Status:            ActiveStatus,
				HashKey:           "Foo",
				HashKeyType:       StringType,
				RangeKey:          "Bar",
				RangeKeyType:      NumberType,
				Throughput:        Throughput{Read: 1, Write: 1},
				ProjectionType:    AllProjection,
				ProjectionAttribs: []string(nil),
			},
			{
				Name: "Seq-ID-index",
				// ARN: "arn:aws:dynamodb:ddblocal:000000000000:table/TestDB-1665411117776473700/index/Seq-ID-index",
				Status:            ActiveStatus,
				HashKey:           "Seq",
				HashKeyType:       NumberType,
				RangeKey:          "ID",
				RangeKeyType:      StringType,
				Throughput:        Throughput{Read: 1, Write: 1},
				ProjectionType:    AllProjection,
				ProjectionAttribs: []string(nil),
			},
			{
				Name: "UUID-index",
				// ARN: "arn:aws:dynamodb:ddblocal:000000000000:table/TestDB-1665411117776473700/index/UUID-index",
				Status:            ActiveStatus,
				HashKey:           "UUID",
				HashKeyType:       StringType,
				Throughput:        Throughput{Read: 1, Write: 1},
				ProjectionType:    AllProjection,
				ProjectionAttribs: []string(nil),
			},
		},
		LSI: []Index{
			{
				Name: "ID-Seq-index",
				// ARN: "arn:aws:dynamodb:ddblocal:000000000000:table/TestDB-1665411117776473700/index/ID-Seq-index",
				Status:            ActiveStatus,
				Backfilling:       false,
				Local:             true,
				HashKey:           "ID",
				HashKeyType:       StringType,
				RangeKey:          "Seq",
				RangeKeyType:      NumberType,
				Throughput:        Throughput{Read: 1, Write: 1},
				ProjectionType:    AllProjection,
				ProjectionAttribs: []string(nil),
			},
		},
	}
	normalizeDesc(&desc)
	if !reflect.DeepEqual(want, desc) {
		t.Errorf("unexpected table description. want:\n%#v\ngot:\n%#v\n", want, desc)
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
	raw := &types.ConsumedCapacity{
		TableName: aws.String("TestTable"),
		Table: &types.Capacity{
			CapacityUnits:      aws.Float64(9),
			ReadCapacityUnits:  aws.Float64(4),
			WriteCapacityUnits: aws.Float64(5),
		},
		GlobalSecondaryIndexes: map[string]types.Capacity{
			"TestGSI": {
				CapacityUnits:      aws.Float64(3),
				ReadCapacityUnits:  aws.Float64(1),
				WriteCapacityUnits: aws.Float64(2),
			},
		},
		LocalSecondaryIndexes: map[string]types.Capacity{
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
	expected := &ConsumedCapacity{
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

	var cc = new(ConsumedCapacity)
	addConsumedCapacity(cc, raw)

	if !reflect.DeepEqual(cc, expected) {
		t.Error("bad ConsumedCapacity:", cc, "≠", expected)
	}
}

func normalizeDesc(desc *Description) {
	desc.ARN = ""
	desc.Created = time.Time{}
	desc.Throughput.LastInc = time.Time{}
	desc.Throughput.LastDec = time.Time{}
	for i, idx := range desc.GSI {
		idx.ARN = ""
		idx.Throughput.LastInc = time.Time{}
		idx.Throughput.LastDec = time.Time{}
		desc.GSI[i] = idx
	}
	sort.Slice(desc.GSI, func(i, j int) bool {
		return desc.GSI[i].Name < desc.GSI[j].Name
	})
	for i, idx := range desc.LSI {
		idx.ARN = ""
		idx.Throughput.LastInc = time.Time{}
		idx.Throughput.LastDec = time.Time{}
		desc.LSI[i] = idx
	}
	sort.Slice(desc.LSI, func(i, j int) bool {
		return desc.LSI[i].Name < desc.LSI[j].Name
	})
}
