package dynamo

import (
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type UserAction struct {
	UserID string    `dynamo:"ID,hash"`
	Time   time.Time `dynamo:",range"`
	Seq    int64     `localIndex:"ID-Seq-index,range"`
	UUID   string
	Name   string
	embeddedWithKeys
}

type embeddedWithKeys struct {
	Embedded **[]byte `index:"Embedded-index,hash"`
}

type Metric struct {
	ID    uint64                  `dynamo:"ID,hash"`
	Time  attributevalue.UnixTime `dynamo:",range"`
	Value uint64
}

type Metric2 struct {
	ID    uint64    `dynamo:"ID,hash"`
	Time  time.Time `dynamo:",range,unixtime"`
	Value uint64
}

func TestCreateTable(t *testing.T) {
	// until I do DeleteTable let's just compare the input
	// if testDB == nil {
	// 	t.Skip(offlineSkipMsg)
	// }

	input := testDB.CreateTable("UserActions", UserAction{}).
		Project("ID-Seq-index", IncludeProjection, "UUID", "Name").
		Provision(4, 2).
		ProvisionIndex("Embedded-index", 1, 2).
		Tag("Tag-Key", "old value").
		Tag("Tag-Key", "Tag-Value").
		SSEEncryption(true, "alias/key", SSETypeKMS).
		input()

	expected := &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("ID"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("Time"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("Seq"),
				AttributeType: types.ScalarAttributeTypeN,
			},
			{
				AttributeName: aws.String("Embedded"),
				AttributeType: types.ScalarAttributeTypeB,
			},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{{
			IndexName: aws.String("Embedded-index"),
			KeySchema: []types.KeySchemaElement{{
				AttributeName: aws.String("Embedded"),
				KeyType:       types.KeyTypeHash,
			}},
			Projection: &types.Projection{
				ProjectionType: types.ProjectionTypeAll,
			},
			ProvisionedThroughput: &types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1),
				WriteCapacityUnits: aws.Int64(2),
			},
		}},
		KeySchema: []types.KeySchemaElement{{
			AttributeName: aws.String("ID"),
			KeyType:       types.KeyTypeHash,
		}, {
			AttributeName: aws.String("Time"),
			KeyType:       types.KeyTypeRange,
		}},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{{
			IndexName: aws.String("ID-Seq-index"),
			KeySchema: []types.KeySchemaElement{{
				AttributeName: aws.String("ID"),
				KeyType:       types.KeyTypeHash,
			}, {
				AttributeName: aws.String("Seq"),
				KeyType:       types.KeyTypeRange,
			}},
			Projection: &types.Projection{
				ProjectionType:   types.ProjectionTypeInclude,
				NonKeyAttributes: []string{"UUID", "Name"},
			},
		}},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(4),
			WriteCapacityUnits: aws.Int64(2),
		},
		Tags: []types.Tag{
			{
				Key:   aws.String("Tag-Key"),
				Value: aws.String("Tag-Value"),
			},
		},
		SSESpecification: &types.SSESpecification{
			Enabled:        aws.Bool(true),
			KMSMasterKeyId: aws.String("alias/key"),
			SSEType:        types.SSEType("KMS"),
		},
		TableName: aws.String("UserActions"),
	}

	if !reflect.DeepEqual(input, expected) {
		t.Error("unexpected input", input)
	}
}

func TestCreateTableUintUnixTime(t *testing.T) {
	input := testDB.CreateTable("Metrics", Metric{}).
		OnDemand(true).
		input()
	input2 := testDB.CreateTable("Metrics", Metric2{}).
		OnDemand(true).
		input()
	expected := &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("ID"),
				AttributeType: types.ScalarAttributeTypeN,
			},
			{
				AttributeName: aws.String("Time"),
				AttributeType: types.ScalarAttributeTypeN,
			},
		},
		KeySchema: []types.KeySchemaElement{{
			AttributeName: aws.String("ID"),
			KeyType:       types.KeyTypeHash,
		}, {
			AttributeName: aws.String("Time"),
			KeyType:       types.KeyTypeRange,
		}},
		BillingMode: types.BillingModePayPerRequest,
		TableName:   aws.String("Metrics"),
	}
	if !reflect.DeepEqual(input, expected) {
		t.Error("unexpected input", input)
	}
	if !reflect.DeepEqual(input2, expected) {
		t.Error("unexpected input (unixtime tag)", input2)
	}
}
