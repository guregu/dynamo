package dynamo

import (
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type UserAction struct {
	UserID string    `dynamo:"ID,hash"`
	Time   time.Time `dynamo:",range"`
	Seq    int64     `localIndex:"ID-Seq-index,range"`
	UUID   string
	embeddedWithKeys
}

type embeddedWithKeys struct {
	Embedded **[]byte `index:"Embedded-index,hash"`
}

type Metric struct {
	ID    uint64                     `dynamo:"ID,hash"`
	Time  dynamodbattribute.UnixTime `dynamo:",range"`
	Value uint64
}

func TestCreateTable(t *testing.T) {
	// until I do DeleteTable let's just compare the input
	// if testDB == nil {
	// 	t.Skip(offlineSkipMsg)
	// }

	input := testDB.CreateTable("UserActions", UserAction{}).
		Project("ID-Seq-index", IncludeProjection, "UUID").
		Provision(4, 2).
		ProvisionIndex("Embedded-index", 1, 2).
		Tag("Tag-Key", "old value").
		Tag("Tag-Key", "Tag-Value").
		input()

	expected := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("ID"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("Time"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("Seq"),
				AttributeType: aws.String("N"),
			},
			{
				AttributeName: aws.String("Embedded"),
				AttributeType: aws.String("B"),
			},
		},
		GlobalSecondaryIndexes: []*dynamodb.GlobalSecondaryIndex{{
			IndexName: aws.String("Embedded-index"),
			KeySchema: []*dynamodb.KeySchemaElement{{
				AttributeName: aws.String("Embedded"),
				KeyType:       aws.String("HASH"),
			}},
			Projection: &dynamodb.Projection{
				ProjectionType: aws.String("ALL"),
			},
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1),
				WriteCapacityUnits: aws.Int64(2),
			},
		}},
		KeySchema: []*dynamodb.KeySchemaElement{{
			AttributeName: aws.String("ID"),
			KeyType:       aws.String("HASH"),
		}, {
			AttributeName: aws.String("Time"),
			KeyType:       aws.String("RANGE"),
		}},
		LocalSecondaryIndexes: []*dynamodb.LocalSecondaryIndex{{
			IndexName: aws.String("ID-Seq-index"),
			KeySchema: []*dynamodb.KeySchemaElement{{
				AttributeName: aws.String("ID"),
				KeyType:       aws.String("HASH"),
			}, {
				AttributeName: aws.String("Seq"),
				KeyType:       aws.String("RANGE"),
			}},
			Projection: &dynamodb.Projection{
				ProjectionType:   aws.String("INCLUDE"),
				NonKeyAttributes: []*string{aws.String("UUID")},
			},
		}},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(4),
			WriteCapacityUnits: aws.Int64(2),
		},
		Tags: []*dynamodb.Tag{
			&dynamodb.Tag{
				Key:   aws.String("Tag-Key"),
				Value: aws.String("Tag-Value"),
			},
		},
		TableName: aws.String("UserActions"),
	}

	if !reflect.DeepEqual(input, expected) {
		t.Error("unexpected input", input)
	}

	t.Run("uint and UnixTime", func(t *testing.T) {
		input := testDB.CreateTable("Metrics", Metric{}).
			OnDemand(true).
			input()
		expected := &dynamodb.CreateTableInput{
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String("ID"),
					AttributeType: aws.String("N"),
				},
				{
					AttributeName: aws.String("Time"),
					AttributeType: aws.String("N"),
				},
			},
			KeySchema: []*dynamodb.KeySchemaElement{{
				AttributeName: aws.String("ID"),
				KeyType:       aws.String("HASH"),
			}, {
				AttributeName: aws.String("Time"),
				KeyType:       aws.String("RANGE"),
			}},
			BillingMode: aws.String(dynamodb.BillingModePayPerRequest),
			TableName:   aws.String("Metrics"),
		}
		if !reflect.DeepEqual(input, expected) {
			t.Error("unexpected input", input)
		}
	})
}
