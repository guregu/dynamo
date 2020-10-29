package dynamo

import (
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type awsTestWidget struct {
	UserID int       // Hash key, a.k.a. partition key
	Time   time.Time // Range key, a.k.a. sort key

	Msg       string   `dynamodbav:"Message"`
	Count     int      `dynamodbav:",omitempty"`
	Friends   []string `dynamodbav:",stringset"` // Sets
	SecretKey string   `dynamodbav:"-"`          // Ignored
}

func TestAWSEncoding(t *testing.T) {
	w := awsTestWidget{
		UserID:    555,
		Time:      time.Now().UTC(),
		Msg:       "hello",
		Count:     0,
		Friends:   []string{"a", "b"},
		SecretKey: "seeeekret",
	}
	av, err := Marshal(AWSEncoding(w))
	if err != nil {
		t.Error(err)
	}
	official, err := dynamodbattribute.Marshal(w)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(av, official) {
		t.Error("AWS marshal not equal")
	}

	blank := awsTestWidget{}
	err = Unmarshal(official, AWSEncoding(&blank))
	if err != nil {
		t.Error(err)
	}
	w.SecretKey = ""

	if !reflect.DeepEqual(w, blank) {
		t.Error("AWS unmarshal not equal")
		t.Logf("%#v != %#v", w, blank)
	}
}

func TestAWSIfaces(t *testing.T) {
	unix := dynamodbattribute.UnixTime(time.Now())
	av, err := Marshal(unix)
	if err != nil {
		t.Error(err)
	}
	official, err := dynamodbattribute.Marshal(unix)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(av, official) {
		t.Error("marshal not equal.", av, "≠", official)
	}

	var result, officialResult dynamodbattribute.UnixTime
	err = Unmarshal(official, &result)
	if err != nil {
		t.Error(err)
	}
	err = dynamodbattribute.Unmarshal(official, &officialResult)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(result, officialResult) {
		t.Error("unmarshal not equal.", result, "≠", officialResult)
	}
}

func TestAWSItems(t *testing.T) {
	type Foo struct {
		ID string `dynamodbav:"id"`
	}

	item := Foo{
		ID: "abcdefg",
	}

	result, err := marshalItem(AWSEncoding(item))
	if err != nil {
		t.Error(err)
	}
	official, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(result, official) {
		t.Error("marshal not equal.", result, "≠", official)
	}

	var unmarshaled, unmarshaledOfficial Foo
	err = unmarshalItem(official, AWSEncoding(&unmarshaled))
	if err != nil {
		t.Error(err)
	}
	err = dynamodbattribute.UnmarshalMap(official, &unmarshaledOfficial)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(unmarshaled, unmarshaledOfficial) {
		t.Error("marshal not equal.", unmarshaled, "≠", unmarshaledOfficial)
	}
}

func TestAWSUnmarshalAppend(t *testing.T) {
	type foo struct {
		A string `dynamo:"wrong1" dynamodbav:"one"`
		B int    `dynamo:"wrong2" dynamodbav:"two"`
	}
	var list []foo
	expect1 := foo{
		A: "test",
		B: 555,
	}
	expect2 := foo{
		A: "two",
		B: 222,
	}
	err := unmarshalAppend(map[string]*dynamodb.AttributeValue{
		"one": &dynamodb.AttributeValue{S: aws.String("test")},
		"two": &dynamodb.AttributeValue{N: aws.String("555")},
	}, AWSEncoding(&list))
	if err != nil {
		t.Error(err)
	}
	if len(list) != 1 && reflect.DeepEqual(list, []foo{expect1}) {
		t.Error("bad AWS unmarshal append:", list)
	}
	err = unmarshalAppend(map[string]*dynamodb.AttributeValue{
		"one": &dynamodb.AttributeValue{S: aws.String("two")},
		"two": &dynamodb.AttributeValue{N: aws.String("222")},
	}, AWSEncoding(&list))
	if err != nil {
		t.Error(err)
	}
	if len(list) != 2 && reflect.DeepEqual(list, []foo{expect1, expect2}) {
		t.Error("bad AWS unmarshal append:", list)
	}
}
