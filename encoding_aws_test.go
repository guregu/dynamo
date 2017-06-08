package dynamo

import (
	"reflect"
	"testing"
	"time"

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
