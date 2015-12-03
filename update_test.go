package dynamo

import (
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
)

func TestUpdate(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	table := testDB.Table(testTable)

	// first, add an item to make sure there is at least one
	item := widget{
		UserID: 42,
		Time:   time.Now().UTC(),
		Msg:    "hello",
		Count:  0,
	}
	err := table.Put(item).Run()
	if err != nil {
		t.Error("unexpected error:", err)
	}

	// change it a bit and check the result
	var result widget
	err = table.Update("UserID", item.UserID).
		Range("Time", item.Time).
		Set("Msg", "changed").
		Add("Count", 1).
		Value(&result)
	expected := widget{
		UserID: item.UserID,
		Time:   item.Time,
		Msg:    "changed",
		Count:  1,
	}
	if err != nil {
		t.Error("unexpected error:", err)
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("bad result. %+v ≠ %+v", result, expected)
	}

	// send an update with a failing condition
	err = table.Update("UserID", item.UserID).
		Range("Time", item.Time).
		Set("Msg", "shouldn't happen").
		Add("Count", 1).
		If("$ > ?", "Count", 100).
		Value(&result)
	if ae := err.(awserr.RequestFailure); ae.Code() != "ConditionalCheckFailedException" {
		t.Error("expected ConditionalCheckFailedException, not", err)
	}
}
