package dynamo

import (
	"reflect"
	"testing"
	"time"
)

func TestDelete(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	table := testDB.Table(testTable)

	// first, add an item to delete later
	item := widget{
		UserID: 42,
		Time:   time.Now().UTC(),
		Msg:    "hello",
	}
	err := table.Put(item).Run()
	if err != nil {
		t.Error("unexpected error:", err)
	}

	// fail to delete it
	err = table.Delete("UserID", item.UserID).
		Range("Time", item.Time).
		If("Msg = ?", "wrong msg").
		Run()
	if !isConditionalCheckErr(err) {
		t.Error("expected ConditionalCheckFailedException, not", err)
	}

	// delete it
	var old widget
	var cc ConsumedCapacity
	err = table.Delete("UserID", item.UserID).Range("Time", item.Time).ConsumedCapacity(&cc).OldValue(&old)
	if err != nil {
		t.Error("unexpected error:", err)
	}
	if !reflect.DeepEqual(old, item) {
		t.Errorf("bad old value. %#v ≠ %#v", old, item)
	}
	if cc.Total != 1 {
		t.Error("invalid ConsumedCapacity", cc)
	}
}
