package dynamo

import (
	"reflect"
	"testing"
	"time"
)

func TestPut(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}

	table := testDB.Table(testTable)

	now := time.Now().UTC()
	item := widget{
		UserID: 42,
		Time:   now,
		Msg:    "old",
	}

	err := table.Put(item).Run()
	if err != nil {
		t.Error("unexpected error:", err)
	}

	newItem := widget{
		UserID: 42,
		Time:   now,
		Msg:    "new",
	}
	var oldValue widget
	err = table.Put(newItem).OldValue(&oldValue)
	if err != nil {
		t.Error("unexpected error:", err)
	}

	if !reflect.DeepEqual(item, oldValue) {
		t.Errorf("bad old value. %#v ≠ %#v", item, oldValue)
	}
}
