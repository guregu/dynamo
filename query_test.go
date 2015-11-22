package dynamo

import (
	"testing"
	"time"
)

func TestGetAllCount(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	table := testDB.Table(testTable)

	// first, add an item to make sure there is at least one
	item := widget{
		UserID: 42,
		Time:   time.Now().UTC(),
		Msg:    "hello",
	}
	err := table.Put(item).Run()
	if err != nil {
		t.Error("unexpected error:", err)
	}

	// now check if get all and count return the same amount of items
	var result []widget
	err = table.Get("UserID", 42).Consistent(true).All(&result)
	if err != nil {
		t.Error("unexpected error:", err)
	}

	ct, err := table.Get("UserID", 42).Consistent(true).Count()
	if err != nil {
		t.Error("unexpected error:", err)
	}

	if int(ct) != len(result) {
		t.Error("count and GetAll don't match. count: %d, get all: %d", ct, len(result))
	}
}
