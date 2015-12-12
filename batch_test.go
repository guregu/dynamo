package dynamo

import (
	"testing"
	"time"
)

func TestBatchGetWrite(t *testing.T) {
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
	// add another item
	item2 := widget{
		UserID: 613,
		Time:   time.Now().UTC(),
		Msg:    "hello",
	}
	err := table.Batch().Write().Put(item, item2).Run()
	if err != nil {
		t.Error("unexpected error:", err)
	}

	// get both
	var results []widget
	err = table.Batch("UserID", "Time").
		Get(Keys{item.UserID, item.Time}, Keys{item2.UserID, item2.Time}).
		Consistent(true).
		All(&results)
	if err != nil {
		t.Error("unexpected error:", err)
	}

	if len(results) != 2 {
		t.Error("expected 2 results, got", len(results))
	}
	for _, result := range results {
		if result.UserID != item.UserID && result.UserID != item2.UserID {
			t.Error("unexpected result", result)
		}
	}

	// delete both
	err = table.Batch("UserID", "Time").Write().
		Delete(Keys{item.UserID, item.Time}, Keys{item2.UserID, item2.Time}).Run()
	if err != nil {
		t.Error("unexpected error:", err)
	}

	// get both again
	results = nil
	err = table.Batch("UserID", "Time").
		Get(Keys{item.UserID, item.Time}, Keys{item2.UserID, item2.Time}).
		Consistent(true).
		All(&results)
	if err != ErrNotFound {
		t.Error("expected ErrNotFound, got", err)
	}
	if len(results) != 0 {
		t.Error("expected 0 results, got", len(results))
	}
}
