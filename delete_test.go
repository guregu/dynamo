package dynamo

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestDelete(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	ctx := context.TODO()
	table := testDB.Table(testTableWidgets)

	// first, add an item to delete later
	item := widget{
		UserID: 42,
		Time:   time.Now().UTC(),
		Msg:    "hello",
		Meta: map[string]string{
			"color": "octarine",
		},
	}
	err := table.Put(item).Run(ctx)
	if err != nil {
		t.Error("unexpected error:", err)
	}

	// fail to delete it
	var curr widget
	wrote, err := table.Delete("UserID", item.UserID).
		Range("Time", item.Time).
		If("Meta.'color' = ?", "octarine").
		If("Msg = ?", "wrong msg").
		CurrentValue(ctx, &curr)
	if wrote {
		t.Error("wrote should be false")
	}
	if !reflect.DeepEqual(curr, item) {
		t.Errorf("bad value. %#v ≠ %#v", curr, item)
	}

	// delete it
	var old widget
	var cc ConsumedCapacity
	err = table.Delete("UserID", item.UserID).Range("Time", item.Time).ConsumedCapacity(&cc).OldValue(ctx, &old)
	if err != nil {
		t.Error("unexpected error:", err)
	}
	if !reflect.DeepEqual(old, item) {
		t.Errorf("bad old value. %#v ≠ %#v", old, item)
	}
	if cc.Total < 1 {
		t.Error("invalid ConsumedCapacity", cc)
	}
}
