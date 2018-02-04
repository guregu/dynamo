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
	var cc ConsumedCapacity
	err = table.Put(newItem).ConsumedCapacity(&cc).OldValue(&oldValue)
	if err != nil {
		t.Error("unexpected error:", err)
	}

	if !reflect.DeepEqual(oldValue, item) {
		t.Errorf("bad old value. %#v ≠ %#v", oldValue, item)
	}

	if cc.Total != 1 || cc.Table != 1 || cc.TableName != testTable {
		t.Errorf("bad consumed capacity: %#v", cc)
	}

	// putting the same item: this should fail
	err = table.Put(newItem).If("attribute_not_exists(UserID)").Run()
	if !isConditionalCheckErr(err) {
		t.Error("expected ConditionalCheckFailedException, not", err)
	}
}
