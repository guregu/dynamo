package dynamo

import (
	"reflect"
	"testing"
	"time"
)

func TestScan(t *testing.T) {
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
	var cc ConsumedCapacity
	err = table.Scan().Filter("UserID = ?", 42).Consistent(true).ConsumedCapacity(&cc).All(&result)
	if err != nil {
		t.Error("unexpected error:", err)
	}

	ct, err := table.Get("UserID", 42).Consistent(true).Count()
	if err != nil {
		t.Error("unexpected error:", err)
	}
	if int(ct) != len(result) {
		t.Errorf("count and scan don't match. count: %d, scan: %d", ct, len(result))
	}
	if cc.Total == 0 {
		t.Error("bad consumed capacity", cc)
	}

	// search for our inserted item
	found := false
	for _, w := range result {
		if w.Time.Equal(item.Time) && reflect.DeepEqual(w, item) {
			found = true
			break
		}
	}
	if !found {
		t.Error("exact match of put item not found in scan")
	}
}

func TestScanPaging(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	table := testDB.Table(testTable)

	widgets := [10]widget{}
	itr := table.Scan().SearchLimit(1).Iter()
	for i := 0; i < len(widgets); i++ {
		more := itr.Next(&widgets[i])
		if itr.Err() != nil {
			t.Error("unexpected error", itr.Err())
		}
		if !more {
			break
		}
		itr = table.Scan().StartFrom(itr.LastEvaluatedKey()).SearchLimit(1).Iter()
	}
}
