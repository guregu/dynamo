package dynamo

import (
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
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

	// check this against Scan's count, too
	var cc2 ConsumedCapacity
	scanCt, err := table.Scan().Filter("UserID = ?", 42).Consistent(true).ConsumedCapacity(&cc2).Count()
	if err != nil {
		t.Error("unexpected error:", err)
	}
	if scanCt != ct {
		t.Errorf("scan count and get count don't match. scan count: %d, get count: %d", scanCt, ct)
	}
	if cc2.Total == 0 {
		t.Error("bad consumed capacity", cc2)
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

func TestScanMagicLEK(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	table := testDB.Table(testTable)

	widgets := []interface{}{
		widget{
			UserID: 2069,
			Time:   time.Date(2069, 4, 00, 0, 0, 0, 0, time.UTC),
			Msg:    "TestScanMagicLEK",
		},
		widget{
			UserID: 2069,
			Time:   time.Date(2069, 4, 10, 0, 0, 0, 0, time.UTC),
			Msg:    "TestScanMagicLEK",
		},
		widget{
			UserID: 2069,
			Time:   time.Date(2069, 4, 20, 0, 0, 0, 0, time.UTC),
			Msg:    "TestScanMagicLEK",
		},
	}

	t.Run("prepare data", func(t *testing.T) {
		if _, err := table.Batch().Write().Put(widgets...).Run(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("having to use DescribeTable", func(t *testing.T) {
		itr := table.Scan().Filter("'Msg' = ?", "TestScanMagicLEK").Limit(2).Iter()
		for i := 0; i < len(widgets); i++ {
			var w widget
			itr.Next(&w)
			if itr.Err() != nil {
				t.Error("unexpected error", itr.Err())
			}
			itr = table.Scan().Filter("'Msg' = ?", "TestScanMagicLEK").StartFrom(itr.LastEvaluatedKey()).Limit(2).Iter()
		}
	})

	t.Run("via index", func(t *testing.T) {
		itr := table.Scan().Index("Msg-Time-index").Filter("UserID = ?", 2069).Limit(2).Iter()
		for i := 0; i < len(widgets); i++ {
			var w widget
			itr.Next(&w)
			if itr.Err() != nil {
				t.Error("unexpected error", itr.Err())
			}
			itr = table.Scan().Index("Msg-Time-index").Filter("UserID = ?", 2069).StartFrom(itr.LastEvaluatedKey()).Limit(2).Iter()
		}
	})

	t.Run("table cache", func(t *testing.T) {
		pk, err := table.primaryKeys(aws.BackgroundContext(), nil, nil, "")
		if err != nil {
			t.Fatal(err)
		}
		expect := map[string]struct{}{
			"UserID": {},
			"Time":   {},
		}
		if !reflect.DeepEqual(pk, expect) {
			t.Error("unexpected key cache. want:", expect, "got:", pk)
		}
	})
}
