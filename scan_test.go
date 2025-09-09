package dynamo

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestScan(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	table := testDB.Table(testTableWidgets)
	ctx := context.TODO()

	// first, add an item to make sure there is at least one
	item := widget{
		UserID: 42,
		Time:   time.Now().UTC(),
		Msg:    "hello",
	}
	err := table.Put(item).Run(ctx)
	if err != nil {
		t.Error("unexpected error:", err)
	}

	// count items via Query
	ct, err := table.Get("UserID", 42).Consistent(true).Count(ctx)
	if err != nil {
		t.Error("unexpected error:", err)
	}

	// now check if get all and count return the same amount of items
	t.Run("All", func(t *testing.T) {
		var result []widget
		var cc ConsumedCapacity
		err = table.Scan().Filter("UserID = ?", 42).Consistent(true).ConsumedCapacity(&cc).All(ctx, &result)
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
	})

	// check this against Scan's count, too
	t.Run("Count", func(t *testing.T) {
		var cc2 ConsumedCapacity
		scanCt, err := table.Scan().Filter("UserID = ?", 42).Consistent(true).ConsumedCapacity(&cc2).Count(ctx)
		if err != nil {
			t.Error("unexpected error:", err)
		}
		if scanCt != ct {
			t.Errorf("scan count and get count don't match. scan count: %d, get count: %d", scanCt, ct)
		}
		if cc2.Total == 0 {
			t.Error("bad consumed capacity", cc2)
		}
	})

	t.Run("AllParallel", func(t *testing.T) {
		var result2 []widget
		var cc3 ConsumedCapacity
		err = table.Scan().Filter("UserID = ?", 42).Consistent(true).ConsumedCapacity(&cc3).AllParallel(context.Background(), 4, &result2)
		if err != nil {
			t.Error("unexpected error:", err)
		}
		if int(ct) != len(result2) {
			t.Errorf("count and scan don't match. count: %d, scan: %d", ct, len(result2))
		}
		if cc3.Total == 0 {
			t.Error("bad consumed capacity", cc3)
		}

		// search for our inserted item
		found := false
		for _, w := range result2 {
			if w.Time.Equal(item.Time) && reflect.DeepEqual(w, item) {
				found = true
				break
			}
		}
		if !found {
			t.Error("exact match of put item not found in scan")
		}
	})
}

func TestScanPaging(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	table := testDB.Table(testTableWidgets)
	ctx := context.TODO()

	// prepare data
	insert := make([]interface{}, 10)
	for i := 0; i < len(insert); i++ {
		insert[i] = widget{
			UserID: 2068,
			Time:   time.Date(2068, 1, i+1, 0, 0, 0, 0, time.UTC),
			Msg:    "garbage",
		}
	}
	if _, err := table.Batch().Write().Put(insert...).Run(ctx, "test"); err != nil {
		t.Fatal(err)
	}

	t.Run("synchronous", func(t *testing.T) {
		widgets := [10]widget{}
		itr := table.Scan().Consistent(true).SearchLimit(1).Iter()
		for i := 0; i < len(widgets); i++ {
			itr.Next(ctx, &widgets[i])
			if itr.Err() != nil {
				t.Error("unexpected error", itr.Err())
				break
			}
			lek, err := itr.LastEvaluatedKey(context.Background())
			if err != nil {
				t.Error("LEK error", err)
			}
			itr = table.Scan().StartFrom(lek).SearchLimit(1).Iter()
		}
		for i, w := range widgets {
			if w.UserID == 0 && w.Time.IsZero() {
				t.Error("scan didn't find item", i, "got:", w)
			}
		}
	})

	t.Run("parallel", func(t *testing.T) {
		const segments = 2
		ctx := context.Background()
		widgets := [10]widget{}
		limit := int(len(widgets) / segments)
		itr := table.Scan().Consistent(true).SearchLimit(limit).IterParallel(ctx, segments)
		for i := 0; i < len(widgets); {
			for ; i < len(widgets) && itr.Next(ctx, &widgets[i]); i++ {
			}
			if itr.Err() != nil {
				t.Error("unexpected error", itr.Err())
				break
			}
			t.Logf("parallel chunk: %d", i)
			lek, err := itr.LastEvaluatedKeys(ctx)
			if err != nil {
				t.Fatal("lek error", err)
			}
			itr = table.Scan().SearchLimit(limit).IterParallelStartFrom(ctx, lek)
		}
		for i, w := range widgets {
			if w.UserID == 0 && w.Time.IsZero() {
				t.Error("scan didn't find item", i, "got:", w)
			}
		}
	})
}

func TestScanMagicLEK(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}

	testDB0 := *testDB
	testDB0.descs = new(sync.Map)
	freshTestDB := &testDB0

	table := freshTestDB.Table(testTableWidgets)
	ctx := context.Background()

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
	// prepare data
	if _, err := table.Batch().Write().Put(widgets...).Run(ctx, "test"); err != nil {
		t.Fatal(err)
	}

	t.Run("having to use DescribeTable", func(t *testing.T) {
		itr := table.Scan().Filter("'Msg' = ?", "TestScanMagicLEK").Limit(2).Iter()
		for i := 0; i < len(widgets); i++ {
			var w widget
			itr.Next(ctx, &w)
			if itr.Err() != nil {
				t.Error("unexpected error", itr.Err())
			}
			lek, err := itr.LastEvaluatedKey(context.Background())
			if err != nil {
				t.Error("LEK error", err)
			}
			itr = table.Scan().Filter("'Msg' = ?", "TestScanMagicLEK").StartFrom(lek).Limit(2).Iter()
		}
	})

	t.Run("via index", func(t *testing.T) {
		itr := table.Scan().Index("Msg-Time-index").Filter("UserID = ?", 2069).Limit(2).Iter()
		for i := 0; i < len(widgets); i++ {
			var w widget
			itr.Next(ctx, &w)
			if itr.Err() != nil {
				t.Error("unexpected error", itr.Err())
			}
			lek, err := itr.LastEvaluatedKey(context.Background())
			if err != nil {
				t.Error("LEK error", err)
			}
			itr = table.Scan().Index("Msg-Time-index").Filter("UserID = ?", 2069).StartFrom(lek).Limit(2).Iter()
		}
	})

	t.Run("table cache", func(t *testing.T) {
		pk, err := table.primaryKeys(context.Background(), nil, nil, "")
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
