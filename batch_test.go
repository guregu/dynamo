package dynamo

import (
	"testing"
	"time"
)

const batchSize = 101

func TestBatchGetWrite(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	table := testDB.Table(testTable)

	items := make([]interface{}, batchSize)
	widgets := make(map[int]widget)
	keys := make([]Keyed, batchSize)
	for i := 0; i < batchSize; i++ {
		now := time.Now().UTC()
		w := widget{
			UserID: i,
			Time:   now,
			Msg:    "batch test",
		}
		widgets[i] = w
		items[i] = w
		keys[i] = Keys{i, now}
	}

	var wcc ConsumedCapacity
	wrote, err := table.Batch().Write().Put(items...).ConsumedCapacity(&wcc).Run()
	if wrote != batchSize {
		t.Error("unexpected wrote:", wrote, "≠", batchSize)
	}
	if err != nil {
		t.Error("unexpected error:", err)
	}
	if wcc.Total == 0 {
		t.Error("bad consumed capacity", wcc)
	}

	// get all
	var results []widget
	var cc ConsumedCapacity
	err = table.Batch("UserID", "Time").
		Get(keys...).
		Consistent(true).
		ConsumedCapacity(&cc).
		All(&results)
	if err != nil {
		t.Error("unexpected error:", err)
	}

	if len(results) != batchSize {
		t.Error("expected", batchSize, "results, got", len(results))
	}

	if cc.Total == 0 {
		t.Error("bad consumed capacity", cc)
	}

	for _, result := range results {
		other := widgets[result.UserID]
		if result.UserID != other.UserID && !result.Time.Equal(other.Time) {
			t.Error("unexpected result", result, "≠", other)
		}
	}

	// delete both
	wrote, err = table.Batch("UserID", "Time").Write().
		Delete(keys...).Run()
	if wrote != batchSize {
		t.Error("unexpected wrote:", wrote, "≠", batchSize)
	}
	if err != nil {
		t.Error("unexpected error:", err)
	}

	// get both again
	results = nil
	err = table.Batch("UserID", "Time").
		Get(keys...).
		Consistent(true).
		All(&results)
	if err != ErrNotFound {
		t.Error("expected ErrNotFound, got", err)
	}
	if len(results) != 0 {
		t.Error("expected 0 results, got", len(results))
	}
}
