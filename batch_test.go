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

func TestBatchGetEmptySets(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	table := testDB.Table(testTable)

	now := time.Now().UnixNano() / 1000000000
	id := int(now)
	entry := widget{UserID: id, Time: time.Now()}
	if err := table.Put(entry).Run(); err != nil {
		panic(err)
	}
	entry2 := widget{UserID: id + batchSize*2, Time: entry.Time}
	if err := table.Put(entry2).Run(); err != nil {
		panic(err)
	}

	keysToCheck := []Keyed{}
	for i := entry.UserID; i <= entry2.UserID; i += 1 {
		keysToCheck = append(keysToCheck, Keys{i, entry.Time})
	}

	results := []widget{}
	err := table.Batch("UserID", "Time").Get(keysToCheck...).Consistent(true).All(&results)
	if err != nil {
		t.Error(err)
	}
	if len(results) != 2 {
		t.Error("batch get empty set, unexpected length:", len(results), "want:", 2)
	}

	if err := table.Delete("UserID", entry.UserID).Range("Time", entry.Time).Run(); err != nil {
		panic(err)
	}

	results = []widget{}
	err = table.Batch("UserID", "Time").Get(keysToCheck...).Consistent(true).All(&results)
	if err != nil {
		t.Error(err)
	}
	if len(results) != 1 {
		t.Error("batch get empty set, unexpected length:", len(results), "want:", 1)
	}

	results = []widget{}
	err = table.Batch("UserID", "Time").Get(keysToCheck[:len(keysToCheck)-1]...).Consistent(true).All(&results)
	if err != ErrNotFound {
		t.Error(err)
	}
	if len(results) != 0 {
		t.Error("batch get empty set, unexpected length:", len(results), "want:", 0)
	}
}

func TestBatchEmptyInput(t *testing.T) {
	table := testDB.Table(testTable)
	err := table.Batch("UserID", "Time").Get().All(nil)
	if err != ErrNoInput {
		t.Error("unexpected error", err)
	}

	_, err = table.Batch("UserID", "Time").Write().Run()
	if err != ErrNoInput {
		t.Error("unexpected error", err)
	}
}
