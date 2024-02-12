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
	table1 := testDB.Table(testTableWidgets)
	table2 := testDB.Table(testTableSprockets)
	tables := []Table{table1, table2}
	totalBatchSize := batchSize * len(tables)

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

	var batches []*BatchWrite
	for _, table := range tables {
		b := table.Batch().Write().Put(items...)
		batches = append(batches, b)
	}
	batch1 := batches[0]
	batch1.Merge(batches[1:]...)
	var wcc ConsumedCapacity
	wrote, err := batch1.ConsumedCapacity(&wcc).Run()
	if wrote != totalBatchSize {
		t.Error("unexpected wrote:", wrote, "≠", totalBatchSize)
	}
	if err != nil {
		t.Error("unexpected error:", err)
	}
	if wcc.Total == 0 {
		t.Error("bad consumed capacity", wcc)
	}

	// get all
	var gets []*BatchGet
	for _, table := range tables {
		b := table.Batch("UserID", "Time").
			Get(keys...).
			Project("UserID", "Time").
			Consistent(true)
		gets = append(gets, b)
	}

	var cc ConsumedCapacity
	get1 := gets[0].ConsumedCapacity(&cc)
	get1.Merge(gets[1:]...)

	var results []widget
	err = get1.All(&results)
	if err != nil {
		t.Error("unexpected error:", err)
	}

	if len(results) != totalBatchSize {
		t.Error("expected", totalBatchSize, "results, got", len(results))
	}

	if cc.Total == 0 {
		t.Error("bad consumed capacity", cc)
	}

	for _, result := range results {
		other := widgets[result.UserID]
		if result.UserID != other.UserID && !result.Time.Equal(other.Time) {
			t.Error("unexpected result", result, "≠", other)
		}
		if result.Msg != "" {
			t.Error("projection not applied, want: blank. got:", result.Msg)
		}
	}

	// delete both
	wrote, err = table1.Batch("UserID", "Time").Write().
		Delete(keys...).
		DeleteInRange(table2, "UserID", "Time", keys...).
		Run()
	if wrote != totalBatchSize {
		t.Error("unexpected wrote:", wrote, "≠", totalBatchSize)
	}
	if err != nil {
		t.Error("unexpected error:", err)
	}

	// get both again
	{
		var results []widget
		err = table1.Batch("UserID", "Time").
			Get(keys...).
			FromRange(table2, "UserID", "Time", keys...).
			Consistent(true).
			All(&results)
		if err != ErrNotFound {
			t.Error("expected ErrNotFound, got", err)
		}
		if len(results) != 0 {
			t.Error("expected 0 results, got", len(results))
		}
	}
}

func TestBatchGetEmptySets(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	table := testDB.Table(testTableWidgets)

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
	table := testDB.Table(testTableWidgets)
	var out []any
	err := table.Batch("UserID", "Time").Get().All(&out)
	if err != ErrNoInput {
		t.Error("unexpected error", err)
	}

	_, err = table.Batch("UserID", "Time").Write().Run()
	if err != ErrNoInput {
		t.Error("unexpected error", err)
	}
}
