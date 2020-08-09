package dynamo

import (
	"reflect"
	"testing"
	"time"
)

func TestUpdate(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	table := testDB.Table(testTable)

	// first, add an item to make sure there is at least one
	item := widget{
		UserID: 42,
		Time:   time.Now().UTC(),
		Msg:    "hello",
		Count:  0,
		Meta: map[string]string{
			"foo":  "bar",
			"nope": "痛",
		},
	}
	err := table.Put(item).Run()
	if err != nil {
		t.Error("unexpected error:", err)
	}

	// change it a bit and check the result
	var result widget
	var cc ConsumedCapacity
	err = table.Update("UserID", item.UserID).
		Range("Time", item.Time).
		Set("Msg", "changed").
		SetExpr("Meta.$ = ?", "foo", "baz").
		Add("Count", 1).
		Add("Test", []string{"A", "B"}).
		RemoveExpr("Meta.$", "nope").
		If("'Count' = ?", 0).
		If("'Msg' = ?", "hello").
		ConsumedCapacity(&cc).
		Value(&result)
	expected := widget{
		UserID: item.UserID,
		Time:   item.Time,
		Msg:    "changed",
		Count:  1,
		Meta: map[string]string{
			"foo": "baz",
		},
	}
	if err != nil {
		t.Error("unexpected error:", err)
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("bad result. %+v ≠ %+v", result, expected)
	}
	if cc.Total != 1 {
		t.Error("bad consumed capacity", cc)
	}

	// send an update with a failing condition
	err = table.Update("UserID", item.UserID).
		Range("Time", item.Time).
		Set("Msg", "shouldn't happen").
		Add("Count", 1).
		If("'Count' > ?", 100).
		If("MeaningOfLife = ?", 42).
		Value(&result)
	if !isConditionalCheckErr(err) {
		t.Error("expected ConditionalCheckFailedException, not", err)
	}
}

func TestUpdateNil(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	table := testDB.Table(testTable)

	// first, add an item to make sure there is at least one
	item := widget{
		UserID: 4242,
		Time:   time.Now().UTC(),
		Msg:    "delete me",
		Meta: map[string]string{
			"abc": "123",
		},
		Count: 100,
	}
	err := table.Put(item).Run()
	if err != nil {
		t.Error("unexpected error:", err)
		t.FailNow()
	}

	type widget2 struct {
		widget
		MsgPtr *string
	}

	// update Msg with 'nil', which should delete it
	var result widget2
	err = table.Update("UserID", item.UserID).Range("Time", item.Time).
		Set("Msg", "").
		Set("Meta.'abc'", nil).
		Set("Meta.'ok'", (*ptrTextMarshaler)(nil)).
		SetExpr("'Count' = ?", (*textMarshaler)(nil)).
		SetExpr("MsgPtr = ?", "").
		Value(&result)
	if err != nil {
		t.Error("unexpected error:", err)
	}
	expected := widget2{
		widget: widget{
			UserID: item.UserID,
			Time:   item.Time,
			Msg:    "",
			Meta: map[string]string{
				"ok": "null",
			},
		},
		MsgPtr: new(string),
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("bad result. %+v ≠ %+v", result, expected)
	}
}
