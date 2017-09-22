package dynamo

import (
	"reflect"
	"testing"
	"time"
)

func TestGetAllCount(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	table := testDB.Table(testTable)

	// first, add an item to make sure there is at least one
	item := widget{
		UserID: 42,
		Time:   time.Now().UTC(),
		Msg:    "hello",
		Meta: map[string]string{
			"foo":        "bar",
			"animal.cow": "moo",
		},
	}
	err := table.Put(item).Run()
	if err != nil {
		t.Error("unexpected error:", err)
	}

	// now check if get all and count return the same amount of items
	var result []widget
	err = table.Get("UserID", 42).Consistent(true).Filter("Msg = ?", item.Msg).All(&result)
	if err != nil {
		t.Error("unexpected error:", err)
	}

	ct, err := table.Get("UserID", 42).Consistent(true).Filter("Msg = ?", item.Msg).Count()
	if err != nil {
		t.Error("unexpected error:", err)
	}
	if int(ct) != len(result) {
		t.Errorf("count and GetAll don't match. count: %d, get all: %d", ct, len(result))
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
		t.Error("exact match of put item not found in get all")
	}

	// query specifically against the inserted item (using GetItem)
	var one widget
	err = table.Get("UserID", 42).Range("Time", Equal, item.Time).Consistent(true).One(&one)
	if err != nil {
		t.Error("unexpected error:", err)
	}
	if !reflect.DeepEqual(one, item) {
		t.Errorf("bad result for get one. %v ≠ %v", one, item)
	}

	// query specifically against the inserted item (using Query)
	one = widget{}
	err = table.Get("UserID", 42).Range("Time", Equal, item.Time).Filter("Msg = ?", item.Msg).Consistent(true).One(&one)
	if err != nil {
		t.Error("unexpected error:", err)
	}
	if !reflect.DeepEqual(one, item) {
		t.Errorf("bad result for get one. %v ≠ %v", one, item)
	}

	// GetItem + Project
	one = widget{}
	projected := widget{
		UserID: item.UserID,
		Time:   item.Time,
	}
	err = table.Get("UserID", 42).Range("Time", Equal, item.Time).Project("UserID", "Time").Consistent(true).One(&one)
	if err != nil {
		t.Error("unexpected error:", err)
	}
	if !reflect.DeepEqual(one, projected) {
		t.Errorf("bad result for get one+project. %v ≠ %v", one, projected)
	}

	// GetItem + ProjectExpr
	one = widget{}
	projected = widget{
		UserID: item.UserID,
		Time:   item.Time,
		Meta: map[string]string{
			"foo":        "bar",
			"animal.cow": "moo",
		},
	}
	err = table.Get("UserID", 42).Range("Time", Equal, item.Time).ProjectExpr("UserID, $, Meta.foo, Meta.$", "Time", "animal.cow").Consistent(true).One(&one)
	if err != nil {
		t.Error("unexpected error:", err)
	}
	if !reflect.DeepEqual(one, projected) {
		t.Errorf("bad result for get one+project. %v ≠ %v", one, projected)
	}

}
