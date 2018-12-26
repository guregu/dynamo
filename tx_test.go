package dynamo

import (
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
)

func TestTx(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}

	date1 := time.Date(1969, 1, 1, 1, 1, 1, 0, time.UTC)
	date2 := time.Date(1969, 2, 2, 2, 2, 2, 0, time.UTC)
	date3 := time.Date(1969, 3, 3, 3, 3, 3, 0, time.UTC)
	widget1 := widget{UserID: 69, Time: date1, Msg: "dog"}
	widget2 := widget{UserID: 69, Time: date2, Msg: "cat"}

	// basic write
	table := testDB.Table(testTable)
	tx := testDB.WriteTransaction()
	tx.Put(table.Put(widget1))
	tx.Put(table.Put(widget2))
	err := tx.Run()
	if err != nil {
		t.Error(err)
	}

	// GetOne
	getTx := testDB.GetTransaction()
	var record1, record2, record3 widget
	getTx.GetOne(table.Get("UserID", 69).Range("Time", Equal, date1), &record1)
	getTx.GetOne(table.Get("UserID", 69).Range("Time", Equal, date2), &record2)
	getTx.GetOne(table.Get("UserID", 69).Range("Time", Equal, date3), &record3)
	err = getTx.Run()
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(record1, widget1) {
		t.Error("bad results:", record1, "≠", widget1)
	}
	if !reflect.DeepEqual(record2, widget2) {
		t.Error("bad results:", record2, "≠", widget2)
	}
	if !reflect.DeepEqual(record3, widget{}) {
		t.Error("bad results:", record3, "≠", widget{})
	}

	// All
	var records []widget
	ctx, cancel := defaultContext()
	defer cancel()
	err = getTx.AllWithContext(ctx, &records)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(records, []widget{widget1, widget2}) {
		t.Error("bad results:", records)
	}

	// Delete & Check
	tx = testDB.WriteTransaction()
	// tx.Check(table.Get("UserID", widget1.UserID).Range("Time", Equal, widget1.Time).Filter("Msg = ?", "dog"))
	tx.Delete(table.Delete("UserID", widget1.UserID).Range("Time", widget1.Time))
	tx.Delete(table.Delete("UserID", widget2.UserID).Range("Time", widget2.Time))
	if err = tx.Run(); err != nil {
		t.Error(err)
	}

	// zero results
	// TODO: should we actually use ErrNotFound?
	// if err = getTx.Run(); err != ErrNotFound {
	// 	t.Error("expected ErrNotFound, got:", err)
	// }

	// TransactionCanceledException
	tx = testDB.WriteTransaction()
	tx.Put(table.Put(widget{UserID: 69, Time: date1}).If("'Msg' = ?", "should not exist"))
	tx.Put(table.Put(widget{UserID: 69, Time: date2}))
	err = tx.Run()
	if err == nil {
		t.Error("expected error")
	} else {
		if err.(awserr.Error).Code() != "TransactionCanceledException" {
			t.Error("unexpected error:", err)
		}
	}

	t.Logf("1: %+v 2: %+v 3: %+v", record1, record2, record3)
	t.Logf("All: %+v (len: %d)", records, len(records))
}
