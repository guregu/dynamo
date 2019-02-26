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

	// basic write & check
	table := testDB.Table(testTable)
	tx := testDB.WriteTx()
	var cc, ccold ConsumedCapacity
	tx.Idempotent(true)
	tx.Put(table.Put(widget1))
	tx.Put(table.Put(widget2))
	tx.Check(table.Check("UserID", 69).Range("Time", date3).IfNotExists())
	tx.ConsumedCapacity(&cc)
	err := tx.Run()
	if err != nil {
		t.Error(err)
	}
	if cc.Total == 0 {
		t.Error("bad consumed capacity:", cc)
	}
	ccold = cc

	err = tx.Run()
	if err != nil {
		t.Error(err)
	}
	if cc.Total <= ccold.Total {
		t.Error("bad consumed capacity:", cc.Total, ccold.Total)
	}
	if cc.Read <= ccold.Read {
		t.Error("bad consumed capacity:", cc.Read, ccold.Read)
	}
	if cc.Write != ccold.Write {
		t.Error("bad consumed capacity:", cc.Write, "≠", ccold.Write)
	}

	// GetOne
	getTx := testDB.GetTx()
	var record1, record2, record3 widget
	var cc2 ConsumedCapacity
	getTx.GetOne(table.Get("UserID", 69).Range("Time", Equal, date1), &record1)
	getTx.GetOne(table.Get("UserID", 69).Range("Time", Equal, date2), &record2)
	getTx.GetOne(table.Get("UserID", 69).Range("Time", Equal, date3), &record3)
	getTx.ConsumedCapacity(&cc2)
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
	if cc2.Total == 0 {
		t.Error("bad consumed capacity:", cc2)
	}

	// All
	oldCC2 := cc2
	var records []widget
	err = getTx.All(&records)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(records, []widget{widget1, widget2}) {
		t.Error("bad results:", records)
	}
	if cc2.Total == oldCC2.Total {
		t.Error("consumed capacity didn't increase", cc2, oldCC2)
	}

	// Check & Update
	widget2.Msg = "bird"
	tx = testDB.WriteTx()
	tx.Check(table.Check("UserID", widget1.UserID).Range("Time", widget1.Time).If("Msg = ?", widget1.Msg))
	tx.Update(table.Update("UserID", widget2.UserID).Range("Time", widget2.Time).Set("Msg", widget2.Msg))
	if err = tx.Run(); err != nil {
		t.Error(err)
	}

	// Delete
	tx = testDB.WriteTx()
	tx.Delete(table.Delete("UserID", widget1.UserID).Range("Time", widget1.Time).If("Msg = ?", widget1.Msg))
	tx.Delete(table.Delete("UserID", widget2.UserID).Range("Time", widget2.Time).If("Msg = ?", widget2.Msg))
	if err = tx.Run(); err != nil {
		t.Error(err)
	}

	// zero results
	if err = getTx.Run(); err != ErrNotFound {
		t.Error("expected ErrNotFound, got:", err)
	}

	// TransactionCanceledException
	tx = testDB.WriteTx()
	tx.Put(table.Put(widget{UserID: 69, Time: date1}).If("'Msg' = ?", "should not exist"))
	tx.Put(table.Put(widget{UserID: 69, Time: date2}))
	tx.Check(table.Check("UserID", 69).Range("Time", date3).IfExists().If("Msg = ?", "don't exist foo"))
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
