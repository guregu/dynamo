package dynamo

import (
	"testing"
)

func TestScan(t *testing.T) {
	db := testDB()
	hits := db.Table("TestDB")

	itr := hits.Scan().Iter()
	var item hit
	for itr.Next(&item) {
		t.Logf("-- item %#v %v \n", item, itr.Err())
	}

	var items []hit
	hits.Scan().All(&items)
	t.Log("ITEMS", items)
	// t.Fail()
}
