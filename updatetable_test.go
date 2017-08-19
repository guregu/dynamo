package dynamo

import (
	"testing"
)

// TODO: enable this test
func _TestUpdateTable(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	table := testDB.Table(testTable)

	desc, err := table.UpdateTable().Provision(2, 1).Run()
	if err != nil {
		t.Error(err)
	}
	if desc.Name != testTable {
		t.Error("wrong name:", desc.Name, "≠", testTable)
	}
	if desc.Status != UpdatingStatus {
		t.Error("bad status:", desc.Status, "≠", UpdatingStatus)
	}
}
