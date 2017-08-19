package dynamo

import (
	"testing"
)

func TestDescribeTable(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	table := testDB.Table(testTable)

	desc, err := table.Describe().Run()
	if err != nil {
		t.Error(err)
		return
	}

	if desc.Name != testTable {
		t.Error("wrong name:", desc.Name, "≠", testTable)
	}
	if desc.HashKey != "UserID" || desc.RangeKey != "Time" {
		t.Error("bad keys:", desc.HashKey, desc.RangeKey)
	}
}
