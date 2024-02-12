package dynamo

import (
	"testing"
)

func TestDescribeTable(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	table := testDB.Table(testTableWidgets)

	desc, err := table.Describe().Run()
	if err != nil {
		t.Error(err)
		return
	}

	if desc.Name != testTableWidgets {
		t.Error("wrong name:", desc.Name, "≠", testTableWidgets)
	}
	if desc.HashKey != "UserID" || desc.RangeKey != "Time" {
		t.Error("bad keys:", desc.HashKey, desc.RangeKey)
	}
}
