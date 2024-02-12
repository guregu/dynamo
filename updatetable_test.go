package dynamo

import (
	"testing"
)

// TODO: enable this test
func _TestUpdateTable(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	table := testDB.Table(testTableWidgets)

	desc, err := table.UpdateTable().CreateIndex(Index{
		Name:              "test123",
		HashKey:           "Time",
		HashKeyType:       StringType,
		RangeKey:          "UserID",
		RangeKeyType:      NumberType,
		ProjectionType:    IncludeProjection,
		ProjectionAttribs: []string{"Msg"},
		Throughput: Throughput{
			Read:  1,
			Write: 1,
		},
	}).Run()

	// desc, err := table.UpdateTable().DeleteIndex("test123").Run()

	// spew.Dump(desc, err)
	// desc, err := table.UpdateTable().Provision(2, 1).Run()
	if err != nil {
		t.Error(err)
	}
	if desc.Name != testTableWidgets {
		t.Error("wrong name:", desc.Name, "≠", testTableWidgets)
	}
	if desc.Status != UpdatingStatus {
		t.Error("bad status:", desc.Status, "≠", UpdatingStatus)
	}
}
