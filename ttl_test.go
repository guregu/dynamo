package dynamo

import (
	"testing"
)

func TestDescribeTTL(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	table := testDB.Table(testTable)

	desc, err := table.DescribeTTL().Run()
	if err != nil {
		t.Error(err)
		return
	}

	if desc.Status != TTLDisabled {
		t.Error("wrong status:", desc.Status, "≠", TTLDisabled)
	}
}

// disable until we have local DB tests
// (AWS doesn't let us change the TTL often enough to test on a real DB)

// func TestUpdateTTL(t *testing.T) {
// 	if testDB == nil {
// 		t.Skip(offlineSkipMsg)
// 	}
// 	table := testDB.Table(testTable)

// 	err := table.UpdateTTL("Date", true).Run()
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	desc, err := table.DescribeTTL().Run()
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}
// 	if desc.Status != TTLEnabled && desc.Status != TTLEnabling {
// 		t.Error("wrong status:", desc.Status, "≠ ENABLED or ENABLING")
// 	}
// 	if desc.Attribute != "Date" {
// 		t.Error("wrong attribute:", desc.Attribute, "≠", "Date")
// 	}
// }
