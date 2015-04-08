package dynamo

import (
	"testing"
)

func TestUpdate(t *testing.T) {
	db := testDB()
	hits := db.Table("TestDB")

	err := hits.Update("UserID", -1).Range("Date", -1).Add("Counter", 1).Remove("Test").Run()
	if err != nil {
		t.Error("err", err)
	}
	t.Fail()
}
