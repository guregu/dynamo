package dynamo

import (
	"testing"
)

func TestUpdate(t *testing.T) {
	db := testDB()
	hits := db.Table("TestDB")

	var ctr struct{ Counter int }

	err := hits.Update("UserID", -1).Range("Date", -1).Add("Counter", 1).Remove("Test").Value(&ctr)
	if err != nil {
		t.Error("err", err)
	}
	t.Logf("updated: %#v", ctr)

	t.Fail()
}
