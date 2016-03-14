package dynamo

import (
	"testing"
	"time"
)

type UserAction struct {
	UserID string    `dynamo:"ID,hash" index:"Seq-ID-index,range"`
	Time   time.Time `dynamo:",range"`
	Seq    int64     `localIndex:"ID-Seq-index,range" index:"Seq-ID-index,hash"`
	UUID   string    `index:"UUID-index,hash"`
}

func TestCreateTable(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	// if err := testDB.CreateTable("UserActions", UserAction{}).Run(); err != nil {
	// 	t.Error(err)
	// }
	// t.Fail()
}
