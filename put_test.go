package dynamo

import (
	"testing"
	"time"

	"github.com/guregu/toki"
)

func TestPutItem(t *testing.T) {
	db := testDB()
	hits := db.Table("TestDB")
	i := 777777
	h := hit{
		User:        666,
		Date:        unixTime{time.Now()},
		ContentID:   "監獄学園",
		Page:        1,
		SkipThis:    "i should disappear",
		Bonus:       &i,
		TestText:    toki.MustParseTime("1:2:3"),
		StringSlice: []string{"A", "B", "C", "QQQ"},
	}

	err := hits.Put(&h)

	t.Log(err)
	t.Fail()
}
