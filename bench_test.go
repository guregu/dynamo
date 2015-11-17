package dynamo

import (
	"testing"
	"time"

	"github.com/guregu/toki"
)

func BenchmarkEncode(b *testing.B) {
	i := 500
	item := hit{
		User:        666,
		Date:        unixTime{time.Now()},
		ContentID:   "監獄学園",
		Page:        1,
		SkipThis:    "i should disappear",
		Bonus:       &i,
		TestText:    toki.MustParseTime("1:2:3"),
		StringSlice: []string{"A", "B", "C", "QQQ"},
		embedMe: embedMe{
			Extra: true,
		},
		Greeting: other{
			Hello: "world",
		},
		Features: map[string]bool{
			"課金":  true,
			"dlc": true,
		},
		Something: nil,
	}

	for n := 0; n < b.N; n++ {
		marshalStruct(&item)
	}
}
