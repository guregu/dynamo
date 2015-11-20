package dynamo

import (
	"testing"
	"time"

	"github.com/guregu/toki"
)

func BenchmarkEncode(b *testing.B) {
	i := 500
	item := fancyObject{
		User:        666,
		Test:        customMarshaler(1),
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

type fancyObject struct {
	User      int `dynamo:"UserID"`
	Test      customMarshaler
	ContentID string
	Page      int
	SkipThis  string `dynamo:"-"`
	Bonus     *int   `dynamo:",omitempty"`

	TestText  toki.Time
	SkipMePlz time.Time `dynamo:",omitempty"`

	StringSlice []string

	embedMe
	Greeting other

	Features  map[string]bool
	Something interface{}

	Check SuperComplex
}

type embedMe struct {
	Extra bool
}

type other struct {
	Hello string
}

type SuperComplex []struct {
	HelpMe struct {
		FFF []int `dynamo:",set"`
	}
}

func makeSuperComplex() SuperComplex {
	sc := make(SuperComplex, 2)
	sc[0].HelpMe.FFF = []int{1, 2, 3}
	return sc
}
