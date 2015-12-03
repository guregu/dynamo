package dynamo

import (
	"testing"
	"time"

	"github.com/guregu/toki"
)

var (
	arbitraryNumber   = 555
	veryComplexObject = fancyObject{
		User:        613,
		Test:        customMarshaler(1),
		ContentID:   "監獄学園",
		Page:        1,
		SkipThis:    "i should disappear",
		Bonus:       &arbitraryNumber,
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
)

func BenchmarkEncodeSimple(b *testing.B) {
	item := simpleObject{
		User:  666,
		Other: "hello",
	}

	for n := 0; n < b.N; n++ {
		marshalItem(&item)
	}
}

func BenchmarkEncodeSimpleMap(b *testing.B) {
	item := map[string]interface{}{
		"User":  666,
		"Other": "hello",
	}

	for n := 0; n < b.N; n++ {
		marshalItem(&item)
	}
}

func BenchmarkDecodeSimple(b *testing.B) {
	item := simpleObject{
		User:  666,
		Other: "hello",
	}
	av, _ := marshalItem(item)

	var out simpleObject
	for n := 0; n < b.N; n++ {
		unmarshalItem(av, &out)
	}
}

func BenchmarkDecodeSimpleMap(b *testing.B) {
	item := simpleObject{
		User:  666,
		Other: "hello",
	}
	av, _ := marshalItem(item)

	var out map[string]interface{}
	for n := 0; n < b.N; n++ {
		unmarshalItem(av, &out)
	}
}

func BenchmarkEncodeVeryComplex(b *testing.B) {
	for n := 0; n < b.N; n++ {
		marshalItem(&veryComplexObject)
	}
}

func BenchmarkDecodeVeryComplex(b *testing.B) {
	av, _ := marshalItem(veryComplexObject)

	var out fancyObject
	for n := 0; n < b.N; n++ {
		unmarshalItem(av, &out)
	}
}

func BenchmarkDecodeVeryComplexMap(b *testing.B) {
	av, _ := marshalItem(veryComplexObject)

	var out map[string]interface{}
	for n := 0; n < b.N; n++ {
		unmarshalItem(av, &out)
	}
}

type simpleObject struct {
	User  int
	Other string
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
