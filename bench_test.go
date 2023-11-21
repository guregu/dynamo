package dynamo

import (
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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
		TestText:    time.Now(),
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
	b.ResetTimer()

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

func BenchmarkUnmarshal3(b *testing.B) {
	var got widget
	rv := reflect.ValueOf(&got)
	r, _ := getDecodePlan(rv.Type())
	// x := newRecipe(rv)
	for i := 0; i < b.N; i++ {
		if err := r.decodeItem(exampleItem, rv); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnmarshalText(b *testing.B) {
	// te := textMarshaler(true)
	got := struct {
		Foo textMarshaler
	}{}

	b.Run("new", func(b *testing.B) {
		rv := reflect.ValueOf(&got)
		// x := newRecipe(rv)
		for i := 0; i < b.N; i++ {
			r, _ := getDecodePlan(rv.Type())
			if err := r.decodeItem(map[string]*dynamodb.AttributeValue{
				"Foo": &dynamodb.AttributeValue{S: aws.String("true")},
			}, rv); err != nil {
				b.Fatal(err)
			}
			if got.Foo != true {
				b.Fatal("bad")
			}
		}
	})
}

func BenchmarkUnmarshalAppend(b *testing.B) {
	items := make([]Item, 10_000)
	for i := range items {
		items[i] = Item{
			"Hello": &dynamodb.AttributeValue{S: aws.String("world")},
		}
	}
	b.ResetTimer()

	dst := make([]struct {
		Hello string
	}, 0, len(items))
	for i := 0; i < b.N; i++ {
		for j := range items {
			if err := unmarshalAppend(items[j], &dst); err != nil {
				b.Fatal(err)
			}
		}
		if len(dst) != len(items) {
			b.Fatal("bad")
		}
		dst = dst[:0]
	}
}

func BenchmarkUnmarshalAppend2(b *testing.B) {
	items := make([]Item, 10_000)
	for i := range items {
		items[i] = Item{
			"Hello": &dynamodb.AttributeValue{S: aws.String("world")},
		}
	}
	b.ResetTimer()

	dst := make([]struct {
		Hello string
	}, 0, len(items))
	do := unmarshalAppendTo(&dst)
	for i := 0; i < b.N; i++ {
		for j := range items {
			if err := do(items[j], &dst); err != nil {
				b.Fatal(err)
			}
		}
		if len(dst) != len(items) {
			b.Fatal("bad")
		}
		dst = dst[:0]
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

	TestText  time.Time
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
