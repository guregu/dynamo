package dynamo

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func TestDecode3(t *testing.T) {
	want := exampleWant
	var got widget
	rv := reflect.ValueOf(&got)
	r := getDecodePlan(rv)
	if err := r.decodeItem(exampleItem, &got); err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Error("bad decode. want:", want, "got:", got)
	}
	// spew.Dump(got)
	// t.Fail()
}

// func BenchmarkUnmarshal2(b *testing.B) {
// 	var got widget
// 	insts := fieldsInStruct2(reflect.TypeOf(&got), nil)
// 	for i := 0; i < b.N; i++ {
// 		runInsts(insts, exampleItem, reflect.ValueOf(&got))
// 	}
// }

func BenchmarkUnmarshal3(b *testing.B) {
	var got widget
	rv := reflect.ValueOf(&got)
	// x := newRecipe(rv)
	for i := 0; i < b.N; i++ {
		r := getDecodePlan(rv)
		if err := r.decodeItem(exampleItem, &got); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnmarshalTE(b *testing.B) {
	// te := textMarshaler(true)
	got := struct {
		Foo textMarshaler
	}{}

	b.Run("new", func(b *testing.B) {
		rv := reflect.ValueOf(&got)
		// x := newRecipe(rv)
		for i := 0; i < b.N; i++ {
			r := getDecodePlan(rv)
			if err := r.decodeItem(map[string]*dynamodb.AttributeValue{
				"Foo": &dynamodb.AttributeValue{S: aws.String("true")},
			}, &got); err != nil {
				b.Fatal(err)
			}
			if got.Foo != true {
				b.Fatal("bad")
			}
		}
	})
}
