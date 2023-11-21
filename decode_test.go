package dynamo

import (
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var itemDecodeOnlyTests = []struct {
	name   string
	given  map[string]*dynamodb.AttributeValue
	expect interface{}
}{
	{
		// unexported embedded pointers should be ignored
		name: "embedded unexported pointer",
		given: map[string]*dynamodb.AttributeValue{
			"Embedded": {BOOL: aws.Bool(true)},
		},
		expect: struct {
			*embedded
		}{},
	},
	{
		// unexported fields should be ignored
		name: "unexported fields",
		given: map[string]*dynamodb.AttributeValue{
			"a": {BOOL: aws.Bool(true)},
		},
		expect: struct {
			a bool
		}{},
	},
	{
		// embedded pointers shouldn't clobber existing fields
		name: "exported pointer embedded struct clobber",
		given: map[string]*dynamodb.AttributeValue{
			"Embedded": {S: aws.String("OK")},
		},
		expect: struct {
			Embedded string
			*ExportedEmbedded
		}{
			Embedded:         "OK",
			ExportedEmbedded: &ExportedEmbedded{},
		},
	},
}

func TestUnmarshalAsymmetric(t *testing.T) {
	for _, tc := range itemDecodeOnlyTests {
		t.Run(tc.name, func(t *testing.T) {
			rv := reflect.New(reflect.TypeOf(tc.expect))
			expect := rv.Interface()
			err := UnmarshalItem(tc.given, expect)
			if err != nil {
				t.Fatalf("%s: unexpected error: %v", tc.name, err)
			}
			if !reflect.DeepEqual(rv.Elem().Interface(), tc.expect) {
				t.Errorf("%s: bad result: %#v ≠ %#v", tc.name, rv.Elem().Interface(), tc.expect)
			}
		})
	}
}

func TestUnmarshalAppend(t *testing.T) {
	var results []struct {
		User  int `dynamo:"UserID"`
		Page  int
		Limit uint
		Null  interface{}
	}
	id := "12345"
	page := "5"
	limit := "20"
	null := true
	item := map[string]*dynamodb.AttributeValue{
		"UserID": {N: &id},
		"Page":   {N: &page},
		"Limit":  {N: &limit},
		"Null":   {NULL: &null},
	}

	for range [15]struct{}{} {
		err := unmarshalAppend(item, &results)
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, h := range results {
		if h.User != 12345 || h.Page != 5 || h.Limit != 20 || h.Null != nil {
			t.Error("invalid hit", h)
		}
	}

	var mapResults []map[string]interface{}

	for range [15]struct{}{} {
		err := unmarshalAppend(item, &mapResults)
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, h := range mapResults {
		if h["UserID"] != 12345.0 || h["Page"] != 5.0 || h["Limit"] != 20.0 || h["Null"] != nil {
			t.Error("invalid interface{} hit", h)
		}
	}
}

func TestUnmarshal(t *testing.T) {
	for _, tc := range encodingTests {
		t.Run(tc.name, func(t *testing.T) {
			rv := reflect.New(reflect.TypeOf(tc.in))
			// dec := newDecodePlan(rv.Elem())
			// err := dec.decodeAttr(flagNone, tc.out, rv)
			err := Unmarshal(tc.out, rv.Interface())
			if err != nil {
				t.Errorf("%s: unexpected error: %v", tc.name, err)
			}

			got := rv.Elem().Interface()
			if !reflect.DeepEqual(got, tc.in) {
				t.Errorf("%s: bad result: \n%#v ≠\n%#v", tc.name, got, tc.out)
			}
		})
	}
}

func TestUnmarshalItem(t *testing.T) {
	for _, tc := range itemEncodingTests {
		t.Run(tc.name, func(t *testing.T) {
			rv := reflect.New(reflect.TypeOf(tc.in))
			err := unmarshalItem(tc.out, rv.Interface())
			if err != nil {
				t.Errorf("%s: unexpected error: %v", tc.name, err)
			}

			iface := rv.Elem().Interface()
			if !reflect.DeepEqual(iface, tc.in) {
				t.Errorf("%s: bad result: %#v ≠ %#v", tc.name, iface, tc.in)
			}
		})
	}
}

func TestUnmarshalNULL(t *testing.T) {
	tru := true
	arbitrary := "hello world"
	double := new(*int)
	item := map[string]*dynamodb.AttributeValue{
		"String":    {NULL: &tru},
		"Slice":     {NULL: &tru},
		"Array":     {NULL: &tru},
		"StringPtr": {NULL: &tru},
		"DoublePtr": {NULL: &tru},
		"Map":       {NULL: &tru},
		"Interface": {NULL: &tru},
	}

	type resultType struct {
		String    string
		Slice     []string
		Array     [2]byte
		StringPtr *string
		DoublePtr **int
		Map       map[string]int
		Interface interface{}
	}

	// dirty result, we want this to be reset
	result := resultType{
		String:    "ABC",
		Slice:     []string{"A", "B"},
		Array:     [2]byte{'A', 'B'},
		StringPtr: &arbitrary,
		DoublePtr: double,
		Map: map[string]int{
			"A": 1,
		},
		Interface: "interface{}",
	}

	if err := UnmarshalItem(item, &result); err != nil {
		t.Error(err)
	}

	if (!reflect.DeepEqual(result, resultType{})) {
		t.Error("unmarshal null: bad result:", result, "≠", resultType{})
	}
}

func TestUnmarshalMissing(t *testing.T) {
	// This test makes sure we're zeroing out fields of structs even if the given data doesn't contain them

	type widget2 struct {
		widget
		Inner struct {
			Blarg string
		}
		Foo *struct {
			Bar int
		}
	}

	w := widget2{
		widget: widget{
			UserID: 111,
			Time:   time.Now().UTC(),
			Msg:    "hello",
		},
	}
	w.Inner.Blarg = "AHH"
	w.Foo = &struct{ Bar int }{Bar: 1337}

	want := widget2{
		widget: widget{
			UserID: 112,
		},
	}

	replace := map[string]*dynamodb.AttributeValue{
		"UserID": {N: aws.String("112")},
	}

	if err := UnmarshalItem(replace, &w); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(want, w) {
		t.Error("bad unmarshal missing. want:", want, "got:", w)
	}

	replace2 := map[string]*dynamodb.AttributeValue{
		"UserID": {N: aws.String("113")},
		"Foo": {M: map[string]*dynamodb.AttributeValue{
			"Bar": {N: aws.String("1338")},
		}},
	}

	want = widget2{
		widget: widget{
			UserID: 113,
		},
		Foo: &struct{ Bar int }{Bar: 1338},
	}

	if err := UnmarshalItem(replace2, &w); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(want, w) {
		t.Error("bad unmarshal missing. want:", want, "got:", w)
	}
}

func TestUnmarshalClearFields(t *testing.T) {
	// tests against a regression from v1.12.0 in which map fields were not properly getting reset

	type Foo struct {
		Map map[string]bool
	}

	items := []Foo{
		{Map: map[string]bool{"a": true}},
		{Map: map[string]bool{"b": true}}, // before fix: {a: true, b: true}
		{Map: map[string]bool{"c": true}}, // before fix: {a: true, b: true, c: true}
	}

	var foo Foo
	for _, item := range items {
		raw, err := MarshalItem(item)
		if err != nil {
			t.Fatal(err)
		}

		if err := UnmarshalItem(raw, &foo); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(item, foo) {
			t.Error("bad result. want:", item, "got:", foo)
		}
	}
}

func BenchmarkUnmarshalReflect(b *testing.B) {
	// av := &dynamodb.AttributeValue{
	// 	L: []*dynamodb.AttributeValue{
	// 		{S: aws.String("fooooooooo")},
	// 	},
	// }
	// for i := 0; i < b.N; i++ {
	// 	var list []string
	// 	unmarshalReflect(av, reflect.ValueOf(&list))
	// }

	var got widget
	for i := 0; i < b.N; i++ {
		unmarshalItem(exampleItem, &got)
	}
}

func TestDecode3(t *testing.T) {
	want := exampleWant
	var got widget
	rv := reflect.ValueOf(&got)
	r, err := getDecodePlan(rv.Type())
	if err != nil {
		t.Fatal(err)
	}
	if err := r.decodeItem(exampleItem, rv); err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Error("bad decode. want:", want, "got:", got)
	}
	// spew.Dump(got)
	// t.Fail()
}

var exampleItem = map[string]*dynamodb.AttributeValue{
	"UserID": {N: aws.String("555")},
	"Msg":    {S: aws.String("fux")},
	"Count":  {N: aws.String("1337")},
	"Meta": {M: map[string]*dynamodb.AttributeValue{
		"Foo": {S: aws.String("1336")},
	}},
}
var exampleWant = widget{
	UserID: 555,
	Msg:    "fux",
	Count:  1337,
	Meta: map[string]string{
		"Foo": "1336",
	},
}
