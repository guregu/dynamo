package dynamo

import (
	"encoding"
	"errors"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

const (
	maxUint = ^uint(0)
	maxInt  = int(maxUint >> 1)
)

var (
	maxIntStr  = strconv.Itoa(maxInt)
	maxUintStr = strconv.FormatUint(uint64(maxUint), 10)
)

type customString string

var encodingTests = []struct {
	name string
	in   interface{}
	out  *dynamodb.AttributeValue
}{
	{
		name: "strings",
		in:   "hello",
		out:  &dynamodb.AttributeValue{S: aws.String("hello")},
	},
	{
		name: "bools",
		in:   true,
		out:  &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
	},
	{
		name: "ints",
		in:   123,
		out:  &dynamodb.AttributeValue{N: aws.String("123")},
	},
	{
		name: "uints",
		in:   uint(123),
		out:  &dynamodb.AttributeValue{N: aws.String("123")},
	},
	{
		name: "floats",
		in:   1.2,
		out:  &dynamodb.AttributeValue{N: aws.String("1.2")},
	},
	{
		name: "pointer (int)",
		in:   new(int),
		out:  &dynamodb.AttributeValue{N: aws.String("0")},
	},
	{
		name: "maps",
		in: map[string]bool{
			"OK": true,
		},
		out: &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{
			"OK": &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
		}},
	},
	{
		name: "empty maps",
		in: struct {
			Empty map[string]bool // dont omit
			Null  map[string]bool // omit
		}{
			Empty: map[string]bool{},
		},
		out: &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{
			"Empty": &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{}},
		}},
	},
	{
		name: "struct",
		in: struct {
			OK bool
		}{OK: true},
		out: &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{
			"OK": &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
		}},
	},
	{
		name: "[]byte",
		in:   []byte{'O', 'K'},
		out:  &dynamodb.AttributeValue{B: []byte{'O', 'K'}},
	},
	{
		name: "slice",
		in:   []int{1, 2, 3},
		out: &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{
			{N: aws.String("1")},
			{N: aws.String("2")},
			{N: aws.String("3")},
		}},
	},
	{
		name: "array",
		in:   [3]int{1, 2, 3},
		out: &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{
			{N: aws.String("1")},
			{N: aws.String("2")},
			{N: aws.String("3")},
		}},
	},
	{
		name: "byte array",
		in:   [4]byte{'a', 'b', 'c', 'd'},
		out:  &dynamodb.AttributeValue{B: []byte{'a', 'b', 'c', 'd'}},
	},
	{
		name: "dynamo.Marshaler",
		in:   customMarshaler(1),
		out:  &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
	},
	{
		name: "encoding.TextMarshaler",
		in:   textMarshaler(true),
		out:  &dynamodb.AttributeValue{S: aws.String("true")},
	},
	{
		name: "dynamodb.AttributeValue",
		in: &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{
			{N: aws.String("1")},
			{N: aws.String("2")},
			{N: aws.String("3")},
		}},
		out: &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{
			{N: aws.String("1")},
			{N: aws.String("2")},
			{N: aws.String("3")},
		}},
	},
	{
		name: "slice with nil",
		in:   []*int64{nil, aws.Int64(0), nil, aws.Int64(1337), nil},
		out: &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{
			{NULL: aws.Bool(true)},
			{N: aws.String("0")},
			{NULL: aws.Bool(true)},
			{N: aws.String("1337")},
			{NULL: aws.Bool(true)},
		}},
	},
	{
		name: "array with nil",
		in:   [...]*int64{nil, aws.Int64(0), nil, aws.Int64(1337), nil},
		out: &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{
			{NULL: aws.Bool(true)},
			{N: aws.String("0")},
			{NULL: aws.Bool(true)},
			{N: aws.String("1337")},
			{NULL: aws.Bool(true)},
		}},
	},
	{
		name: "slice with empty string",
		in:   []string{"", "hello", "", "world", ""},
		out: &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{
			{S: aws.String("")},
			{S: aws.String("hello")},
			{S: aws.String("")},
			{S: aws.String("world")},
			{S: aws.String("")},
		}},
	},
	{
		name: "array with empty string",
		in:   [...]string{"", "hello", "", "world", ""},
		out: &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{
			{S: aws.String("")},
			{S: aws.String("hello")},
			{S: aws.String("")},
			{S: aws.String("world")},
			{S: aws.String("")},
		}},
	},
	{
		name: "slice of string pointers",
		in:   []*string{nil, aws.String("hello"), aws.String(""), aws.String("world"), nil},
		out: &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{
			{NULL: aws.Bool(true)},
			{S: aws.String("hello")},
			{S: aws.String("")},
			{S: aws.String("world")},
			{NULL: aws.Bool(true)},
		}},
	},
	{
		name: "slice with empty binary",
		in:   [][]byte{[]byte{}, []byte("hello"), []byte{}, []byte("world"), []byte{}},
		out: &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{
			{B: []byte{}},
			{B: []byte{'h', 'e', 'l', 'l', 'o'}},
			{B: []byte{}},
			{B: []byte{'w', 'o', 'r', 'l', 'd'}},
			{B: []byte{}},
		}},
	},
	{
		name: "array with empty binary",
		in:   [...][]byte{[]byte{}, []byte("hello"), []byte{}, []byte("world"), []byte{}},
		out: &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{
			{B: []byte{}},
			{B: []byte{'h', 'e', 'l', 'l', 'o'}},
			{B: []byte{}},
			{B: []byte{'w', 'o', 'r', 'l', 'd'}},
			{B: []byte{}},
		}},
	},
	{
		name: "array with empty binary ptrs",
		in:   [...]*[]byte{byteSlicePtr([]byte{}), byteSlicePtr([]byte("hello")), nil, byteSlicePtr([]byte("world")), byteSlicePtr([]byte{})},
		out: &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{
			{B: []byte{}},
			{B: []byte{'h', 'e', 'l', 'l', 'o'}},
			{NULL: aws.Bool(true)},
			{B: []byte{'w', 'o', 'r', 'l', 'd'}},
			{B: []byte{}},
		}},
	},
}

var itemEncodingTests = []struct {
	name       string
	in         interface{}
	out        map[string]*dynamodb.AttributeValue
	asymmetric bool
}{
	{
		name: "strings",
		in: struct {
			A string
		}{
			A: "hello",
		},
		out: map[string]*dynamodb.AttributeValue{
			"A": &dynamodb.AttributeValue{S: aws.String("hello")},
		},
	},
	{
		name: "pointer (string)",
		in: &struct {
			A string
		}{
			A: "hello",
		},
		out: map[string]*dynamodb.AttributeValue{
			"A": &dynamodb.AttributeValue{S: aws.String("hello")},
		},
	},
	{
		name: "pointer (value receiver TextMarshaler)",
		in: &struct {
			A *textMarshaler
		}{
			A: new(textMarshaler),
		},
		out: map[string]*dynamodb.AttributeValue{
			"A": &dynamodb.AttributeValue{S: aws.String("false")},
		},
	},
	{
		name: "rename",
		in: struct {
			A string `dynamo:"renamed"`
		}{
			A: "hello",
		},
		out: map[string]*dynamodb.AttributeValue{
			"renamed": &dynamodb.AttributeValue{S: aws.String("hello")},
		},
	},
	{
		name: "skip",
		in: struct {
			A     string `dynamo:"-"`
			Other bool
		}{
			A:     "",
			Other: true,
		},
		out: map[string]*dynamodb.AttributeValue{
			"Other": &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
		},
	},
	{
		name: "omitempty",
		in: struct {
			A       bool       `dynamo:",omitempty"`
			B       *bool      `dynamo:",omitempty"`
			NilTime *time.Time `dynamo:",omitempty"`
			L       []string   `dynamo:",omitempty"`
			Other   bool
		}{
			Other: true,
		},
		out: map[string]*dynamodb.AttributeValue{
			"Other": &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
		},
	},
	{
		name: "omitemptyelem",
		in: struct {
			L     []*string         `dynamo:",omitemptyelem"`
			SS    []string          `dynamo:",omitemptyelem,set"`
			M     map[string]string `dynamo:",omitemptyelem"`
			Other bool
		}{
			L:     []*string{nil, aws.String("")},
			SS:    []string{""},
			M:     map[string]string{"test": ""},
			Other: true,
		},
		out: map[string]*dynamodb.AttributeValue{
			"L":     &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{}},
			"M":     &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{}},
			"Other": &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
		},
		asymmetric: true,
	},
	{
		name: "omitemptyelem + omitempty",
		in: struct {
			L     []*string         `dynamo:",omitemptyelem,omitempty"`
			M     map[string]string `dynamo:",omitemptyelem,omitempty"`
			Other bool
		}{
			L:     []*string{nil, aws.String("")},
			M:     map[string]string{"test": ""},
			Other: true,
		},
		out: map[string]*dynamodb.AttributeValue{
			"Other": &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
		},
		asymmetric: true,
	},
	{
		name: "automatic omitempty",
		in: struct {
			OK         string
			EmptyStr   string
			EmptyB     []byte
			EmptyL     []int
			EmptyM     map[string]bool
			EmptyPtr   *int
			EmptyIface interface{}
			NilTime    *time.Time
			NilCustom  *customMarshaler
			NilText    *textMarshaler
			NilAWS     *dynamodbattribute.UnixTime
		}{
			OK:     "OK",
			EmptyL: []int{},
		},
		out: map[string]*dynamodb.AttributeValue{
			"OK":     &dynamodb.AttributeValue{S: aws.String("OK")},
			"EmptyL": &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{}},
		},
	},
	{
		name: "allowempty flag",
		in: struct {
			S string `dynamo:",allowempty"`
			B []byte `dynamo:",allowempty"`
		}{
			B: []byte{},
		},
		out: map[string]*dynamodb.AttributeValue{
			"S": &dynamodb.AttributeValue{S: aws.String("")},
			"B": &dynamodb.AttributeValue{B: []byte{}},
		},
	},
	{
		name: "allowemptyelem flag",
		in: struct {
			M map[string]*string `dynamo:",allowemptyelem"`
		}{
			M: map[string]*string{"null": nil, "empty": aws.String(""), "normal": aws.String("hello")},
		},
		out: map[string]*dynamodb.AttributeValue{
			"M": &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{
				"null":   &dynamodb.AttributeValue{NULL: aws.Bool(true)},
				"empty":  &dynamodb.AttributeValue{S: aws.String("")},
				"normal": &dynamodb.AttributeValue{S: aws.String("hello")},
			}},
		},
	},
	{
		name: "null flag",
		in: struct {
			S       string             `dynamo:",null"`
			B       []byte             `dynamo:",null"`
			NilTime *time.Time         `dynamo:",null"`
			M       map[string]*string `dynamo:",null"`
			SS      []string           `dynamo:",null,set"`
		}{},
		out: map[string]*dynamodb.AttributeValue{
			"S":       &dynamodb.AttributeValue{NULL: aws.Bool(true)},
			"B":       &dynamodb.AttributeValue{NULL: aws.Bool(true)},
			"NilTime": &dynamodb.AttributeValue{NULL: aws.Bool(true)},
			"M":       &dynamodb.AttributeValue{NULL: aws.Bool(true)},
			"SS":      &dynamodb.AttributeValue{NULL: aws.Bool(true)},
		},
	},
	{
		name: "embedded struct",
		in: struct {
			embedded
		}{
			embedded: embedded{
				Embedded: true,
			},
		},
		out: map[string]*dynamodb.AttributeValue{
			"Embedded": &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
		},
	},
	{
		name: "embedded struct clobber",
		in: struct {
			Embedded string
			embedded
		}{
			Embedded: "OK",
		},
		out: map[string]*dynamodb.AttributeValue{
			"Embedded": &dynamodb.AttributeValue{S: aws.String("OK")},
		},
	},
	{
		name: "sets",
		in: struct {
			SS1 []string                   `dynamo:",set"`
			SS2 []textMarshaler            `dynamo:",set"`
			SS3 map[string]struct{}        `dynamo:",set"`
			SS4 map[string]bool            `dynamo:",set"`
			SS5 map[customString]struct{}  `dynamo:",set"`
			SS6 []customString             `dynamo:",set"`
			SS7 map[textMarshaler]struct{} `dynamo:",set"`
			SS8 map[textMarshaler]bool     `dynamo:",set"`
			SS9 []string                   `dynamo:",set"`
			BS1 [][]byte                   `dynamo:",set"`
			BS2 map[[1]byte]struct{}       `dynamo:",set"`
			BS3 map[[1]byte]bool           `dynamo:",set"`
			BS4 [][]byte                   `dynamo:",set"`
			NS1 []int                      `dynamo:",set"`
			NS2 []float64                  `dynamo:",set"`
			NS3 []uint                     `dynamo:",set"`
			NS4 map[int]struct{}           `dynamo:",set"`
			NS5 map[uint]bool              `dynamo:",set"`
		}{
			SS1: []string{"A", "B"},
			SS2: []textMarshaler{textMarshaler(true), textMarshaler(false)},
			SS3: map[string]struct{}{"A": struct{}{}},
			SS4: map[string]bool{"A": true},
			SS5: map[customString]struct{}{"A": struct{}{}},
			SS6: []customString{"A", "B"},
			SS7: map[textMarshaler]struct{}{textMarshaler(true): struct{}{}},
			SS8: map[textMarshaler]bool{textMarshaler(false): true},
			SS9: []string{"A", "B", ""},
			BS1: [][]byte{[]byte{'A'}, []byte{'B'}},
			BS2: map[[1]byte]struct{}{[1]byte{'A'}: struct{}{}},
			BS3: map[[1]byte]bool{[1]byte{'A'}: true},
			BS4: [][]byte{[]byte{'A'}, []byte{'B'}, []byte{}},
			NS1: []int{1, 2},
			NS2: []float64{1, 2},
			NS3: []uint{1, 2},
			NS4: map[int]struct{}{maxInt: struct{}{}},
			NS5: map[uint]bool{maxUint: true},
		},
		out: map[string]*dynamodb.AttributeValue{
			"SS1": &dynamodb.AttributeValue{SS: []*string{aws.String("A"), aws.String("B")}},
			"SS2": &dynamodb.AttributeValue{SS: []*string{aws.String("true"), aws.String("false")}},
			"SS3": &dynamodb.AttributeValue{SS: []*string{aws.String("A")}},
			"SS4": &dynamodb.AttributeValue{SS: []*string{aws.String("A")}},
			"SS5": &dynamodb.AttributeValue{SS: []*string{aws.String("A")}},
			"SS6": &dynamodb.AttributeValue{SS: []*string{aws.String("A"), aws.String("B")}},
			"SS7": &dynamodb.AttributeValue{SS: []*string{aws.String("true")}},
			"SS8": &dynamodb.AttributeValue{SS: []*string{aws.String("false")}},
			"SS9": &dynamodb.AttributeValue{SS: []*string{aws.String("A"), aws.String("B"), aws.String("")}},
			"BS1": &dynamodb.AttributeValue{BS: [][]byte{[]byte{'A'}, []byte{'B'}}},
			"BS2": &dynamodb.AttributeValue{BS: [][]byte{[]byte{'A'}}},
			"BS3": &dynamodb.AttributeValue{BS: [][]byte{[]byte{'A'}}},
			"BS4": &dynamodb.AttributeValue{BS: [][]byte{[]byte{'A'}, []byte{'B'}, []byte{}}},
			"NS1": &dynamodb.AttributeValue{NS: []*string{aws.String("1"), aws.String("2")}},
			"NS2": &dynamodb.AttributeValue{NS: []*string{aws.String("1"), aws.String("2")}},
			"NS3": &dynamodb.AttributeValue{NS: []*string{aws.String("1"), aws.String("2")}},
			"NS4": &dynamodb.AttributeValue{NS: []*string{aws.String(maxIntStr)}},
			"NS5": &dynamodb.AttributeValue{NS: []*string{aws.String(maxUintStr)}},
		},
	},
	{
		name: "map as item",
		in: map[string]interface{}{
			"S": "Hello",
			"B": []byte{'A', 'B'},
			"N": float64(1.2),
			"L": []interface{}{"A", "B", 1.2},
			"M": map[string]interface{}{
				"OK": true,
			},
		},
		out: map[string]*dynamodb.AttributeValue{
			"S": &dynamodb.AttributeValue{S: aws.String("Hello")},
			"B": &dynamodb.AttributeValue{B: []byte{'A', 'B'}},
			"N": &dynamodb.AttributeValue{N: aws.String("1.2")},
			"L": &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{
				&dynamodb.AttributeValue{S: aws.String("A")},
				&dynamodb.AttributeValue{S: aws.String("B")},
				&dynamodb.AttributeValue{N: aws.String("1.2")},
			}},
			"M": &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{
				"OK": &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
			}},
		},
	},
	{
		name: "map as key",
		in: struct {
			M map[string]interface{}
		}{
			M: map[string]interface{}{
				"Hello": "world",
			},
		},
		out: map[string]*dynamodb.AttributeValue{
			"M": &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{
				"Hello": &dynamodb.AttributeValue{S: aws.String("world")},
			}},
		},
	},
	{
		name: "map string attributevalue",
		in: map[string]*dynamodb.AttributeValue{
			"M": &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{
				"Hello": &dynamodb.AttributeValue{S: aws.String("world")},
			}},
		},
		out: map[string]*dynamodb.AttributeValue{
			"M": &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{
				"Hello": &dynamodb.AttributeValue{S: aws.String("world")},
			}},
		},
	},
	{
		name: "time.Time (regular encoding)",
		in: struct {
			TTL time.Time
		}{
			TTL: time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		out: map[string]*dynamodb.AttributeValue{
			"TTL": &dynamodb.AttributeValue{S: aws.String("2019-01-01T00:00:00Z")},
		},
	},
	{
		name: "time.Time (unixtime encoding)",
		in: struct {
			TTL time.Time `dynamo:",unixtime"`
		}{
			TTL: time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		out: map[string]*dynamodb.AttributeValue{
			"TTL": &dynamodb.AttributeValue{N: aws.String("1546300800")},
		},
	},
	{
		name: "time.Time (zero unixtime encoding)",
		in: struct {
			TTL time.Time `dynamo:",unixtime"`
		}{
			TTL: time.Time{},
		},
		out: map[string]*dynamodb.AttributeValue{},
	},
	{
		name: "*time.Time (unixtime encoding)",
		in: struct {
			TTL *time.Time `dynamo:",unixtime"`
		}{
			TTL: aws.Time(time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)),
		},
		out: map[string]*dynamodb.AttributeValue{
			"TTL": &dynamodb.AttributeValue{N: aws.String("1546300800")},
		},
	},
	{
		name: "*time.Time (zero unixtime encoding)",
		in: struct {
			TTL *time.Time `dynamo:",unixtime"`
		}{
			TTL: nil,
		},
		out: map[string]*dynamodb.AttributeValue{},
	},
	{
		name: "dynamodb.ItemUnmarshaler",
		in:   customItemMarshaler{Thing: 52},
		out: map[string]*dynamodb.AttributeValue{
			"thing": &dynamodb.AttributeValue{N: aws.String("52")},
		},
	},
	{
		name: "*dynamodb.ItemUnmarshaler",
		in:   &customItemMarshaler{Thing: 52},
		out: map[string]*dynamodb.AttributeValue{
			"thing": &dynamodb.AttributeValue{N: aws.String("52")},
		},
	},
}

type embedded struct {
	Embedded bool
}

type customMarshaler int

func (cm customMarshaler) MarshalDynamo() (*dynamodb.AttributeValue, error) {
	return &dynamodb.AttributeValue{
		BOOL: aws.Bool(cm != 0),
	}, nil
}

func (cm *customMarshaler) UnmarshalDynamo(av *dynamodb.AttributeValue) error {
	if *av.BOOL == true {
		*cm = 1
	}
	return nil
}

type textMarshaler bool

func (tm textMarshaler) MarshalText() ([]byte, error) {
	if tm {
		return []byte("true"), nil
	}
	return []byte("false"), nil
}

func (tm *textMarshaler) UnmarshalText(text []byte) error {
	*tm = string(text) == "true"
	return nil
}

type ptrTextMarshaler bool

func (tm *ptrTextMarshaler) MarshalText() ([]byte, error) {
	if tm == nil {
		return []byte("null"), nil
	}
	if *tm {
		return []byte("true"), nil
	}
	return []byte("false"), nil
}

func (tm *ptrTextMarshaler) UnmarshalText(text []byte) error {
	if string(text) == "null" {
		return nil
	}
	*tm = string(text) == "true"
	return nil
}

type customItemMarshaler struct {
	Thing interface{} `dynamo:"thing"`
}

func (cim *customItemMarshaler) MarshalDynamoItem() (map[string]*dynamodb.AttributeValue, error) {
	thing := strconv.Itoa(cim.Thing.(int))
	attrs := map[string]*dynamodb.AttributeValue{
		"thing": &dynamodb.AttributeValue{
			N: &thing,
		},
	}

	return attrs, nil
}

func (cim *customItemMarshaler) UnmarshalDynamoItem(item map[string]*dynamodb.AttributeValue) error {
	thingAttr := item["thing"]

	if thingAttr == nil || thingAttr.N == nil {
		return errors.New("Missing or not a number")
	}

	thing, err := strconv.Atoi(*thingAttr.N)
	if err != nil {
		return errors.New("Invalid number")
	}

	cim.Thing = thing
	return nil
}

func byteSlicePtr(a []byte) *[]byte {
	return &a
}

var (
	_ Marshaler                = new(customMarshaler)
	_ Unmarshaler              = new(customMarshaler)
	_ ItemMarshaler            = new(customItemMarshaler)
	_ ItemUnmarshaler          = new(customItemMarshaler)
	_ encoding.TextMarshaler   = new(textMarshaler)
	_ encoding.TextUnmarshaler = new(textMarshaler)
	_ encoding.TextMarshaler   = new(ptrTextMarshaler)
	_ encoding.TextUnmarshaler = new(ptrTextMarshaler)
)
