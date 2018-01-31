package dynamo

import (
	"encoding"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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
}

var itemEncodingTests = []struct {
	name string
	in   interface{}
	out  map[string]*dynamodb.AttributeValue
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
			A     bool `dynamo:",omitempty"`
			Other bool
		}{
			Other: true,
		},
		out: map[string]*dynamodb.AttributeValue{
			"Other": &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
		},
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
			SS1 []string                  `dynamo:",set"`
			SS2 []textMarshaler           `dynamo:",set"`
			SS3 map[string]struct{}       `dynamo:",set"`
			SS4 map[string]bool           `dynamo:",set"`
			SS5 map[customString]struct{} `dynamo:",set"`
			SS6 []customString            `dynamo:",set"`
			BS1 [][]byte                  `dynamo:",set"`
			BS2 map[[1]byte]struct{}      `dynamo:",set"`
			BS3 map[[1]byte]bool          `dynamo:",set"`
			NS1 []int                     `dynamo:",set"`
			NS2 []float64                 `dynamo:",set"`
			NS3 []uint                    `dynamo:",set"`
			NS4 map[int]struct{}          `dynamo:",set"`
			NS5 map[uint]bool             `dynamo:",set"`
		}{
			SS1: []string{"A", "B"},
			SS2: []textMarshaler{textMarshaler(true), textMarshaler(false)},
			SS3: map[string]struct{}{"A": struct{}{}},
			SS4: map[string]bool{"A": true},
			SS5: map[customString]struct{}{"A": struct{}{}},
			SS6: []customString{"A", "B"},
			BS1: [][]byte{[]byte{'A'}, []byte{'B'}},
			BS2: map[[1]byte]struct{}{[1]byte{'A'}: struct{}{}},
			BS3: map[[1]byte]bool{[1]byte{'A'}: true},
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
			"BS1": &dynamodb.AttributeValue{BS: [][]byte{[]byte{'A'}, []byte{'B'}}},
			"BS2": &dynamodb.AttributeValue{BS: [][]byte{[]byte{'A'}}},
			"BS3": &dynamodb.AttributeValue{BS: [][]byte{[]byte{'A'}}},
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

var (
	_ Marshaler                = new(customMarshaler)
	_ Unmarshaler              = new(customMarshaler)
	_ encoding.TextMarshaler   = new(textMarshaler)
	_ encoding.TextUnmarshaler = new(textMarshaler)
)
