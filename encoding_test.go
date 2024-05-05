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
type customEmpty struct{}

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
			"OK": {BOOL: aws.Bool(true)},
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
			"Empty": {M: map[string]*dynamodb.AttributeValue{}},
		}},
	},
	{
		name: "textMarshaler maps",
		in: struct {
			M1 map[textMarshaler]bool // dont omit
		}{
			M1: map[textMarshaler]bool{textMarshaler(true): true},
		},
		out: &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{
			"M1": {M: map[string]*dynamodb.AttributeValue{
				"true": {BOOL: aws.Bool(true)},
			}},
		}},
	},
	{
		name: "struct",
		in: struct {
			OK bool
		}{OK: true},
		out: &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{
			"OK": {BOOL: aws.Bool(true)},
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
		in:   [][]byte{{}, []byte("hello"), {}, []byte("world"), {}},
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
		in:   [...][]byte{{}, []byte("hello"), {}, []byte("world"), {}},
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
			"A": {S: aws.String("hello")},
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
			"A": {S: aws.String("hello")},
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
			"A": {S: aws.String("false")},
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
			"renamed": {S: aws.String("hello")},
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
			"Other": {BOOL: aws.Bool(true)},
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
			"Other": {BOOL: aws.Bool(true)},
		},
	},
	{
		name: "automatic omitempty",
		in: struct {
			OK         string
			EmptyStr   string
			EmptyStr2  customString
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
			"OK":     {S: aws.String("OK")},
			"EmptyL": {L: []*dynamodb.AttributeValue{}},
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
			"S": {S: aws.String("")},
			"B": {B: []byte{}},
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
			"M": {M: map[string]*dynamodb.AttributeValue{
				"null":   {NULL: aws.Bool(true)},
				"empty":  {S: aws.String("")},
				"normal": {S: aws.String("hello")},
			}},
		},
	},
	{
		name: "allowemptyelem flag on map with map element",
		in: struct {
			M map[string]interface{} `dynamo:",allowemptyelem"`
		}{
			M: map[string]interface{}{
				"nestedmap": map[string]interface{}{
					"empty": "",
				},
			},
		},
		out: map[string]*dynamodb.AttributeValue{
			"M": {M: map[string]*dynamodb.AttributeValue{
				"nestedmap": {M: map[string]*dynamodb.AttributeValue{
					"empty": {S: aws.String("")},
				}},
			}},
		},
	},
	{
		name: "allowemptyelem flag on map with slice element, which has a map element",
		in: struct {
			M map[string]interface{} `dynamo:",allowemptyelem"`
		}{
			M: map[string]interface{}{
				"slice": []interface{}{
					map[string]interface{}{"empty": ""},
				},
			},
		},
		out: map[string]*dynamodb.AttributeValue{
			"M": {M: map[string]*dynamodb.AttributeValue{
				"slice": {L: []*dynamodb.AttributeValue{
					{M: map[string]*dynamodb.AttributeValue{
						"empty": {S: aws.String("")},
					}},
				}},
			}},
		},
	},
	{
		name: "allowemptyelem flag on slice with map element",
		in: struct {
			L []interface{} `dynamo:",allowemptyelem"`
		}{
			L: []interface{}{
				map[string]interface{}{
					"empty": "",
				},
			},
		},
		out: map[string]*dynamodb.AttributeValue{
			"L": {L: []*dynamodb.AttributeValue{
				{
					M: map[string]*dynamodb.AttributeValue{
						"empty": {S: aws.String("")},
					},
				},
			},
			},
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
			"S":       {NULL: aws.Bool(true)},
			"B":       {NULL: aws.Bool(true)},
			"NilTime": {NULL: aws.Bool(true)},
			"M":       {NULL: aws.Bool(true)},
			"SS":      {NULL: aws.Bool(true)},
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
			"Embedded": {BOOL: aws.Bool(true)},
		},
	},
	{
		name: "exported embedded struct",
		in: struct {
			ExportedEmbedded
		}{
			ExportedEmbedded: ExportedEmbedded{
				Embedded: true,
			},
		},
		out: map[string]*dynamodb.AttributeValue{
			"Embedded": {BOOL: aws.Bool(true)},
		},
	},
	{
		name: "exported pointer embedded struct",
		in: struct {
			*ExportedEmbedded
		}{
			ExportedEmbedded: &ExportedEmbedded{
				Embedded: true,
			},
		},
		out: map[string]*dynamodb.AttributeValue{
			"Embedded": {BOOL: aws.Bool(true)},
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
			"Embedded": {S: aws.String("OK")},
		},
	},
	{
		name: "pointer embedded struct clobber",
		in: struct {
			Embedded string
			*embedded
		}{
			Embedded: "OK",
		},
		out: map[string]*dynamodb.AttributeValue{
			"Embedded": {S: aws.String("OK")},
		},
	},
	{
		name: "exported embedded struct clobber",
		in: struct {
			Embedded string
			ExportedEmbedded
		}{
			Embedded: "OK",
		},
		out: map[string]*dynamodb.AttributeValue{
			"Embedded": {S: aws.String("OK")},
		},
	},
	{
		name: "sets",
		in: struct {
			SS1  []string                   `dynamo:",set"`
			SS2  []textMarshaler            `dynamo:",set"`
			SS3  map[string]struct{}        `dynamo:",set"`
			SS4  map[string]bool            `dynamo:",set"`
			SS5  map[customString]struct{}  `dynamo:",set"`
			SS6  []customString             `dynamo:",set"`
			SS7  map[textMarshaler]struct{} `dynamo:",set"`
			SS8  map[textMarshaler]bool     `dynamo:",set"`
			SS9  []string                   `dynamo:",set"`
			SS10 map[string]customEmpty     `dynamo:",set"`
			BS1  [][]byte                   `dynamo:",set"`
			BS2  map[[1]byte]struct{}       `dynamo:",set"`
			BS3  map[[1]byte]bool           `dynamo:",set"`
			BS4  [][]byte                   `dynamo:",set"`
			NS1  []int                      `dynamo:",set"`
			NS2  []float64                  `dynamo:",set"`
			NS3  []uint                     `dynamo:",set"`
			NS4  map[int]struct{}           `dynamo:",set"`
			NS5  map[uint]bool              `dynamo:",set"`
		}{
			SS1:  []string{"A", "B"},
			SS2:  []textMarshaler{textMarshaler(true), textMarshaler(false)},
			SS3:  map[string]struct{}{"A": {}},
			SS4:  map[string]bool{"A": true},
			SS5:  map[customString]struct{}{"A": {}},
			SS6:  []customString{"A", "B"},
			SS7:  map[textMarshaler]struct{}{textMarshaler(true): {}},
			SS8:  map[textMarshaler]bool{textMarshaler(false): true},
			SS9:  []string{"A", "B", ""},
			SS10: map[string]customEmpty{"A": {}},
			BS1:  [][]byte{{'A'}, {'B'}},
			BS2:  map[[1]byte]struct{}{{'A'}: {}},
			BS3:  map[[1]byte]bool{{'A'}: true},
			BS4:  [][]byte{{'A'}, {'B'}, {}},
			NS1:  []int{1, 2},
			NS2:  []float64{1, 2},
			NS3:  []uint{1, 2},
			NS4:  map[int]struct{}{maxInt: {}},
			NS5:  map[uint]bool{maxUint: true},
		},
		out: map[string]*dynamodb.AttributeValue{
			"SS1":  {SS: []*string{aws.String("A"), aws.String("B")}},
			"SS2":  {SS: []*string{aws.String("true"), aws.String("false")}},
			"SS3":  {SS: []*string{aws.String("A")}},
			"SS4":  {SS: []*string{aws.String("A")}},
			"SS5":  {SS: []*string{aws.String("A")}},
			"SS6":  {SS: []*string{aws.String("A"), aws.String("B")}},
			"SS7":  {SS: []*string{aws.String("true")}},
			"SS8":  {SS: []*string{aws.String("false")}},
			"SS9":  {SS: []*string{aws.String("A"), aws.String("B"), aws.String("")}},
			"SS10": {SS: []*string{aws.String("A")}},
			"BS1":  {BS: [][]byte{{'A'}, {'B'}}},
			"BS2":  {BS: [][]byte{{'A'}}},
			"BS3":  {BS: [][]byte{{'A'}}},
			"BS4":  {BS: [][]byte{{'A'}, {'B'}, {}}},
			"NS1":  {NS: []*string{aws.String("1"), aws.String("2")}},
			"NS2":  {NS: []*string{aws.String("1"), aws.String("2")}},
			"NS3":  {NS: []*string{aws.String("1"), aws.String("2")}},
			"NS4":  {NS: []*string{aws.String(maxIntStr)}},
			"NS5":  {NS: []*string{aws.String(maxUintStr)}},
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
			"S": {S: aws.String("Hello")},
			"B": {B: []byte{'A', 'B'}},
			"N": {N: aws.String("1.2")},
			"L": {L: []*dynamodb.AttributeValue{
				{S: aws.String("A")},
				{S: aws.String("B")},
				{N: aws.String("1.2")},
			}},
			"M": {M: map[string]*dynamodb.AttributeValue{
				"OK": {BOOL: aws.Bool(true)},
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
			"M": {M: map[string]*dynamodb.AttributeValue{
				"Hello": {S: aws.String("world")},
			}},
		},
	},
	{
		name: "map string attributevalue",
		in: map[string]*dynamodb.AttributeValue{
			"M": {M: map[string]*dynamodb.AttributeValue{
				"Hello": {S: aws.String("world")},
			}},
		},
		out: map[string]*dynamodb.AttributeValue{
			"M": {M: map[string]*dynamodb.AttributeValue{
				"Hello": {S: aws.String("world")},
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
			"TTL": {S: aws.String("2019-01-01T00:00:00Z")},
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
			"TTL": {N: aws.String("1546300800")},
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
			"TTL": {N: aws.String("1546300800")},
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
			"thing": {N: aws.String("52")},
		},
	},
	{
		name: "*dynamodb.ItemUnmarshaler",
		in:   &customItemMarshaler{Thing: 52},
		out: map[string]*dynamodb.AttributeValue{
			"thing": {N: aws.String("52")},
		},
	},
	{
		name: "self-recursive struct",
		in: Person{
			Spouse: &Person{
				Name:     "Peggy",
				Children: []Person{{Name: "Bobby", Children: []Person{}}},
			},
			Children: []Person{{Name: "Bobby", Children: []Person{}}},
			Name:     "Hank",
		},
		out: map[string]*dynamodb.AttributeValue{
			"Name": {S: aws.String("Hank")},
			"Spouse": {M: map[string]*dynamodb.AttributeValue{
				"Name": {S: aws.String("Peggy")},
				"Children": {L: []*dynamodb.AttributeValue{
					{M: map[string]*dynamodb.AttributeValue{
						"Name":     {S: aws.String("Bobby")},
						"Children": {L: []*dynamodb.AttributeValue{}},
					}},
				},
				},
			}},
			"Children": {L: []*dynamodb.AttributeValue{
				{M: map[string]*dynamodb.AttributeValue{
					"Name":     {S: aws.String("Bobby")},
					"Children": {L: []*dynamodb.AttributeValue{}},
				}},
			}},
		},
	},
	{
		name: "struct with recursive field",
		in: Friend{
			ID: 555,
			Person: Person{
				Spouse: &Person{
					Name:     "Peggy",
					Children: []Person{{Name: "Bobby", Children: []Person{}}},
				},
				Children: []Person{{Name: "Bobby", Children: []Person{}}},
				Name:     "Hank",
			},
			Nickname: "H-Dawg",
		},
		out: map[string]*dynamodb.AttributeValue{
			"ID":       {N: aws.String("555")},
			"Nickname": {S: aws.String("H-Dawg")},
			"Person": {M: map[string]*dynamodb.AttributeValue{
				"Name": {S: aws.String("Hank")},
				"Spouse": {M: map[string]*dynamodb.AttributeValue{
					"Name": {S: aws.String("Peggy")},
					"Children": {L: []*dynamodb.AttributeValue{
						{M: map[string]*dynamodb.AttributeValue{
							"Name":     {S: aws.String("Bobby")},
							"Children": {L: []*dynamodb.AttributeValue{}},
						}},
					},
					},
				}},
				"Children": {L: []*dynamodb.AttributeValue{
					{M: map[string]*dynamodb.AttributeValue{
						"Name":     {S: aws.String("Bobby")},
						"Children": {L: []*dynamodb.AttributeValue{}},
					}},
				}},
			},
			},
		},
	},
	{
		name: "mega recursion A -> B -> *A -> B",
		in: MegaRecursiveA{
			ID:   123,
			Text: "hello",
			Child: MegaRecursiveB{
				ID:   "test",
				Blah: 555,
				Child: &MegaRecursiveA{
					ID:   222,
					Text: "help",
					Child: MegaRecursiveB{
						ID:   "why",
						Blah: 1337,
					},
					Friends: []MegaRecursiveA{},
					Enemies: []MegaRecursiveB{},
				},
			},
			Friends: []MegaRecursiveA{
				{ID: 1, Text: "suffering", Child: MegaRecursiveB{ID: "pain"}, Friends: []MegaRecursiveA{}, Enemies: []MegaRecursiveB{}},
				{ID: 2, Text: "love", Child: MegaRecursiveB{ID: "understanding"}, Friends: []MegaRecursiveA{}, Enemies: []MegaRecursiveB{}},
			},
			Enemies: []MegaRecursiveB{
				{ID: "recursion", Blah: 30},
			},
		},
		out: map[string]*dynamodb.AttributeValue{
			"ID":   {N: aws.String("123")},
			"Text": {S: aws.String("hello")},
			"Friends": {L: []*dynamodb.AttributeValue{
				{M: map[string]*dynamodb.AttributeValue{
					"ID":   {N: aws.String("1")},
					"Text": {S: aws.String("suffering")},
					"Child": {M: map[string]*dynamodb.AttributeValue{
						"ID": {S: aws.String("pain")},
					}},
					"Friends": {L: []*dynamodb.AttributeValue{}},
					"Enemies": {L: []*dynamodb.AttributeValue{}},
				}},
				{M: map[string]*dynamodb.AttributeValue{
					"ID":   {N: aws.String("2")},
					"Text": {S: aws.String("love")},
					"Child": {M: map[string]*dynamodb.AttributeValue{
						"ID": {S: aws.String("understanding")},
					}},
					"Friends": {L: []*dynamodb.AttributeValue{}},
					"Enemies": {L: []*dynamodb.AttributeValue{}},
				}},
			}},
			"Enemies": {L: []*dynamodb.AttributeValue{
				{M: map[string]*dynamodb.AttributeValue{
					"ID":   {S: aws.String("recursion")},
					"Blah": {N: aws.String("30")},
				}},
			}},
			"Child": {M: map[string]*dynamodb.AttributeValue{
				"ID":   {S: aws.String("test")},
				"Blah": {N: aws.String("555")},
				"Child": {M: map[string]*dynamodb.AttributeValue{
					"ID":      {N: aws.String("222")},
					"Text":    {S: aws.String("help")},
					"Friends": {L: []*dynamodb.AttributeValue{}},
					"Enemies": {L: []*dynamodb.AttributeValue{}},
					"Child": {M: map[string]*dynamodb.AttributeValue{
						"ID":   {S: aws.String("why")},
						"Blah": {N: aws.String("1337")},
					}},
				}},
			}},
		},
	},
}

type embedded struct {
	Embedded bool
}

type ExportedEmbedded struct {
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
		"thing": {
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

type Person struct {
	Spouse   *Person
	Children []Person
	Name     string
}

type Friend struct {
	ID       int
	Person   Person
	Nickname string
}

type MegaRecursiveA struct {
	ID      int
	Child   MegaRecursiveB
	Text    string
	Friends []MegaRecursiveA
	Enemies []MegaRecursiveB
}

type MegaRecursiveB struct {
	ID    string
	Child *MegaRecursiveA
	Blah  int `dynamo:",omitempty"`
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
