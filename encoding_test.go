package dynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

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
		name: "dynamo.Marshaler",
		in:   customMarshaler(1),
		out:  &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
	},
	{
		name: "encoding.TextMarshaler",
		in:   textMarshaler(true),
		out:  &dynamodb.AttributeValue{S: aws.String("true")},
	},
}
