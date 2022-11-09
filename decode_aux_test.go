package dynamo_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/guregu/dynamo"
)

type Coffee struct {
	Name string
}

func TestEncodingAux(t *testing.T) {
	// This tests behavior of embedded anonymous (unexported) structs
	// using the "aux" unmarshaling trick.
	// See: https://github.com/guregu/dynamo/issues/181

	in := map[string]types.AttributeValue{
		"ID":   &types.AttributeValueMemberS{Value: "intenso"},
		"Name": &types.AttributeValueMemberS{Value: "Intenso 12"},
	}

	type coffeeItemDefault struct {
		ID string
		Coffee
	}

	tests := []struct {
		name string
		out  interface{}
	}{
		{name: "no custom unmarshalling", out: coffeeItemDefault{ID: "intenso", Coffee: Coffee{Name: "Intenso 12"}}},
		{name: "AWS SDK pointer", out: coffeeItemSDKEmbeddedPointer{ID: "intenso", Coffee: &Coffee{Name: "Intenso 12"}}},
		{name: "flat", out: coffeeItemFlat{ID: "intenso", Name: "Intenso 12"}},
		{name: "flat (invalid)", out: coffeeItemInvalid{}}, // want to make sure this doesn't panic
		{name: "embedded", out: coffeeItemEmbedded{ID: "intenso", Coffee: Coffee{Name: "Intenso 12"}}},
		{name: "embedded pointer", out: coffeeItemEmbeddedPointer{ID: "intenso", Coffee: &Coffee{Name: "Intenso 12"}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			out := reflect.New(reflect.TypeOf(test.out)).Interface()
			if err := dynamo.UnmarshalItem(in, out); err != nil {
				t.Fatal(err)
			}
			got := reflect.ValueOf(out).Elem().Interface()
			if !reflect.DeepEqual(test.out, got) {
				t.Error("bad value. want:", test.out, "got:", got)
			}
		})
	}
}

type coffeeItemFlat struct {
	ID   string
	Name string
}

func (c *coffeeItemFlat) UnmarshalDynamoItem(item map[string]types.AttributeValue) error {
	type alias coffeeItemFlat
	aux := struct {
		*alias
	}{
		alias: (*alias)(c),
	}
	if err := dynamo.UnmarshalItem(item, &aux); err != nil {
		return err
	}
	return nil
}

type coffeeItemInvalid struct {
	ID   string
	Name string
}

func (c *coffeeItemInvalid) UnmarshalDynamoItem(item map[string]types.AttributeValue) error {
	type alias coffeeItemInvalid
	aux := struct {
		*alias
	}{
		alias: (*alias)(nil),
	}
	if err := dynamo.UnmarshalItem(item, &aux); err != nil {
		return err
	}
	return nil
}

type coffeeItemEmbedded struct {
	ID string
	Coffee
}

func (c *coffeeItemEmbedded) UnmarshalDynamoItem(item map[string]types.AttributeValue) error {
	type alias coffeeItemEmbedded
	aux := struct {
		*alias
	}{
		alias: (*alias)(c),
	}
	if err := dynamo.UnmarshalItem(item, &aux); err != nil {
		return err
	}
	return nil
}

type coffeeItemEmbeddedPointer struct {
	ID string
	*Coffee
}

func (c *coffeeItemEmbeddedPointer) UnmarshalDynamoItem(item map[string]types.AttributeValue) error {
	type alias coffeeItemEmbeddedPointer
	aux := struct {
		*alias
	}{
		alias: (*alias)(c),
	}
	if err := dynamo.UnmarshalItem(item, &aux); err != nil {
		return err
	}
	return nil
}

func (c *coffeeItemEmbeddedPointer) UnmarshalJSON(data []byte) error {
	type alias coffeeItemEmbeddedPointer
	aux := struct {
		*alias
	}{
		alias: (*alias)(c),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	return nil
}

type coffeeItemSDKEmbeddedPointer struct {
	ID string
	*Coffee
}

func (c *coffeeItemSDKEmbeddedPointer) UnmarshalDynamoItem(item map[string]types.AttributeValue) error {
	type alias coffeeItemEmbeddedPointer
	aux := struct {
		*alias
	}{
		alias: (*alias)(c),
	}
	if err := attributevalue.UnmarshalMap(item, &aux); err != nil {
		return err
	}
	return nil
}
