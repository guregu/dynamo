package dynamo

import (
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Unmarshaler is the interface implemented by objects that can unmarshal
// an AttributeValue into themselves.
type Unmarshaler interface {
	UnmarshalDynamo(av types.AttributeValue) error
}

// ItemUnmarshaler is the interface implemented by objects that can unmarshal
// an Item (a map of strings to AttributeValues) into themselves.
type ItemUnmarshaler interface {
	UnmarshalDynamoItem(item Item) error
}

// Unmarshal decodes a DynamoDB item into out, which must be a pointer.
func UnmarshalItem(item Item, out interface{}) error {
	return unmarshalItem(item, out)
}

// Unmarshal decodes a DynamoDB value into out, which must be a pointer.
func Unmarshal(av types.AttributeValue, out interface{}) error {
	switch out := out.(type) {
	case awsEncoder:
		return attributevalue.Unmarshal(av, out.iface)
	}

	rv := reflect.ValueOf(out)
	plan, err := typedefOf(rv.Type())
	if err != nil {
		return err
	}
	return plan.decodeAttr(flagNone, av, rv)
}

// used in iterators for unmarshaling one item
type unmarshalFunc func(Item, interface{}) error

func unmarshalItem(item Item, out interface{}) error {
	rv := reflect.ValueOf(out)
	plan, err := typedefOf(rv.Type())
	if err != nil {
		return err
	}
	return plan.decodeItem(item, rv)
}

func unmarshalAppend(item Item, out interface{}) error {
	if awsenc, ok := out.(awsEncoder); ok {
		return unmarshalAppendAWS(item, awsenc.iface)
	}

	rv := reflect.ValueOf(out)
	if rv.Kind() != reflect.Ptr || rv.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("dynamo: unmarshal append: result argument must be a slice pointer")
	}

	slicev := rv.Elem()
	innerRV := reflect.New(slicev.Type().Elem())
	if err := unmarshalItem(item, innerRV.Interface()); err != nil {
		return err
	}
	slicev = reflect.Append(slicev, innerRV.Elem())

	rv.Elem().Set(slicev)
	return nil
}

func unmarshalAppendTo(out interface{}) func(item Item, out interface{}) error {
	if awsenc, ok := out.(awsEncoder); ok {
		return func(item Item, _ any) error {
			return unmarshalAppendAWS(item, awsenc.iface)
		}
	}

	ptr := reflect.ValueOf(out)
	slicet := ptr.Type().Elem()
	membert := slicet.Elem()
	if ptr.Kind() != reflect.Ptr || slicet.Kind() != reflect.Slice {
		return func(item Item, _ any) error {
			return fmt.Errorf("dynamo: unmarshal append: result argument must be a slice pointer")
		}
	}

	plan, err := typedefOf(membert)
	if err != nil {
		return func(item Item, _ any) error {
			return err
		}
	}

	/*
		Like:
			return func(item, ...) {
				member := new(T)
				decode(item, member)
				*slice = append(*slice, *member)
			}
	*/
	return func(item map[string]types.AttributeValue, _ any) error {
		member := reflect.New(membert) // *T of *[]T
		if err := plan.decodeItem(item, member); err != nil {
			return err
		}
		slice := ptr.Elem()
		slice = reflect.Append(slice, member.Elem())
		ptr.Elem().Set(slice)
		return nil
	}
}
