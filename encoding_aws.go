package dynamo

import (
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type Coder interface {
	Marshaler
	Unmarshaler
}

type awsEncoder struct {
	iface interface{}
}

func (w awsEncoder) MarshalDynamo() (*dynamodb.AttributeValue, error) {
	return dynamodbattribute.Marshal(w.iface)
}

func (w awsEncoder) UnmarshalDynamo(av *dynamodb.AttributeValue) error {
	return dynamodbattribute.Unmarshal(av, w.iface)
}

// AWSEncoding wraps an object, forcing it to use AWS's official dynamodbattribute package
// for encoding and decoding. This allows you to use the "dynamodbav" struct tags.
// When decoding, v must be a pointer.
func AWSEncoding(v interface{}) Coder {
	return awsEncoder{v}
}

func unmarshalAppendAWS(item map[string]*dynamodb.AttributeValue, out interface{}) error {
	rv := reflect.ValueOf(out)
	if rv.Kind() != reflect.Ptr || rv.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("dynamo: unmarshal append AWS: result argument must be a slice pointer")
	}

	slicev := rv.Elem()
	innerRV := reflect.New(slicev.Type().Elem())
	if err := dynamodbattribute.UnmarshalMap(item, innerRV.Interface()); err != nil {
		return err
	}
	slicev = reflect.Append(slicev, innerRV.Elem())

	rv.Elem().Set(slicev)
	return nil
}
