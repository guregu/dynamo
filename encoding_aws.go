package dynamo

import (
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
