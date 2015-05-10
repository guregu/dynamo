package dynamo

import (
	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/service/dynamodb"
)

type Delete struct {
	table      Table
	returnType string

	hashKey   string
	hashValue *dynamodb.AttributeValue

	rangeKey   string
	rangeValue *dynamodb.AttributeValue

	err error
}

func (table Table) Delete(hashKey string, value interface{}) *Delete {
	d := &Delete{
		table:   table,
		hashKey: hashKey,
	}
	d.hashValue, d.err = marshal(value)
	return d
}

func (d *Delete) Range(name string, value interface{}) *Delete {
	var err error
	d.rangeKey = name
	d.rangeValue, err = marshal(value)
	d.setError(err)
	return d
}

func (d *Delete) Run() error {
	if d.err != nil {
		return d.err
	}

	d.returnType = "NONE"
	_, err := d.run()
	return err
}

func (d *Delete) run() (*dynamodb.DeleteItemOutput, error) {
	input := d.deleteInput()
	var output *dynamodb.DeleteItemOutput
	err := retry(func() error {
		var err error
		output, err = d.table.db.client.DeleteItem(input)
		return err
	})
	return output, err
}

func (d *Delete) deleteInput() *dynamodb.DeleteItemInput {
	input := &dynamodb.DeleteItemInput{
		TableName:    aws.String(d.table.Name),
		Key:          d.key(),
		ReturnValues: &d.returnType,
	}
	return input
}

func (d *Delete) key() *map[string]*dynamodb.AttributeValue {
	key := map[string]*dynamodb.AttributeValue{
		d.hashKey: d.hashValue,
	}
	if d.rangeKey != "" {
		key[d.rangeKey] = d.rangeValue
	}
	return &key
}

func (d *Delete) setError(err error) {
	if err != nil {
		d.err = err
	}
}
