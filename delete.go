package dynamo

import (
	// "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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
	d.hashValue, d.err = marshal(value, "")
	return d
}

func (d *Delete) Range(name string, value interface{}) *Delete {
	var err error
	d.rangeKey = name
	d.rangeValue, err = marshal(value, "")
	d.setError(err)
	return d
}

func (d *Delete) Run() error {
	d.returnType = "NONE"
	_, err := d.run()
	return err
}

func (d *Delete) OldValue(out interface{}) error {
	d.returnType = "ALL_OLD"
	output, err := d.run()
	switch {
	case err != nil:
		return err
	case output.Attributes == nil:
		return ErrNotFound
	}
	return unmarshalItem(output.Attributes, out)
}

func (d *Delete) run() (*dynamodb.DeleteItemOutput, error) {
	if d.err != nil {
		return nil, d.err
	}

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
		TableName:    &d.table.Name,
		Key:          d.key(),
		ReturnValues: &d.returnType,
	}
	return input
}

func (d *Delete) key() map[string]*dynamodb.AttributeValue {
	key := map[string]*dynamodb.AttributeValue{
		d.hashKey: d.hashValue,
	}
	if d.rangeKey != "" {
		key[d.rangeKey] = d.rangeValue
	}
	return key
}

func (d *Delete) setError(err error) {
	if err != nil {
		d.err = err
	}
}
