package dynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Delete is a request to delete an item.
// See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html
type Delete struct {
	table      Table
	returnType string

	hashKey   string
	hashValue *dynamodb.AttributeValue

	rangeKey   string
	rangeValue *dynamodb.AttributeValue

	subber
	condition string

	err error
	cc  *ConsumedCapacity
}

// Delete creates a new request to delete an item.
// Key is the name of the hash key (a.k.a. partition key).
// Value is the value of the hash key.
func (table Table) Delete(name string, value interface{}) *Delete {
	d := &Delete{
		table:   table,
		hashKey: name,
	}
	d.hashValue, d.err = marshal(value, "")
	return d
}

// Range specifies the range key (a.k.a. sort key) to delete.
// Name is the name of the range key.
// Value is the value of the range key.
func (d *Delete) Range(name string, value interface{}) *Delete {
	var err error
	d.rangeKey = name
	d.rangeValue, err = marshal(value, "")
	d.setError(err)
	return d
}

// If specifies a conditional expression for this delete to succeed.
// Use single quotes to specificy reserved names inline (like 'Count').
// Use the placeholder ? within the expression to substitute values, and use $ for names.
// You need to use quoted or placeholder names when the name is a reserved word in DynamoDB.
func (d *Delete) If(expr string, args ...interface{}) *Delete {
	expr, err := d.subExpr(expr, args...)
	d.setError(err)
	d.condition = expr
	return d
}

// ConsumedCapacity will measure the throughput capacity consumed by this operation and add it to cc.
func (d *Delete) ConsumedCapacity(cc *ConsumedCapacity) *Delete {
	d.cc = cc
	return d
}

// Run executes this delete request.
func (d *Delete) Run() error {
	ctx, cancel := defaultContext()
	defer cancel()
	return d.RunWithContext(ctx)
}

func (d *Delete) RunWithContext(ctx aws.Context) error {
	d.returnType = "NONE"
	_, err := d.run(ctx)
	return err
}

// OldValue executes this delete request, unmarshaling the previous value to out.
// Returns ErrNotFound is there was no previous value.
func (d *Delete) OldValue(out interface{}) error {
	ctx, cancel := defaultContext()
	defer cancel()
	return d.OldValueWithContext(ctx, out)
}

func (d *Delete) OldValueWithContext(ctx aws.Context, out interface{}) error {
	d.returnType = "ALL_OLD"
	output, err := d.run(ctx)
	switch {
	case err != nil:
		return err
	case output.Attributes == nil:
		return ErrNotFound
	}
	return unmarshalItem(output.Attributes, out)
}

func (d *Delete) run(ctx aws.Context) (*dynamodb.DeleteItemOutput, error) {
	if d.err != nil {
		return nil, d.err
	}

	input := d.deleteInput()
	var output *dynamodb.DeleteItemOutput
	err := retry(ctx, func() error {
		var err error
		output, err = d.table.db.client.DeleteItemWithContext(ctx, input)
		return err
	})
	if d.cc != nil {
		addConsumedCapacity(d.cc, output.ConsumedCapacity)
	}
	return output, err
}

func (d *Delete) deleteInput() *dynamodb.DeleteItemInput {
	input := &dynamodb.DeleteItemInput{
		TableName:                 &d.table.name,
		Key:                       d.key(),
		ReturnValues:              &d.returnType,
		ExpressionAttributeNames:  d.nameExpr,
		ExpressionAttributeValues: d.valueExpr,
	}
	if d.condition != "" {
		input.ConditionExpression = &d.condition
	}
	if d.cc != nil {
		input.ReturnConsumedCapacity = aws.String(dynamodb.ReturnConsumedCapacityIndexes)
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
