package dynamo

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Delete is a request to delete an item.
// See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html
type Delete struct {
	table Table

	returnType types.ReturnValue
	onCondFail types.ReturnValuesOnConditionCheckFailure

	hashKey   string
	hashValue types.AttributeValue

	rangeKey   string
	rangeValue types.AttributeValue

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
	d.hashValue, d.err = marshal(value, flagNone)
	if d.hashValue == nil {
		d.setError(fmt.Errorf("dynamo: delete hash key value is nil or omitted for attribute %q", d.hashKey))
	}
	return d
}

// Range specifies the range key (a.k.a. sort key) to delete.
// Name is the name of the range key.
// Value is the value of the range key.
func (d *Delete) Range(name string, value interface{}) *Delete {
	var err error
	d.rangeKey = name
	d.rangeValue, err = marshal(value, flagNone)
	d.setError(err)
	if d.rangeValue == nil {
		d.setError(fmt.Errorf("dynamo: delete range key value is nil or omitted for attribute %q", d.rangeKey))
	}
	return d
}

// If specifies a conditional expression for this delete to succeed.
// Use single quotes to specificy reserved names inline (like 'Count').
// Use the placeholder ? within the expression to substitute values, and use $ for names.
// You need to use quoted or placeholder names when the name is a reserved word in DynamoDB.
// Multiple calls to If will be combined with AND.
func (d *Delete) If(expr string, args ...interface{}) *Delete {
	expr, err := d.subExprN(expr, args...)
	d.setError(err)
	if d.condition != "" {
		d.condition += " AND "
	}
	d.condition += wrapExpr(expr)
	return d
}

// ConsumedCapacity will measure the throughput capacity consumed by this operation and add it to cc.
func (d *Delete) ConsumedCapacity(cc *ConsumedCapacity) *Delete {
	d.cc = cc
	return d
}

// Run executes this delete request.
func (d *Delete) Run(ctx context.Context) error {
	d.returnType = types.ReturnValueNone
	_, err := d.run(ctx)
	return err
}

// OldValue executes this delete request, unmarshaling the previous value to out.
// Returns ErrNotFound is there was no previous value.
func (d *Delete) OldValue(ctx context.Context, out interface{}) error {
	d.returnType = types.ReturnValueAllOld
	output, err := d.run(ctx)
	switch {
	case err != nil:
		return err
	case output.Attributes == nil:
		return ErrNotFound
	}
	return unmarshalItem(output.Attributes, out)
}

// CurrentValue executes this delete.
// If successful, the return value `deleted` will be true, and nothing will be unmarshaled to `out`
//
// If the delete is unsuccessful because of a condition check failure, `deleted` will be false, the current value of the item will be unmarshaled to `out`, and `err` will be nil.
//
// If the delete is unsuccessful for any other reason, `deleted` will be false and `err` will be non-nil.
//
// See also: [UnmarshalItemFromCondCheckFailed].
func (d *Delete) CurrentValue(ctx context.Context, out interface{}) (wrote bool, err error) {
	d.returnType = types.ReturnValueNone
	d.onCondFail = types.ReturnValuesOnConditionCheckFailureAllOld
	_, err = d.run(ctx)
	if err != nil {
		if ok, err := UnmarshalItemFromCondCheckFailed(err, out); ok {
			return false, err
		}
		return false, err
	}
	return true, nil
}

// IncludeAllItemsInCondCheckFail specifies whether an item delete that fails its condition check should include the item itself in the error.
// Such items can be extracted using [UnmarshalItemFromCondCheckFailed] for single deletes, or [UnmarshalItemsFromTxCondCheckFailed] for write transactions.
func (d *Delete) IncludeItemInCondCheckFail(enabled bool) *Delete {
	if enabled {
		d.onCondFail = types.ReturnValuesOnConditionCheckFailureAllOld
	} else {
		d.onCondFail = types.ReturnValuesOnConditionCheckFailureNone
	}
	return d
}

func (d *Delete) run(ctx context.Context) (*dynamodb.DeleteItemOutput, error) {
	if d.err != nil {
		return nil, d.err
	}

	input := d.deleteInput()
	var output *dynamodb.DeleteItemOutput
	err := d.table.db.retry(ctx, func() error {
		var err error
		output, err = d.table.db.client.DeleteItem(ctx, input)
		d.cc.incRequests()
		return err
	})
	if output != nil {
		d.cc.add(output.ConsumedCapacity)
	}
	return output, err
}

func (d *Delete) deleteInput() *dynamodb.DeleteItemInput {
	input := &dynamodb.DeleteItemInput{
		TableName:                 &d.table.name,
		Key:                       d.key(),
		ReturnValues:              d.returnType,
		ExpressionAttributeNames:  d.nameExpr,
		ExpressionAttributeValues: d.valueExpr,
	}
	if d.condition != "" {
		input.ConditionExpression = &d.condition
		input.ReturnValuesOnConditionCheckFailure = d.onCondFail
	}
	if d.cc != nil {
		input.ReturnConsumedCapacity = types.ReturnConsumedCapacityIndexes
	}
	return input
}

func (d *Delete) writeTxItem() (*types.TransactWriteItem, error) {
	if d.err != nil {
		return nil, d.err
	}
	input := d.deleteInput()
	item := &types.TransactWriteItem{
		Delete: &types.Delete{
			TableName:                           input.TableName,
			Key:                                 input.Key,
			ExpressionAttributeNames:            input.ExpressionAttributeNames,
			ExpressionAttributeValues:           input.ExpressionAttributeValues,
			ConditionExpression:                 input.ConditionExpression,
			ReturnValuesOnConditionCheckFailure: input.ReturnValuesOnConditionCheckFailure,
		},
	}
	return item, nil
}

func (d *Delete) key() Item {
	key := Item{
		d.hashKey: d.hashValue,
	}
	if d.rangeKey != "" {
		key[d.rangeKey] = d.rangeValue
	}
	return key
}

func (d *Delete) setError(err error) {
	if d.err == nil {
		d.err = err
	}
}
