package dynamo

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Put is a request to create or replace an item.
// See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html
type Put struct {
	table      Table
	returnType string

	item Item
	subber
	condition string

	err error
	cc  *ConsumedCapacity
}

// Put creates a new request to create or replace an item.
func (table Table) Put(item interface{}) *Put {
	encoded, err := marshalItem(item)
	return &Put{
		table: table,
		item:  encoded,
		err:   err,
	}
}

// If specifies a conditional expression for this put to succeed.
// Use single quotes to specificy reserved names inline (like 'Count').
// Use the placeholder ? within the expression to substitute values, and use $ for names.
// You need to use quoted or placeholder names when the name is a reserved word in DynamoDB.
// Multiple calls to If will be combined with AND.
func (p *Put) If(expr string, args ...interface{}) *Put {
	expr, err := p.subExprN(expr, args...)
	p.setError(err)
	if p.condition != "" {
		p.condition += " AND "
	}
	p.condition += wrapExpr(expr)
	return p
}

// ConsumedCapacity will measure the throughput capacity consumed by this operation and add it to cc.
func (p *Put) ConsumedCapacity(cc *ConsumedCapacity) *Put {
	p.cc = cc
	return p
}

// Run executes this put.
func (p *Put) Run(ctx context.Context) error {
	p.returnType = "NONE"
	_, err := p.run(ctx)
	return err
}

// OldValue executes this put, unmarshaling the previous value into out.
// Returns ErrNotFound is there was no previous value.
func (p *Put) OldValue(ctx context.Context, out interface{}) error {
	p.returnType = "ALL_OLD"
	output, err := p.run(ctx)
	switch {
	case err != nil:
		return err
	case output.Attributes == nil:
		return ErrNotFound
	}
	return unmarshalItem(output.Attributes, out)
}

func (p *Put) run(ctx context.Context) (output *dynamodb.PutItemOutput, err error) {
	if p.err != nil {
		return nil, p.err
	}

	req := p.input()
	p.table.db.retry(ctx, func() error {
		output, err = p.table.db.client.PutItem(ctx, req)
		p.cc.incRequests()
		return err
	})
	if output != nil {
		p.cc.add(output.ConsumedCapacity)
	}
	return
}

func (p *Put) input() *dynamodb.PutItemInput {
	input := &dynamodb.PutItemInput{
		TableName:                 &p.table.name,
		Item:                      p.item,
		ReturnValues:              types.ReturnValue(p.returnType),
		ExpressionAttributeNames:  p.nameExpr,
		ExpressionAttributeValues: p.valueExpr,
	}
	if p.condition != "" {
		input.ConditionExpression = &p.condition
	}
	if p.cc != nil {
		input.ReturnConsumedCapacity = types.ReturnConsumedCapacityIndexes
	}
	return input
}

func (p *Put) writeTxItem() (*types.TransactWriteItem, error) {
	if p.err != nil {
		return nil, p.err
	}
	input := p.input()
	item := &types.TransactWriteItem{
		Put: &types.Put{
			TableName:                 input.TableName,
			Item:                      input.Item,
			ExpressionAttributeNames:  input.ExpressionAttributeNames,
			ExpressionAttributeValues: input.ExpressionAttributeValues,
			ConditionExpression:       input.ConditionExpression,
			// TODO: add support when aws-sdk-go updates
			// ReturnValuesOnConditionCheckFailure: aws.String(dynamodb.ReturnValuesOnConditionCheckFailureAllOld),
		},
	}
	return item, nil
}

func (p *Put) setError(err error) {
	if p.err == nil {
		p.err = err
	}
}
