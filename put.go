package dynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Put is a request to create or replace an item.
// See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html
type Put struct {
	table      Table
	returnType string

	item map[string]*dynamodb.AttributeValue
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
func (p *Put) If(expr string, args ...interface{}) *Put {
	expr, err := p.subExpr(expr, args...)
	p.setError(err)
	p.condition = expr
	return p
}

// ConsumedCapacity will measure the throughput capacity consumed by this operation and add it to cc.
func (p *Put) ConsumedCapacity(cc *ConsumedCapacity) *Put {
	p.cc = cc
	return p
}

// Run executes this put.
func (p *Put) Run() error {
	ctx, cancel := defaultContext()
	defer cancel()
	return p.RunWithContext(ctx)
}

// Run executes this put.
func (p *Put) RunWithContext(ctx aws.Context) error {
	p.returnType = "NONE"
	_, err := p.run(ctx)
	return err
}

// OldValue executes this put, unmarshaling the previous value into out.
// Returns ErrNotFound is there was no previous value.
func (p *Put) OldValue(out interface{}) error {
	ctx, cancel := defaultContext()
	defer cancel()
	return p.OldValueWithContext(ctx, out)
}

// OldValue executes this put, unmarshaling the previous value into out.
// Returns ErrNotFound is there was no previous value.
func (p *Put) OldValueWithContext(ctx aws.Context, out interface{}) error {
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

func (p *Put) run(ctx aws.Context) (output *dynamodb.PutItemOutput, err error) {
	if p.err != nil {
		return nil, p.err
	}

	req := p.input()
	retry(ctx, func() error {
		output, err = p.table.db.client.PutItemWithContext(ctx, req)
		return err
	})
	if p.cc != nil {
		addConsumedCapacity(p.cc, output.ConsumedCapacity)
	}
	return
}

func (p *Put) input() *dynamodb.PutItemInput {
	input := &dynamodb.PutItemInput{
		TableName:                 &p.table.name,
		Item:                      p.item,
		ReturnValues:              &p.returnType,
		ExpressionAttributeNames:  p.nameExpr,
		ExpressionAttributeValues: p.valueExpr,
	}
	if p.condition != "" {
		input.ConditionExpression = &p.condition
	}
	if p.cc != nil {
		input.ReturnConsumedCapacity = aws.String(dynamodb.ReturnConsumedCapacityIndexes)
	}
	return input
}

func (p *Put) setError(err error) {
	if p.err != nil {
		p.err = err
	}
}
