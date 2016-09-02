package dynamo

import (
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

// Run executes this put.
func (p *Put) Run() error {
	p.returnType = "NONE"
	_, err := p.run()
	return err
}

// OldValue executes this put, unmarshaling the previous value into out.
// Returns ErrNotFound is there was no previous value.
func (p *Put) OldValue(out interface{}) error {
	p.returnType = "ALL_OLD"
	output, err := p.run()
	switch {
	case err != nil:
		return err
	case output.Attributes == nil:
		return ErrNotFound
	}
	return unmarshalItem(output.Attributes, out)
}

func (p *Put) run() (output *dynamodb.PutItemOutput, err error) {
	if p.err != nil {
		return nil, p.err
	}

	req := p.input()
	retry(func() error {
		output, err = p.table.db.client.PutItem(req)
		return err
	})
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
	return input
}

func (p *Put) setError(err error) {
	if p.err != nil {
		p.err = err
	}
}
