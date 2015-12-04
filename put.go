package dynamo

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Put struct {
	table      Table
	returnType string

	item map[string]*dynamodb.AttributeValue
	subber
	condition string

	err error
}

// Put creates a new item or replaces an existing one.
func (table Table) Put(item interface{}) *Put {
	encoded, err := marshalItem(item)
	return &Put{
		table: table,
		item:  encoded,
		err:   err,
	}
}

func (p *Put) If(expr string, args ...interface{}) *Put {
	expr, err := p.subExpr(expr, args)
	p.setError(err)
	p.condition = expr
	return p
}

func (p *Put) Run() error {
	p.returnType = "NONE"
	_, err := p.run()
	return err
}

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
		TableName:                 &p.table.Name,
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
