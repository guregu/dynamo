package dynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type ConditionCheck struct {
	table      Table
	hashKey    string
	hashValue  *dynamodb.AttributeValue
	rangeKey   string
	rangeValue *dynamodb.AttributeValue

	condition string
	subber

	err error
}

func (table Table) Check(hashKey string, value interface{}) *ConditionCheck {
	check := &ConditionCheck{
		table:   table,
		hashKey: hashKey,
	}
	check.hashValue, check.err = marshal(value, "")
	return check
}

func (check *ConditionCheck) Range(rangeKey string, value interface{}) *ConditionCheck {
	check.rangeKey = rangeKey
	var err error
	check.rangeValue, err = marshal(value, "")
	check.setError(err)
	return check
}

// If specifies a conditional expression for this coniditon check to succeed.
// Use single quotes to specificy reserved names inline (like 'Count').
// Use the placeholder ? within the expression to substitute values, and use $ for names.
// You need to use quoted or placeholder names when the name is a reserved word in DynamoDB.
func (check *ConditionCheck) If(expr string, args ...interface{}) *ConditionCheck {
	cond, err := check.subExpr(expr, args...)
	check.setError(err)
	check.condition = cond
	return check
}

func (check *ConditionCheck) writeTxItem() (*dynamodb.TransactWriteItem, error) {
	if check.err != nil {
		return nil, check.err
	}
	item := &dynamodb.ConditionCheck{
		TableName: aws.String(check.table.name),
		Key:       check.keys(),
		ExpressionAttributeNames:  check.nameExpr,
		ExpressionAttributeValues: check.valueExpr,
	}
	if check.condition != "" {
		item.ConditionExpression = aws.String(check.condition)
	}
	return &dynamodb.TransactWriteItem{
		ConditionCheck: item,
	}, nil
}

func (check *ConditionCheck) keys() map[string]*dynamodb.AttributeValue {
	keys := map[string]*dynamodb.AttributeValue{check.hashKey: check.hashValue}
	if check.rangeKey != "" {
		keys[check.rangeKey] = check.rangeValue
	}
	return keys
}

func (check *ConditionCheck) setError(err error) {
	if check.err == nil {
		check.err = err
	}
}
