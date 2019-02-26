package dynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// ConditionCheck represents a condition for a write transaction to succeed.
// It is used along with WriteTx.Check.
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

// Check creates a new ConditionCheck, which represents a condition for a write transaction to succeed.
// hashKey specifies the name of the table's hash key and value specifies the value of the hash key.
// You must use Range to specify a range key for tables with hash and range keys.
func (table Table) Check(hashKey string, value interface{}) *ConditionCheck {
	check := &ConditionCheck{
		table:   table,
		hashKey: hashKey,
	}
	check.hashValue, check.err = marshal(value, "")
	return check
}

// Range specifies the name and value of the range key for this item.
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
// Multiple calls to If will be combined with AND.
func (check *ConditionCheck) If(expr string, args ...interface{}) *ConditionCheck {
	expr = wrapExpr(expr)
	cond, err := check.subExpr(expr, args...)
	check.setError(err)
	if check.condition != "" {
		check.condition += " AND "
	}
	check.condition += cond
	return check
}

// IfExists sets this check to succeed if the item exists.
func (check *ConditionCheck) IfExists() *ConditionCheck {
	return check.If("attribute_exists($)", check.hashKey)
}

// IfNotExists sets this check to succeed if the item does not exist.
func (check *ConditionCheck) IfNotExists() *ConditionCheck {
	return check.If("attribute_not_exists($)", check.hashKey)
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
