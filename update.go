package dynamo

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Update struct {
	table      Table
	returnType string

	hashKey   string
	hashValue *dynamodb.AttributeValue

	rangeKey   string
	rangeValue *dynamodb.AttributeValue

	set    map[string]string
	add    map[string]string
	del    map[string]string
	remove map[string]struct{}

	condition string

	subber

	err error
}

func (table Table) Update(hashKey string, value interface{}) *Update {
	u := &Update{
		table:   table,
		hashKey: hashKey,

		set:    make(map[string]string),
		add:    make(map[string]string),
		del:    make(map[string]string),
		remove: make(map[string]struct{}),
	}
	u.hashValue, u.err = marshal(value, "")
	return u
}

func (u *Update) Range(name string, value interface{}) *Update {
	var err error
	u.rangeKey = name
	u.rangeValue, err = marshal(value, "")
	u.setError(err)
	return u
}

func (u *Update) Set(name string, value interface{}) *Update {
	name = u.subName(name)
	vsub, err := u.subValue(value)
	u.setError(err)
	u.set[name] = vsub
	return u
}

func (u *Update) Add(name string, value interface{}) *Update {
	name = u.subName(name)
	vsub, err := u.subValue(value)
	u.setError(err)
	u.add[name] = vsub
	return u
}

// Delete removes the given value from the set specified by name.
func (u *Update) Delete(name string, value interface{}) *Update {
	name = u.subName(name)
	vsub, err := u.subValue(value)
	u.setError(err)
	u.del[name] = vsub
	return u
}

func (u *Update) Remove(names ...string) *Update {
	for _, n := range names {
		n = u.subName(n)
		u.remove[n] = struct{}{}
	}
	return u
}

// If specifies a conditional expression for this update.
// Use the placeholder ? within the expression to substitute values, and use $ for names.
// You need to use placeholder names when the name is a reserved word in DynamoDB.
func (u *Update) If(expr string, args ...interface{}) *Update {
	cond, err := u.subExpr(expr, args)
	u.setError(err)
	u.condition = cond
	return u
}

func (u *Update) Run() error {
	u.returnType = "NONE"
	_, err := u.run()
	return err
}

func (u *Update) Value(out interface{}) error {
	u.returnType = "ALL_NEW"
	output, err := u.run()
	if err != nil {
		return err
	}
	return unmarshalItem(output.Attributes, out)
}

func (u *Update) OldValue(out interface{}) error {
	u.returnType = "ALL_OLD"
	output, err := u.run()
	if err != nil {
		return err
	}
	return unmarshalItem(output.Attributes, out)
}

func (u *Update) run() (*dynamodb.UpdateItemOutput, error) {
	if u.err != nil {
		return nil, u.err
	}

	input := u.updateInput()
	var output *dynamodb.UpdateItemOutput
	err := retry(func() error {
		var err error
		output, err = u.table.db.client.UpdateItem(input)
		return err
	})
	return output, err
}

func (u *Update) updateInput() *dynamodb.UpdateItemInput {
	input := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(u.table.Name),
		Key:                       u.key(),
		UpdateExpression:          u.updateExpr(),
		ExpressionAttributeNames:  u.nameExpr,
		ExpressionAttributeValues: u.valueExpr,
		ReturnValues:              &u.returnType,
	}
	if u.condition != "" {
		input.ConditionExpression = &u.condition
	}
	fmt.Printf("UPDATE: %#v\n", input)
	return input
}

func (u *Update) key() map[string]*dynamodb.AttributeValue {
	key := map[string]*dynamodb.AttributeValue{
		u.hashKey: u.hashValue,
	}
	if u.rangeKey != "" {
		key[u.rangeKey] = u.rangeValue
	}
	return key
}

func (u *Update) updateExpr() *string {
	var expr []string

	sets := make([]string, 0, len(u.set))
	for k, v := range u.set {
		sets = append(sets, fmt.Sprintf("%s = %s", k, v))
	}
	if len(sets) > 0 {
		expr = append(expr, "SET", strings.Join(sets, ", "))
	}

	adds := make([]string, 0, len(u.add))
	for k, v := range u.add {
		adds = append(adds, fmt.Sprintf("%s %s", k, v))
	}
	if len(adds) > 0 {
		expr = append(expr, "ADD", strings.Join(adds, ", "))
	}

	dels := make([]string, 0, len(u.del))
	for k, v := range u.del {
		dels = append(dels, fmt.Sprintf("%s %s", k, v))
	}
	if len(dels) > 0 {
		expr = append(expr, "DELETE", strings.Join(dels, ", "))
	}

	rems := make([]string, 0, len(u.remove))
	for k, _ := range u.remove {
		rems = append(rems, k)
	}
	if len(rems) > 0 {
		expr = append(expr, "REMOVE", strings.Join(rems, ", "))
	}

	return aws.String(strings.Join(expr, " "))
}

func (u *Update) setError(err error) {
	if err != nil {
		u.err = err
	}
}
