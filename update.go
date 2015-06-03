package dynamo

import (
	"fmt"
	"strings"

	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/service/dynamodb"
)

type Update struct {
	table      Table
	returnType string

	hashKey   string
	hashValue *dynamodb.AttributeValue

	rangeKey   string
	rangeValue *dynamodb.AttributeValue

	set    map[string]*dynamodb.AttributeValue
	add    map[string]*dynamodb.AttributeValue
	del    map[string]*dynamodb.AttributeValue
	remove map[string]struct{}

	subber

	err error
}

func (table Table) Update(hashKey string, value interface{}) *Update {
	u := &Update{
		table:   table,
		hashKey: hashKey,

		set:    make(map[string]*dynamodb.AttributeValue),
		add:    make(map[string]*dynamodb.AttributeValue),
		del:    make(map[string]*dynamodb.AttributeValue),
		remove: make(map[string]struct{}),
	}
	u.hashValue, u.err = marshal(value)
	return u
}

func (u *Update) Range(name string, value interface{}) *Update {
	var err error
	u.rangeKey = name
	u.rangeValue, err = marshal(value)
	u.setError(err)
	return u
}

func (u *Update) Set(name string, value interface{}) *Update {
	name = u.substitute(name)
	av, err := marshal(value)
	u.setError(err)
	u.set[name] = av
	return u
}

func (u *Update) Add(name string, value interface{}) *Update {
	name = u.substitute(name)
	av, err := marshal(value)
	u.setError(err)
	u.add[name] = av
	return u
}

func (u *Update) Delete(name string, value interface{}) *Update {
	name = u.substitute(name)
	av, err := marshal(value) // TODO: marshal slices to sets
	u.setError(err)
	u.del[name] = av
	return u
}

func (u *Update) Remove(names ...string) *Update {
	for _, n := range names {
		n = u.substitute(n)
		u.remove[n] = struct{}{}
	}
	return u
}

func (u *Update) Run() error {
	if u.err != nil {
		return u.err
	}

	u.returnType = "NONE"
	_, err := u.run()
	return err
}

func (u *Update) Value(out interface{}) error {
	if u.err != nil {
		return u.err
	}

	u.returnType = "ALL_NEW"
	output, err := u.run()
	if err != nil {
		return err
	}
	return unmarshalItem(output.Attributes, out)
}

func (u *Update) OldValue(out interface{}) error {
	if u.err != nil {
		return u.err
	}

	u.returnType = "ALL_OLD"
	output, err := u.run()
	if err != nil {
		return err
	}
	return unmarshalItem(output.Attributes, out)
}

func (u *Update) run() (*dynamodb.UpdateItemOutput, error) {
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
		ExpressionAttributeNames:  u.nameMap(),
		ExpressionAttributeValues: u.expvals(),
		ReturnValues:              &u.returnType,
	}
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
	for k, _ := range u.set {
		n := u.unsub(k)
		sets = append(sets, fmt.Sprintf("%s = :s%s", k, n))
	}
	if len(sets) > 0 {
		expr = append(expr, "SET", strings.Join(sets, ", "))
	}

	adds := make([]string, 0, len(u.add))
	for k, _ := range u.add {
		n := u.unsub(k)
		adds = append(adds, fmt.Sprintf("%s :a%s", k, n))
	}
	if len(adds) > 0 {
		expr = append(expr, "ADD", strings.Join(adds, ", "))
	}

	dels := make([]string, 0, len(u.del))
	for k, _ := range u.del {
		n := u.unsub(k)
		dels = append(dels, fmt.Sprintf("%s :d%s", k, n))
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

func (u *Update) expvals() map[string]*dynamodb.AttributeValue {
	l := len(u.set) + len(u.add) + len(u.del)
	if l == 0 {
		return nil
	}
	expvals := make(map[string]*dynamodb.AttributeValue, l)
	for k, v := range u.set {
		k = u.unsub(k)
		ev := fmt.Sprintf(":s%s", k)
		expvals[ev] = v
	}
	for k, v := range u.add {
		k = u.unsub(k)
		ev := fmt.Sprintf(":a%s", k)
		expvals[ev] = v
	}
	for k, v := range u.del {
		k = u.unsub(k)
		ev := fmt.Sprintf(":d%s", k)
		expvals[ev] = v
	}
	return expvals
}

func (u *Update) setError(err error) {
	if err != nil {
		u.err = err
	}
}
