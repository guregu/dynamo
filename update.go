package dynamo

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Update represents changes to an existing item.
// It uses the UpdateItem API.
// See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
type Update struct {
	table      Table
	returnType string

	hashKey   string
	hashValue *dynamodb.AttributeValue

	rangeKey   string
	rangeValue *dynamodb.AttributeValue

	set    []string
	add    map[string]string
	del    map[string]string
	remove map[string]struct{}

	condition string

	subber

	err error
}

// Update creates a new request to modify an existing item.
func (table Table) Update(hashKey string, value interface{}) *Update {
	u := &Update{
		table:   table,
		hashKey: hashKey,

		set:    make([]string, 0),
		add:    make(map[string]string),
		del:    make(map[string]string),
		remove: make(map[string]struct{}),
	}
	u.hashValue, u.err = marshal(value, "")
	return u
}

// Range specifies the range key (sort key) for the item to update.
func (u *Update) Range(name string, value interface{}) *Update {
	var err error
	u.rangeKey = name
	u.rangeValue, err = marshal(value, "")
	u.setError(err)
	return u
}

// Set changes path to the given value.
// Paths that are reserved words are automatically escaped.
// Use single quotes to escape complex values like 'User'.'Count'.
func (u *Update) Set(path string, value interface{}) *Update {
	path, err := u.escape(path)
	u.setError(err)
	expr, err := u.subExpr("🝕 = ?", path, value)
	u.setError(err)
	u.set = append(u.set, expr)
	return u
}

// SetIfNotExists changes path to the given value, if it does not already exist.
func (u *Update) SetIfNotExists(path string, value interface{}) *Update {
	path, err := u.escape(path)
	u.setError(err)
	expr, err := u.subExpr("🝕 = if_not_exists(🝕, ?)", path, path, value)
	u.setError(err)
	u.set = append(u.set, expr)
	return u
}

// Append appends value  to the end of the list specified by path.
func (u *Update) Append(path string, value interface{}) *Update {
	path, err := u.escape(path)
	u.setError(err)
	expr, err := u.subExpr("🝕 = list_append(🝕, ?)", path, path, value)
	u.setError(err)
	u.set = append(u.set, expr)
	return u
}

// Prepend inserts value to the beginning of the list specified by path.
func (u *Update) Prepend(path string, value interface{}) *Update {
	path, err := u.escape(path)
	u.setError(err)
	expr, err := u.subExpr("🝕 = list_append(?, 🝕)", path, value, path)
	u.setError(err)
	u.set = append(u.set, expr)
	return u
}

// Add adds value to path.
// Path can be a number or a set.
// If path represents a set, value must be []int or []string.
// Path must be a top-level attribute.
func (u *Update) Add(path string, value interface{}) *Update {
	path, err := u.escape(path)
	u.setError(err)
	vsub, err := u.subValue(value, "set")
	u.setError(err)
	u.add[path] = vsub
	return u
}

// AddStringsToSet adds the given values to the string set specified by path.
func (u *Update) AddStringsToSet(path string, values ...string) *Update {
	return u.Add(path, values)
}

// AddIntsToSet adds the given values to the number set specified by path.
func (u *Update) AddIntsToSet(path string, values ...int) *Update {
	return u.Add(path, values)
}

// AddFloatsToSet adds the given values to the number set specified by path.
func (u *Update) AddFloatsToSet(path string, values ...float64) *Update {
	return u.Add(path, values)
}

func (u *Update) delete(path string, value interface{}) *Update {
	path, err := u.escape(path)
	u.setError(err)
	vsub, err := u.subValue(value, "set")
	u.setError(err)
	u.del[path] = vsub
	return u
}

// DeleteStringsFromSet deletes the given values from the string set specified by path.
func (u *Update) DeleteStringsFromSet(path string, values ...string) *Update {
	return u.delete(path, values)
}

// DeleteIntsFromSet deletes the given values from the number set specified by path.
func (u *Update) DeleteIntsFromSet(path string, values ...int) *Update {
	return u.delete(path, values)
}

// DeleteFloatsFromSet deletes the given values from the number set specified by path.
func (u *Update) DeleteFloatsFromSet(path string, values ...float64) *Update {
	return u.delete(path, values)
}

// Remove removes the paths from this item, deleting the specified attributes.
func (u *Update) Remove(paths ...string) *Update {
	for _, n := range paths {
		n, err := u.escape(n)
		u.setError(err)
		u.remove[n] = struct{}{}
	}
	return u
}

// If specifies a conditional expression for this update to succeed.
// Use single quotes to specificy reserved names inline (like 'Count').
// Use the placeholder ? within the expression to substitute values, and use $ for names.
// You need to use quoted or placeholder names when the name is a reserved word in DynamoDB.
func (u *Update) If(expr string, args ...interface{}) *Update {
	cond, err := u.subExpr(expr, args...)
	u.setError(err)
	u.condition = cond
	return u
}

// Run executes this update.
func (u *Update) Run() error {
	u.returnType = "NONE"
	_, err := u.run()
	return err
}

// Value executes this update, encoding out with the new value.
func (u *Update) Value(out interface{}) error {
	u.returnType = "ALL_NEW"
	output, err := u.run()
	if err != nil {
		return err
	}
	return unmarshalItem(output.Attributes, out)
}

// OldValue executes this update, encoding out with the previous value.
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
		TableName:                 &u.table.name,
		Key:                       u.key(),
		UpdateExpression:          u.updateExpr(),
		ExpressionAttributeNames:  u.nameExpr,
		ExpressionAttributeValues: u.valueExpr,
		ReturnValues:              &u.returnType,
	}
	if u.condition != "" {
		input.ConditionExpression = &u.condition
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

	if len(u.set) > 0 {
		expr = append(expr, "SET", strings.Join(u.set, ", "))
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
	for k := range u.remove {
		rems = append(rems, k)
	}
	if len(rems) > 0 {
		expr = append(expr, "REMOVE", strings.Join(rems, ", "))
	}

	joined := strings.Join(expr, " ")
	return &joined
}

func (u *Update) setError(err error) {
	if err != nil {
		u.err = err
	}
}
