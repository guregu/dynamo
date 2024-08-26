package dynamo

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Update represents changes to an existing item.
// It uses the UpdateItem API.
// See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
type Update struct {
	table Table

	returnType types.ReturnValue
	onCondFail types.ReturnValuesOnConditionCheckFailure

	hashKey   string
	hashValue types.AttributeValue

	rangeKey   string
	rangeValue types.AttributeValue

	set    []string
	add    map[string]string
	del    map[string]string
	remove map[string]struct{}

	condition string

	subber

	err error
	cc  *ConsumedCapacity
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
	u.hashValue, u.err = marshal(value, flagNone)
	if u.hashValue == nil {
		u.setError(fmt.Errorf("dynamo: update hash key value is nil or omitted for attribute %q", u.hashKey))
	}
	return u
}

// Range specifies the range key (sort key) for the item to update.
func (u *Update) Range(name string, value interface{}) *Update {
	var err error
	u.rangeKey = name
	u.rangeValue, err = marshal(value, flagNone)
	u.setError(err)
	if u.rangeValue == nil {
		u.setError(fmt.Errorf("dynamo: update range key value is nil or omitted for attribute %q", u.rangeKey))
	}
	return u
}

// Set changes path to the given value.
// If value is an empty string or nil, path will be removed instead.
// Paths that are reserved words are automatically escaped.
// Use single quotes to escape complex values like 'User'.'Count'.
func (u *Update) Set(path string, value interface{}) *Update {
	v, err := marshal(value, flagNone)
	if v == nil && err == nil {
		// auto-omitted value
		return u.Remove(path)
	}
	u.setError(err)

	path, err = u.escape(path)
	u.setError(err)
	expr, err := u.subExpr("🝕 = ?", path, v)
	u.setError(err)
	u.set = append(u.set, expr)
	return u
}

// SetNullable changes path to the given value, allowing empty and nil values.
// If value is an empty string or []byte, it will be set as-is.
// If value is nil, the DynamoDB NULL type will be used.
// Paths that are reserved words are automatically escaped.
// Use single quotes to escape complex values like 'User'.'Count'.
func (u *Update) SetNullable(path string, value interface{}) *Update {
	path, err := u.escape(path)
	u.setError(err)
	expr, err := u.subExprN("🝕 = ?", path, value)
	u.setError(err)
	u.set = append(u.set, expr)
	return u
}

// SetSet changes a set at the given path to the given value.
// SetSet marshals value to a string set, number set, or binary set.
// If value is of zero length or nil, path will be removed instead.
// Paths that are reserved words are automatically escaped.
// Use single quotes to escape complex values like 'User'.'Count'.
func (u *Update) SetSet(path string, value interface{}) *Update {
	v, err := marshal(value, flagSet)
	if v == nil && err == nil {
		// empty set
		return u.Remove(path)
	}
	u.setError(err)

	path, err = u.escape(path)
	u.setError(err)
	expr, err := u.subExpr("🝕 = ?", path, v)
	u.setError(err)
	u.set = append(u.set, expr)
	return u
}

// SetIfNotExists changes path to the given value, if it does not already exist.
func (u *Update) SetIfNotExists(path string, value interface{}) *Update {
	path, err := u.escape(path)
	u.setError(err)
	expr, err := u.subExprN("🝕 = if_not_exists(🝕, ?)", path, path, value)
	u.setError(err)
	u.set = append(u.set, expr)
	return u
}

// SetExpr performs a custom set expression, substituting the args into expr as in filter expressions.
// See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html#DDB-UpdateItem-request-UpdateExpression
//
//	SetExpr("MyMap.$.$ = ?", key1, key2, val)
//	SetExpr("'Counter' = 'Counter' + ?", 1)
func (u *Update) SetExpr(expr string, args ...interface{}) *Update {
	expr, err := u.subExprN(expr, args...)
	u.setError(err)
	u.set = append(u.set, expr)
	return u
}

// Append appends value to the end of the list specified by path.
func (u *Update) Append(path string, value interface{}) *Update {
	path, err := u.escape(path)
	u.setError(err)
	expr, err := u.subExprN("🝕 = list_append(🝕, ?)", path, path, value)
	u.setError(err)
	u.set = append(u.set, expr)
	return u
}

// Prepend inserts value to the beginning of the list specified by path.
func (u *Update) Prepend(path string, value interface{}) *Update {
	path, err := u.escape(path)
	u.setError(err)
	expr, err := u.subExprN("🝕 = list_append(?, 🝕)", path, value, path)
	u.setError(err)
	u.set = append(u.set, expr)
	return u
}

// Add adds value to path.
// Path can be a number or a set.
// If path represents a number, value is atomically added to the number.
// If path represents a set, value must be a slice, a map[*]struct{}, or map[*]bool.
// Path must be a top-level attribute.
func (u *Update) Add(path string, value interface{}) *Update {
	path, err := u.escape(path)
	u.setError(err)
	vsub, err := u.subValue(value, flagSet)
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
	vsub, err := u.subValue(value, flagSet)
	u.setError(err)
	u.del[path] = vsub
	return u
}

// DeleteFromSet deletes value from the set given by path.
// If value marshals to a set, those values will be deleted.
// If value marshals to a number, string, or binary, that value will be deleted.
// Delete is only for deleting values from sets. See Remove for removing entire attributes.
func (u *Update) DeleteFromSet(path string, value interface{}) *Update {
	v, err := marshal(value, flagSet)
	if err != nil {
		u.setError(err)
		return u
	}
	switch t := v.(type) {
	// ok:
	case *types.AttributeValueMemberNS, *types.AttributeValueMemberSS, *types.AttributeValueMemberBS:

	// need to box:
	case *types.AttributeValueMemberN:
		v = &types.AttributeValueMemberNS{Value: []string{t.Value}}
	case *types.AttributeValueMemberS:
		v = &types.AttributeValueMemberSS{Value: []string{t.Value}}
	case *types.AttributeValueMemberB:
		v = &types.AttributeValueMemberBS{Value: [][]byte{t.Value}}

	default:
		u.setError(fmt.Errorf("dynamo: Update.DeleteFromSet given unsupported value: %v (%T: %s)", value, value, avTypeName(v)))
		return u
	}

	return u.delete(path, v)
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

// RemoveExpr performs a custom remove expression, substituting the args into expr as in filter expressions.
//
//	RemoveExpr("MyList[$]", 5)
func (u *Update) RemoveExpr(expr string, args ...interface{}) *Update {
	expr, err := u.subExpr(expr, args...)
	u.setError(err)
	u.remove[expr] = struct{}{}
	return u
}

// If specifies a conditional expression for this update to succeed.
// Use single quotes to specificy reserved names inline (like 'Count').
// Use the placeholder ? within the expression to substitute values, and use $ for names.
// You need to use quoted or placeholder names when the name is a reserved word in DynamoDB.
// Multiple calls to Update will be combined with AND.
func (u *Update) If(expr string, args ...interface{}) *Update {
	cond, err := u.subExprN(expr, args...)
	u.setError(err)
	if u.condition != "" {
		u.condition += " AND "
	}
	u.condition += wrapExpr(cond)
	return u
}

// ConsumedCapacity will measure the throughput capacity consumed by this operation and add it to cc.
func (u *Update) ConsumedCapacity(cc *ConsumedCapacity) *Update {
	u.cc = cc
	return u
}

// Run executes this update.
func (u *Update) Run(ctx context.Context) error {
	u.returnType = types.ReturnValueNone
	_, err := u.run(ctx)
	return err
}

// Value executes this update, encoding out with the new value after the update.
// This is equivalent to ReturnValues = ALL_NEW in the DynamoDB API.
func (u *Update) Value(ctx context.Context, out interface{}) error {
	u.returnType = types.ReturnValueAllNew
	output, err := u.run(ctx)
	if err != nil {
		return err
	}
	return unmarshalItem(output.Attributes, out)
}

// OldValue executes this update, encoding out with the old value before the update.
// This is equivalent to ReturnValues = ALL_OLD in the DynamoDB API.
func (u *Update) OldValue(ctx context.Context, out interface{}) error {
	u.returnType = types.ReturnValueAllOld
	output, err := u.run(ctx)
	if err != nil {
		return err
	}
	return unmarshalItem(output.Attributes, out)
}

// OnlyUpdatedValue executes this update, encoding out with only with new values of the attributes that were changed.
// This is equivalent to ReturnValues = UPDATED_NEW in the DynamoDB API.
func (u *Update) OnlyUpdatedValue(ctx context.Context, out interface{}) error {
	u.returnType = types.ReturnValueUpdatedNew
	output, err := u.run(ctx)
	if err != nil {
		return err
	}
	return unmarshalItem(output.Attributes, out)
}

// OnlyUpdatedOldValue executes this update, encoding out with only with old values of the attributes that were changed.
// This is equivalent to ReturnValues = UPDATED_OLD in the DynamoDB API.
func (u *Update) OnlyUpdatedOldValue(ctx context.Context, out interface{}) error {
	u.returnType = types.ReturnValueUpdatedOld
	output, err := u.run(ctx)
	if err != nil {
		return err
	}
	return unmarshalItem(output.Attributes, out)
}

// CurrentValue executes this update.
// If successful, the return value `wrote` will be true, and the input item will be unmarshaled to `out`.
//
// If the update is unsuccessful because of a condition check failure, `wrote` will be false, the current value of the item will be unmarshaled to `out`, and `err` will be nil.
//
// If the update is unsuccessful for any other reason, `wrote` will be false and `err` will be non-nil.
//
// See also: [UnmarshalItemFromCondCheckFailed].
func (u *Update) CurrentValue(ctx context.Context, out interface{}) (wrote bool, err error) {
	u.returnType = types.ReturnValueAllNew
	u.onCondFail = types.ReturnValuesOnConditionCheckFailureAllOld
	output, err := u.run(ctx)
	if err != nil {
		if ok, err := UnmarshalItemFromCondCheckFailed(err, out); ok {
			return false, err
		}
		return false, err
	}
	return true, unmarshalItem(output.Attributes, out)
}

// IncludeAllItemsInCondCheckFail specifies whether an item update that fails its condition check should include the item itself in the error.
// Such items can be extracted using [UnmarshalItemFromCondCheckFailed] for single updates, or [UnmarshalItemsFromTxCondCheckFailed] for write transactions.
func (u *Update) IncludeItemInCondCheckFail(enabled bool) *Update {
	if enabled {
		u.onCondFail = types.ReturnValuesOnConditionCheckFailureAllOld
	} else {
		u.onCondFail = types.ReturnValuesOnConditionCheckFailureNone
	}
	return u
}

func (u *Update) run(ctx context.Context) (*dynamodb.UpdateItemOutput, error) {
	if u.err != nil {
		return nil, u.err
	}

	input := u.updateInput()
	var output *dynamodb.UpdateItemOutput
	err := u.table.db.retry(ctx, func() error {
		var err error
		output, err = u.table.db.client.UpdateItem(ctx, input)
		u.cc.incRequests()
		return err
	})
	if output != nil {
		u.cc.add(output.ConsumedCapacity)
	}
	return output, err
}

func (u *Update) updateInput() *dynamodb.UpdateItemInput {
	input := &dynamodb.UpdateItemInput{
		TableName:                 &u.table.name,
		Key:                       u.key(),
		UpdateExpression:          u.updateExpr(),
		ExpressionAttributeNames:  u.nameExpr,
		ExpressionAttributeValues: u.valueExpr,
		ReturnValues:              u.returnType,
	}
	if u.condition != "" {
		input.ConditionExpression = &u.condition
		input.ReturnValuesOnConditionCheckFailure = u.onCondFail
	}
	if u.cc != nil {
		input.ReturnConsumedCapacity = types.ReturnConsumedCapacityIndexes
	}
	return input
}

func (u *Update) writeTxItem() (*types.TransactWriteItem, error) {
	if u.err != nil {
		return nil, u.err
	}
	input := u.updateInput()
	item := &types.TransactWriteItem{
		Update: &types.Update{
			TableName:                           input.TableName,
			Key:                                 input.Key,
			UpdateExpression:                    input.UpdateExpression,
			ExpressionAttributeNames:            input.ExpressionAttributeNames,
			ExpressionAttributeValues:           input.ExpressionAttributeValues,
			ConditionExpression:                 input.ConditionExpression,
			ReturnValuesOnConditionCheckFailure: input.ReturnValuesOnConditionCheckFailure,
		},
	}
	return item, nil
}

func (u *Update) key() Item {
	key := Item{
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
	if u.err == nil {
		u.err = err
	}
}
