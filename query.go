package dynamo

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Query is a request to get one or more items in a table.
// Query uses the DynamoDB query for requests for multiple items, and GetItem for one.
// See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html
// and http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html
type Query struct {
	table    Table
	startKey map[string]*dynamodb.AttributeValue
	index    string

	hashKey   string
	hashValue *dynamodb.AttributeValue

	rangeKey    string
	rangeValues []*dynamodb.AttributeValue
	rangeOp     Operator

	projection  string
	filters     []string
	consistent  bool
	limit       int64
	searchLimit int64
	order       *Order

	subber

	err error
	cc  *ConsumedCapacity
}

var (
	// ErrNotFound is returned when no items could be found in Get or OldValue and similar operations.
	ErrNotFound = errors.New("dynamo: no item found")
	// ErrTooMany is returned when one item was requested, but the query returned multiple items.
	ErrTooMany = errors.New("dynamo: too many items")
)

// Operator is an operation to apply in key comparisons.
type Operator string

// Operators used for comparing against the range key in queries.
const (
	Equal          Operator = "EQ"
	NotEqual       Operator = "NE"
	Less           Operator = "LT"
	LessOrEqual    Operator = "LE"
	Greater        Operator = "GT"
	GreaterOrEqual Operator = "GE"
	BeginsWith     Operator = "BEGINS_WITH"
	Between        Operator = "BETWEEN"
)

// Order is used for specifying the order of results.
type Order bool

// Orders for sorting results.
const (
	Ascending  Order = true  // ScanIndexForward = true
	Descending       = false // ScanIndexForward = false
)

var selectCount = aws.String("COUNT")

// Get creates a new request to get an item.
// Name is the name of the hash key (a.k.a. partition key).
// Value is the value of the hash key.
func (table Table) Get(name string, value interface{}) *Query {
	q := &Query{
		table:   table,
		hashKey: name,
	}
	q.hashValue, q.err = marshal(value, flagNone)
	if q.hashValue == nil {
		q.setError(fmt.Errorf("dynamo: query hash key value is nil or omitted for attribute %q", q.hashKey))
	}
	return q
}

// Range specifies the range key (a.k.a. sort key) or keys to get.
// For single item requests using One, op must be Equal.
// Name is the name of the range key.
// Op specifies the operator to use when comparing values.
func (q *Query) Range(name string, op Operator, values ...interface{}) *Query {
	var err error
	q.rangeKey = name
	q.rangeOp = op
	q.rangeValues, err = marshalSliceNoOmit(values)
	q.setError(err)
	for i, v := range q.rangeValues {
		if v == nil {
			q.setError(fmt.Errorf("dynamo: query range key value is nil or omitted for attribute %q (range key #%d of %d)", q.rangeKey, i+1, len(q.rangeValues)))
			break
		}
	}
	if len(q.rangeValues) == 0 {
		q.setError(fmt.Errorf("dynamo: query range key values are missing for attribute %q", q.rangeKey))
	}
	return q
}

// StartFrom makes this query continue from a previous one.
// Use Query.Iter's LastEvaluatedKey.
func (q *Query) StartFrom(key PagingKey) *Query {
	q.startKey = key
	return q
}

// Index specifies the name of the index that this query will operate on.
func (q *Query) Index(name string) *Query {
	q.index = name
	return q
}

// Project limits the result attributes to the given paths.
func (q *Query) Project(paths ...string) *Query {
	var expr string
	for i, p := range paths {
		if i != 0 {
			expr += ", "
		}
		name, err := q.escape(p)
		q.setError(err)
		expr += name
	}
	q.projection = expr
	return q
}

// ProjectExpr limits the result attributes to the given expression.
// Use single quotes to specificy reserved names inline (like 'Count').
// Use the placeholder ? within the expression to substitute values, and use $ for names.
// You need to use quoted or placeholder names when the name is a reserved word in DynamoDB.
func (q *Query) ProjectExpr(expr string, args ...interface{}) *Query {
	expr, err := q.subExpr(expr, args...)
	q.setError(err)
	q.projection = expr
	return q
}

// Filter takes an expression that all results will be evaluated against.
// Use single quotes to specificy reserved names inline (like 'Count').
// Use the placeholder ? within the expression to substitute values, and use $ for names.
// You need to use quoted or placeholder names when the name is a reserved word in DynamoDB.
// Multiple calls to Filter will be combined with AND.
func (q *Query) Filter(expr string, args ...interface{}) *Query {
	expr, err := q.subExprN(expr, args...)
	q.setError(err)
	q.filters = append(q.filters, wrapExpr(expr))
	return q
}

// Consistent will, if on is true, make this query a strongly consistent read.
// Queries are eventually consistent by default.
// Strongly consistent reads are more resource-heavy than eventually consistent reads.
func (q *Query) Consistent(on bool) *Query {
	q.consistent = on
	return q
}

// Limit specifies the maximum amount of results to return.
func (q *Query) Limit(limit int64) *Query {
	q.limit = limit
	return q
}

// SearchLimit specifies the maximum amount of results to examine.
// If a filter is not specified, the number of results will be limited.
// If a filter is specified, the number of results to consider for filtering will be limited.
func (q *Query) SearchLimit(limit int64) *Query {
	q.searchLimit = limit
	return q
}

// Order specifies the desired result order.
// Requires a range key (a.k.a. partition key) to be specified.
func (q *Query) Order(order Order) *Query {
	q.order = &order
	return q
}

// ConsumedCapacity will measure the throughput capacity consumed by this operation and add it to cc.
func (q *Query) ConsumedCapacity(cc *ConsumedCapacity) *Query {
	q.cc = cc
	return q
}

// One executes this query and retrieves a single result,
// unmarshaling the result to out.
func (q *Query) One(out interface{}) error {
	ctx, cancel := defaultContext()
	defer cancel()
	return q.OneWithContext(ctx, out)
}

func (q *Query) OneWithContext(ctx context.Context, out interface{}) error {
	if q.err != nil {
		return q.err
	}

	// Can we use the GetItem API?
	if q.canGetItem() {
		req := q.getItemInput()

		var res *dynamodb.GetItemOutput
		err := q.table.db.retry(ctx, func() error {
			var err error
			res, err = q.table.db.client.GetItemWithContext(ctx, req)
			if err != nil {
				return err
			}
			if res.Item == nil {
				return ErrNotFound
			}
			return nil
		})
		if err != nil {
			return err
		}
		if q.cc != nil {
			addConsumedCapacity(q.cc, res.ConsumedCapacity)
		}

		return unmarshalItem(res.Item, out)
	}

	// If not, try a Query.
	req := q.queryInput()

	var res *dynamodb.QueryOutput
	err := q.table.db.retry(ctx, func() error {
		var err error
		res, err = q.table.db.client.QueryWithContext(ctx, req)
		if err != nil {
			return err
		}

		switch {
		case len(res.Items) == 0:
			return ErrNotFound
		case len(res.Items) > 1 && q.limit != 1:
			return ErrTooMany
		case res.LastEvaluatedKey != nil && q.searchLimit != 0:
			return ErrTooMany
		}

		return nil
	})
	if err != nil {
		return err
	}
	if q.cc != nil {
		addConsumedCapacity(q.cc, res.ConsumedCapacity)
	}

	return unmarshalItem(res.Items[0], out)
}

// Count executes this request, returning the number of results.
func (q *Query) Count() (int64, error) {
	ctx, cancel := defaultContext()
	defer cancel()
	return q.CountWithContext(ctx)
}

func (q *Query) CountWithContext(ctx context.Context) (int64, error) {
	if q.err != nil {
		return 0, q.err
	}

	var count int64
	var res *dynamodb.QueryOutput
	for {
		req := q.queryInput()
		req.Select = selectCount

		err := q.table.db.retry(ctx, func() error {
			var err error
			res, err = q.table.db.client.QueryWithContext(ctx, req)
			if err != nil {
				return err
			}
			if res.Count == nil {
				return errors.New("nil count")
			}
			count += *res.Count
			return nil
		})
		if err != nil {
			return 0, err
		}
		if q.cc != nil {
			addConsumedCapacity(q.cc, res.ConsumedCapacity)
		}

		q.startKey = res.LastEvaluatedKey
		if res.LastEvaluatedKey == nil || q.searchLimit > 0 {
			break
		}
	}

	return count, nil
}

// queryIter is the iterator for Query operations
type queryIter struct {
	query  *Query
	input  *dynamodb.QueryInput
	output *dynamodb.QueryOutput
	err    error
	idx    int
	n      int64

	// last item evaluated
	last map[string]*dynamodb.AttributeValue
	// cache of primary keys, used for generating LEKs
	keys map[string]struct{}
	// example LastEvaluatedKey and ExclusiveStartKey, used to lazily evaluate the primary keys if possible
	exLEK  map[string]*dynamodb.AttributeValue
	exESK  map[string]*dynamodb.AttributeValue
	keyErr error

	unmarshal unmarshalFunc
}

// Next tries to unmarshal the next result into out.
// Returns false when it is complete or if it runs into an error.
func (itr *queryIter) Next(out interface{}) bool {
	ctx, cancel := defaultContext()
	defer cancel()
	return itr.NextWithContext(ctx, out)
}

func (itr *queryIter) NextWithContext(ctx context.Context, out interface{}) bool {
	// stop if we have an error
	if ctx.Err() != nil {
		itr.err = ctx.Err()
	}
	if itr.err != nil {
		return false
	}

	// stop if exceed limit
	if itr.query.limit > 0 && itr.n == itr.query.limit {
		// proactively grab the keys for LEK inferral, but don't count it as a real error yet to keep backwards compat
		itr.keys, itr.keyErr = itr.query.table.primaryKeys(ctx, itr.exLEK, itr.exESK, itr.query.index)
		return false
	}

	// can we use results we already have?
	if itr.output != nil && itr.idx < len(itr.output.Items) {
		item := itr.output.Items[itr.idx]
		itr.last = item
		itr.err = itr.unmarshal(item, out)
		itr.idx++
		itr.n++
		return itr.err == nil
	}

	// new query
	if itr.input == nil {
		itr.input = itr.query.queryInput()
	}
	if len(itr.input.ExclusiveStartKey) > len(itr.exESK) {
		itr.exESK = itr.input.ExclusiveStartKey
	}
	if itr.output != nil && itr.idx >= len(itr.output.Items) {
		// have we exhausted all results?
		if itr.output.LastEvaluatedKey == nil || itr.query.searchLimit > 0 {
			return false
		}

		// no, prepare next request and reset index
		itr.input.ExclusiveStartKey = itr.output.LastEvaluatedKey
		itr.idx = 0
	}

	itr.err = itr.query.table.db.retry(ctx, func() error {
		var err error
		itr.output, err = itr.query.table.db.client.QueryWithContext(ctx, itr.input)
		return err
	})

	if itr.err != nil {
		return false
	}
	if itr.query.cc != nil {
		addConsumedCapacity(itr.query.cc, itr.output.ConsumedCapacity)
	}
	if len(itr.output.LastEvaluatedKey) > len(itr.exLEK) {
		itr.exLEK = itr.output.LastEvaluatedKey
	}

	if len(itr.output.Items) == 0 {
		if itr.output.LastEvaluatedKey != nil {
			// we need to retry until we get some data
			return itr.NextWithContext(ctx, out)
		}
		// we're done
		return false
	}

	item := itr.output.Items[itr.idx]
	itr.last = item
	itr.err = itr.unmarshal(item, out)
	itr.idx++
	itr.n++
	return itr.err == nil
}

// Err returns the error encountered, if any.
// You should check this after Next is finished.
func (itr *queryIter) Err() error {
	return itr.err
}

func (itr *queryIter) LastEvaluatedKey() PagingKey {
	if itr.output != nil {
		// if we've hit the end of our results, we can use the real LEK
		if itr.idx == len(itr.output.Items) {
			return itr.output.LastEvaluatedKey
		}

		// figure out the primary keys if needed
		if itr.keys == nil && itr.keyErr == nil {
			ctx, _ := defaultContext() // TODO(v2): take context instead of using the default
			itr.keys, itr.keyErr = itr.query.table.primaryKeys(ctx, itr.exLEK, itr.exESK, itr.query.index)
		}
		if itr.keyErr != nil {
			// primaryKeys can fail if the credentials lack DescribeTable permissions
			// in order to preserve backwards compatibility, we fall back to the old behavior and warn
			// see: https://github.com/guregu/dynamo/pull/187#issuecomment-1045183901
			// TODO(v2): rejigger this API.
			itr.query.table.db.log("dynamo: Warning:", itr.keyErr, "Returning a later LastEvaluatedKey.")
			return itr.output.LastEvaluatedKey
		}

		// we can't use the real LEK, so we need to infer the LEK from the last item we saw
		lek, err := lekify(itr.last, itr.keys)
		// unfortunately, this API can't return an error so a warning is the best we can do...
		// this matches old behavior before the LEK was automatically generated
		// TODO(v2): fix this.
		if err != nil {
			itr.query.table.db.log("dynamo: Warning:", err, "Returning a later LastEvaluatedKey.")
			return itr.output.LastEvaluatedKey
		}
		return lek
	}
	return nil
}

// All executes this request and unmarshals all results to out, which must be a pointer to a slice.
func (q *Query) All(out interface{}) error {
	ctx, cancel := defaultContext()
	defer cancel()
	return q.AllWithContext(ctx, out)
}

func (q *Query) AllWithContext(ctx context.Context, out interface{}) error {
	iter := &queryIter{
		query:     q,
		unmarshal: unmarshalAppend,
		err:       q.err,
	}
	for iter.NextWithContext(ctx, out) {
	}
	return iter.Err()
}

// AllWithLastEvaluatedKey executes this request and unmarshals all results to out, which must be a pointer to a slice.
// This returns a PagingKey you can use with StartFrom to split up results.
func (q *Query) AllWithLastEvaluatedKey(out interface{}) (PagingKey, error) {
	ctx, cancel := defaultContext()
	defer cancel()
	return q.AllWithLastEvaluatedKeyContext(ctx, out)
}

func (q *Query) AllWithLastEvaluatedKeyContext(ctx context.Context, out interface{}) (PagingKey, error) {
	iter := &queryIter{
		query:     q,
		unmarshal: unmarshalAppend,
		err:       q.err,
	}
	for iter.NextWithContext(ctx, out) {
	}
	return iter.LastEvaluatedKey(), iter.Err()
}

// Iter returns a results iterator for this request.
func (q *Query) Iter() PagingIter {
	iter := &queryIter{
		query:     q,
		unmarshal: unmarshalItem,
		err:       q.err,
	}

	return iter
}

// can we use the get item API?
func (q *Query) canGetItem() bool {
	switch {
	case q.rangeOp != "" && q.rangeOp != Equal:
		return false
	case q.index != "":
		return false
	case len(q.filters) > 0:
		return false
	case q.limit > 0:
		return false
	}
	return true
}

func (q *Query) queryInput() *dynamodb.QueryInput {
	req := &dynamodb.QueryInput{
		TableName:                 &q.table.name,
		KeyConditions:             q.keyConditions(),
		ExclusiveStartKey:         q.startKey,
		ExpressionAttributeNames:  q.nameExpr,
		ExpressionAttributeValues: q.valueExpr,
	}
	if q.consistent {
		req.ConsistentRead = &q.consistent
	}
	if q.limit > 0 {
		if len(q.filters) == 0 {
			req.Limit = &q.limit
		}
	}
	if q.searchLimit > 0 {
		req.Limit = &q.searchLimit
	}
	if q.projection != "" {
		req.ProjectionExpression = &q.projection
	}
	if len(q.filters) > 0 {
		filter := strings.Join(q.filters, " AND ")
		req.FilterExpression = &filter
	}
	if q.index != "" {
		req.IndexName = &q.index
	}
	if q.order != nil {
		req.ScanIndexForward = (*bool)(q.order)
	}
	if q.cc != nil {
		req.ReturnConsumedCapacity = aws.String(dynamodb.ReturnConsumedCapacityIndexes)
	}
	return req
}

func (q *Query) keyConditions() map[string]*dynamodb.Condition {
	conds := map[string]*dynamodb.Condition{
		q.hashKey: {
			AttributeValueList: []*dynamodb.AttributeValue{q.hashValue},
			ComparisonOperator: aws.String(string(Equal)),
		},
	}
	if q.rangeKey != "" && q.rangeOp != "" {
		conds[q.rangeKey] = &dynamodb.Condition{
			AttributeValueList: q.rangeValues,
			ComparisonOperator: aws.String(string(q.rangeOp)),
		}
	}
	return conds
}

func (q *Query) getItemInput() *dynamodb.GetItemInput {
	req := &dynamodb.GetItemInput{
		TableName:                &q.table.name,
		Key:                      q.keys(),
		ExpressionAttributeNames: q.nameExpr,
	}
	if q.consistent {
		req.ConsistentRead = &q.consistent
	}
	if q.projection != "" {
		req.ProjectionExpression = &q.projection
	}
	if q.cc != nil {
		req.ReturnConsumedCapacity = aws.String(dynamodb.ReturnConsumedCapacityIndexes)
	}
	return req
}

func (q *Query) getTxItem() (*dynamodb.TransactGetItem, error) {
	if !q.canGetItem() {
		return nil, errors.New("dynamo: transaction Query is too complex; no indexes or filters are allowed")
	}
	input := q.getItemInput()
	return &dynamodb.TransactGetItem{
		Get: &dynamodb.Get{
			TableName:                input.TableName,
			Key:                      input.Key,
			ExpressionAttributeNames: input.ExpressionAttributeNames,
			ProjectionExpression:     input.ProjectionExpression,
		},
	}, nil
}

func (q *Query) keys() map[string]*dynamodb.AttributeValue {
	keys := map[string]*dynamodb.AttributeValue{
		q.hashKey: q.hashValue,
	}
	if q.rangeKey != "" && len(q.rangeValues) > 0 {
		keys[q.rangeKey] = q.rangeValues[0]
	}
	return keys
}

func (q *Query) keysAndAttribs() *dynamodb.KeysAndAttributes {
	kas := &dynamodb.KeysAndAttributes{
		Keys:                     []map[string]*dynamodb.AttributeValue{q.keys()},
		ExpressionAttributeNames: q.nameExpr,
		ConsistentRead:           &q.consistent,
	}
	if q.projection != "" {
		kas.ProjectionExpression = &q.projection
	}
	return kas
}

func (q *Query) setError(err error) {
	if q.err == nil {
		q.err = err
	}
}
