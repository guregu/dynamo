package dynamo

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Query is a request to get one or more items in a table.
// Query uses the DynamoDB query for requests for multiple items, and GetItem for one.
// See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html
// and http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html
type Query struct {
	table    Table
	startKey Item
	index    string

	hashKey   string
	hashValue types.AttributeValue

	rangeKey    string
	rangeValues []types.AttributeValue
	rangeOp     Operator

	projection  string
	filters     []string
	consistent  bool
	limit       int
	searchLimit int32
	reqLimit    int
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

var selectCount types.Select = "COUNT"

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
func (q *Query) Limit(limit int) *Query {
	q.limit = limit
	return q
}

// SearchLimit specifies the maximum amount of results to examine.
// If a filter is not specified, the number of results will be limited.
// If a filter is specified, the number of results to consider for filtering will be limited.
// SearchLimit > 0 implies RequestLimit(1).
// Note: limit will be capped to MaxInt32 as that is the maximum number the DynamoDB API will accept.
func (q *Query) SearchLimit(limit int) *Query {
	q.searchLimit = int32(min(limit, math.MaxInt32))
	return q
}

// RequestLimit specifies the maximum amount of requests to make against DynamoDB's API.
// A limit of zero or less means unlimited requests.
func (q *Query) RequestLimit(limit int) *Query {
	q.reqLimit = limit
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
func (q *Query) One(ctx context.Context, out interface{}) error {
	if q.err != nil {
		return q.err
	}

	// Can we use the GetItem API?
	if q.canGetItem() {
		req := q.getItemInput()

		var res *dynamodb.GetItemOutput
		err := q.table.db.retry(ctx, func() error {
			var err error
			res, err = q.table.db.client.GetItem(ctx, req)
			q.cc.incRequests()
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
		q.cc.add(res.ConsumedCapacity)

		return unmarshalItem(res.Item, out)
	}

	// If not, try a Query.
	req := q.queryInput()

	var res *dynamodb.QueryOutput
	err := q.table.db.retry(ctx, func() error {
		var err error
		res, err = q.table.db.client.Query(ctx, req)
		q.cc.incRequests()
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
	q.cc.add(res.ConsumedCapacity)

	return unmarshalItem(res.Items[0], out)
}

// Count executes this request, returning the number of results.
func (q *Query) Count(ctx context.Context) (int, error) {
	if q.err != nil {
		return 0, q.err
	}

	var count int
	var scanned int32
	var reqs int
	var res *dynamodb.QueryOutput
	for {
		input := q.queryInput()
		input.Select = selectCount

		err := q.table.db.retry(ctx, func() error {
			var err error
			res, err = q.table.db.client.Query(ctx, input)
			q.cc.incRequests()
			if err != nil {
				return err
			}
			reqs++

			count += int(res.Count)
			scanned += res.ScannedCount

			return nil
		})
		if err != nil {
			return 0, err
		}
		q.cc.add(res.ConsumedCapacity)

		q.startKey = res.LastEvaluatedKey
		if res.LastEvaluatedKey == nil ||
			(q.limit > 0 && count >= q.limit) ||
			(q.searchLimit > 0 && scanned >= q.searchLimit) ||
			(q.reqLimit > 0 && reqs >= q.reqLimit) {
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
	n      int
	reqs   int

	// last item evaluated
	last Item
	// cache of primary keys, used for generating LEKs
	keys map[string]struct{}
	// example LastEvaluatedKey and ExclusiveStartKey, used to lazily evaluate the primary keys if possible
	exLEK  Item
	exESK  Item
	keyErr error

	unmarshal unmarshalFunc
}

// Next tries to unmarshal the next result into out.
// Returns false when it is complete or if it runs into an error.
func (itr *queryIter) Next(ctx context.Context, out interface{}) bool {
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
		// have we hit the request limit?
		if itr.query.reqLimit > 0 && itr.reqs == itr.query.reqLimit {
			return false
		}

		// no, prepare next request and reset index
		itr.input.ExclusiveStartKey = itr.output.LastEvaluatedKey
		itr.idx = 0
	}

	itr.err = itr.query.table.db.retry(ctx, func() error {
		var err error
		itr.output, err = itr.query.table.db.client.Query(ctx, itr.input)
		itr.query.cc.incRequests()
		return err
	})

	if itr.err != nil {
		return false
	}
	itr.query.cc.add(itr.output.ConsumedCapacity)
	if len(itr.output.LastEvaluatedKey) > len(itr.exLEK) {
		itr.exLEK = itr.output.LastEvaluatedKey
	}
	itr.reqs++

	if len(itr.output.Items) == 0 {
		if itr.query.reqLimit > 0 && itr.reqs == itr.query.reqLimit {
			return false
		}
		if itr.output.LastEvaluatedKey != nil {
			// we need to retry until we get some data
			return itr.Next(ctx, out)
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

func (itr *queryIter) LastEvaluatedKey(ctx context.Context) (PagingKey, error) {
	if itr.output != nil {
		// if we've hit the end of our results, we can use the real LEK
		if itr.idx == len(itr.output.Items) {
			return itr.output.LastEvaluatedKey, nil
		}

		// figure out the primary keys if needed
		if itr.keys == nil && itr.keyErr == nil {
			itr.keys, itr.keyErr = itr.query.table.primaryKeys(ctx, itr.exLEK, itr.exESK, itr.query.index)
		}
		if itr.keyErr != nil {
			// primaryKeys can fail if the credentials lack DescribeTable permissions
			// in order to preserve backwards compatibility, we fall back to the old behavior and warn
			// see: https://github.com/guregu/dynamo/pull/187#issuecomment-1045183901
			return itr.output.LastEvaluatedKey, fmt.Errorf("dynamo: failed to determine LastEvaluatedKey in query: %w", itr.keyErr)
		}

		// we can't use the real LEK, so we need to infer the LEK from the last item we saw
		lek, err := lekify(itr.last, itr.keys)
		if err != nil {
			return itr.output.LastEvaluatedKey, fmt.Errorf("dynamo: failed to infer LastEvaluatedKey in query: %w", err)
		}
		return lek, nil
	}
	return nil, nil
}

// All executes this request and unmarshals all results to out, which must be a pointer to a slice.
func (q *Query) All(ctx context.Context, out interface{}) error {
	iter := &queryIter{
		query:     q,
		unmarshal: unmarshalAppendTo(out),
		err:       q.err,
	}
	for iter.Next(ctx, out) {
	}
	return iter.Err()
}

// AllWithLastEvaluatedKey executes this request and unmarshals all results to out, which must be a pointer to a slice.
// This returns a PagingKey you can use with StartFrom to split up results.
func (q *Query) AllWithLastEvaluatedKey(ctx context.Context, out interface{}) (PagingKey, error) {
	iter := &queryIter{
		query:     q,
		unmarshal: unmarshalAppendTo(out),
		err:       q.err,
	}
	for iter.Next(ctx, out) {
	}
	lek, err := iter.LastEvaluatedKey(ctx)
	return lek, errors.Join(iter.Err(), err)
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
			limit := int32(min(math.MaxInt32, q.limit))
			req.Limit = &limit
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
		req.ReturnConsumedCapacity = types.ReturnConsumedCapacityIndexes
	}
	return req
}

func (q *Query) keyConditions() map[string]types.Condition {
	conds := map[string]types.Condition{
		q.hashKey: {
			AttributeValueList: []types.AttributeValue{q.hashValue},
			ComparisonOperator: types.ComparisonOperatorEq,
		},
	}
	if q.rangeKey != "" && q.rangeOp != "" {
		conds[q.rangeKey] = types.Condition{
			AttributeValueList: q.rangeValues,
			ComparisonOperator: types.ComparisonOperator(q.rangeOp),
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
		req.ReturnConsumedCapacity = types.ReturnConsumedCapacityIndexes
	}
	return req
}

func (q *Query) getTxItem() (types.TransactGetItem, error) {
	if !q.canGetItem() {
		return types.TransactGetItem{}, errors.New("dynamo: transaction Query is too complex; no indexes or filters are allowed")
	}
	input := q.getItemInput()
	return types.TransactGetItem{
		Get: &types.Get{
			TableName:                input.TableName,
			Key:                      input.Key,
			ExpressionAttributeNames: input.ExpressionAttributeNames,
			ProjectionExpression:     input.ProjectionExpression,
		},
	}, nil
}

func (q *Query) keys() Item {
	keys := Item{
		q.hashKey: q.hashValue,
	}
	if q.rangeKey != "" && len(q.rangeValues) > 0 {
		keys[q.rangeKey] = q.rangeValues[0]
	}
	return keys
}

func (q *Query) keysAndAttribs() types.KeysAndAttributes {
	kas := types.KeysAndAttributes{
		Keys:                     []Item{q.keys()},
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
