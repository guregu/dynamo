package dynamo

import (
	"errors"
	"strings"

	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/service/dynamodb"
	// "github.com/davecgh/go-spew/spew"
)

type Query struct {
	table    Table
	startKey *map[string]*dynamodb.AttributeValue

	hashKey   string
	hashValue *dynamodb.AttributeValue

	rangeKey    string
	rangeValues []*dynamodb.AttributeValue
	rangeOp     Operator

	projection string
	consistent bool
	limit      int64

	err error
}

var ErrNotFound = errors.New("dynamo: no record found")

type Operator *string

var (
	// These are OK in key comparisons
	Equals         Operator = Operator(aws.String("EQ"))
	NotEquals               = Operator(aws.String("NE"))
	LessOrEqual             = Operator(aws.String("LE"))
	Less                    = Operator(aws.String("LT"))
	GreaterOrEqual          = Operator(aws.String("GE"))
	Greater                 = Operator(aws.String("GT"))
	BeginsWith              = Operator(aws.String("BEGINS_WITH"))
	Between                 = Operator(aws.String("BETWEEN"))
	// These can't be used in key comparions
	IsNull      Operator = Operator(aws.String("NULL"))
	NotNull              = Operator(aws.String("NOT_NULL"))
	Contains             = Operator(aws.String("CONTAINS"))
	NotContains          = Operator(aws.String("NOT_CONTAINS"))
	In                   = Operator(aws.String("IN"))
)

var (
	selectAllAttributes      = aws.String("ALL_ATTRIBUTES")
	selectCount              = aws.String("COUNT")
	selectSpecificAttributes = aws.String("SPECIFIC_ATTRIBUTES")
)

func (table Table) Get(key string, value interface{}) *Query {
	q := &Query{
		table:   table,
		hashKey: key,
	}
	q.hashValue, q.err = marshal(value)
	return q
}

func (q *Query) Range(key string, op Operator, values ...interface{}) *Query {
	var err error
	q.rangeKey = key
	q.rangeOp = op
	q.rangeValues, err = marshalSlice(values)
	q.setError(err)
	return q
}

func (q *Query) Project(expr ...string) *Query {
	q.projection = strings.Join(expr, ", ")
	return q
}

func (q *Query) Consistent(on bool) *Query {
	q.consistent = on
	return q
}

func (q *Query) Limit(limit int64) *Query {
	q.limit = limit
	return q
}

func (q *Query) One(out interface{}) error {
	if q.err != nil {
		return q.err
	}

	if q.rangeOp != nil && q.rangeOp != Equals {
		// do a query and return the first result
		return errors.New("not implemented")
	}

	// otherwise use GetItem
	req := q.getItemInput()

	res, err := q.table.db.client.GetItem(req)
	if err != nil {
		return err
	}

	if res.Item == nil {
		return ErrNotFound
	}

	return unmarshalItem(res.Item, out)
}

func (q *Query) All(out interface{}) error {
	if q.err != nil {
		return q.err
	}

	// TODO: make this smarter by appending to the result array
	var items []*map[string]*dynamodb.AttributeValue
	for {
		req := q.queryInput()

		res, err := q.table.db.client.Query(req)
		if err != nil {
			return err
		}

		if items == nil {
			items = res.Items
		} else {
			items = append(items, res.Items...)
		}

		// do we need to check for more results?
		// TODO: Query.Next() or something to continue manually
		q.startKey = res.LastEvaluatedKey
		if res.LastEvaluatedKey == nil || q.limit > 0 {
			break
		}
	}

	return unmarshalAll(items, out)
}

func (q *Query) Count() (int64, error) {
	if q.err != nil {
		return 0, q.err
	}

	var count int64
	for {
		req := q.queryInput()
		req.Select = selectCount

		res, err := q.table.db.client.Query(req)
		if err != nil {
			return 0, err
		}
		if res.Count == nil {
			return 0, errors.New("nil count")
		}
		count += *res.Count

		q.startKey = res.LastEvaluatedKey
		if res.LastEvaluatedKey == nil || q.limit > 0 {
			break
		}
	}

	return count, nil
}

func (q *Query) queryInput() *dynamodb.QueryInput {
	req := &dynamodb.QueryInput{
		TableName:         aws.String(q.table.Name),
		KeyConditions:     q.keyConditions(),
		ExclusiveStartKey: q.startKey,
	}
	if q.consistent {
		req.ConsistentRead = aws.Boolean(q.consistent)
	}
	if q.limit > 0 {
		req.Limit = aws.Long(q.limit)
	}
	if q.projection != "" {
		req.ProjectionExpression = &q.projection
	}
	return req
}

func (q *Query) keyConditions() *map[string]*dynamodb.Condition {
	conds := map[string]*dynamodb.Condition{
		q.hashKey: &dynamodb.Condition{
			AttributeValueList: []*dynamodb.AttributeValue{q.hashValue},
			ComparisonOperator: Equals,
		},
	}
	if q.rangeKey != "" && q.rangeOp != nil {
		conds[q.rangeKey] = &dynamodb.Condition{
			AttributeValueList: q.rangeValues,
			ComparisonOperator: q.rangeOp,
		}
	}
	return &conds
}

func (q *Query) getItemInput() *dynamodb.GetItemInput {
	req := &dynamodb.GetItemInput{
		TableName: aws.String(q.table.Name),
		Key:       q.keys(),
	}
	if q.consistent {
		req.ConsistentRead = aws.Boolean(q.consistent)
	}
	if q.projection != "" {
		req.ProjectionExpression = aws.String(q.projection)
	}
	return req
}

func (q *Query) keys() *map[string]*dynamodb.AttributeValue {
	keys := map[string]*dynamodb.AttributeValue{
		q.hashKey: q.hashValue,
	}
	if q.rangeKey != "" && len(q.rangeValues) > 0 {
		keys[q.rangeKey] = q.rangeValues[0]
	}
	return &keys
}

func (q *Query) setError(err error) {
	if err != nil {
		q.err = err
	}
}
