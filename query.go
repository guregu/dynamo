package dynamo

import (
	"errors"
	"log"

	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/gen/dynamodb"
)

type Query struct {
	table      Table
	hashKey    string
	hashValue  dynamodb.AttributeValue
	rangeKey   string
	rangeValue []dynamodb.AttributeValue
	rangeOp    string

	err error
}

type Operator aws.StringValue

var (
	// These are OK in key comparisons
	Equals         Operator = Operator(aws.String(dynamodb.ComparisonOperatorEq))
	LessOrEqual             = aws.String(dynamodb.ComparisonOperatorLe)
	Less                    = aws.String(dynamodb.ComparisonOperatorLt)
	GreaterOrEqual          = aws.String(dynamodb.ComparisonOperatorGe)
	Greater                 = aws.String(dynamodb.ComparisonOperatorGt)
	BeginsWith              = aws.String(dynamodb.ComparisonOperatorBeginsWith)
	Between                 = aws.String(dynamodb.ComparisonOperatorBetween)

	// These can't be used in key comparions
	IsNull      Operator = Operator(aws.String(dynamodb.ComparisonOperatorNull))
	NotNull              = aws.String(dynamodb.ComparisonOperatorNotNull)
	Contains             = aws.String(dynamodb.ComparisonOperatorContains)
	NotContains          = aws.String(dynamodb.ComparisonOperatorNotContains)
	In                   = aws.String(dynamodb.ComparisonOperatorIn)
)

func (table Table) Get(key string, value interface{}) *Query {
	q := &Query{
		table:   table,
		hashKey: key,
	}
	q.hashValue, q.err = marshal(value)
	return q
}

func (q *Query) One(out interface{}) error {
	if q.err != nil {
		return q.err
	}

	// TODO: do GetItem
	return nil
}

func (q *Query) All(out interface{}) error {
	if q.err != nil {
		return q.err
	}

	req := &dynamodb.QueryInput{
		TableName:     aws.String(q.table.Name),
		KeyConditions: q.keyConditions(),
		Select:        aws.String(dynamodb.SelectAllAttributes),
	}

	res, err := q.table.db.client.Query(req)
	if err != nil {
		return err
	}

	unmarshalAll(res.Items, out)
	for _, item := range res.Items {
		for k, v := range item {
			log.Println("got", k, v)
		}
	}

	return nil
}

func (q *Query) Count() (int, error) {
	if q.err != nil {
		return 0, q.err
	}

	req := &dynamodb.QueryInput{
		TableName:     aws.String(q.table.Name),
		KeyConditions: q.keyConditions(),
		Select:        aws.String(dynamodb.SelectCount),
	}

	res, err := q.table.db.client.Query(req)
	if err != nil {
		return 0, err
	}
	if res.Count == nil {
		return 0, errors.New("nil count")
	}

	return int(*res.Count), nil
}

func (q *Query) keyConditions() map[string]dynamodb.Condition {
	conds := map[string]dynamodb.Condition{
		q.hashKey: dynamodb.Condition{
			AttributeValueList: []dynamodb.AttributeValue{q.hashValue},
			ComparisonOperator: aws.StringValue(Equals),
		},
	}
	if q.rangeKey != "" && q.rangeOp != "" {
		conds[q.rangeKey] = dynamodb.Condition{
			AttributeValueList: q.rangeValue,
			ComparisonOperator: &q.rangeOp,
		}
	}
	return conds
}

func (q *Query) setError(err error) {
	if err != nil {
		q.err = err
	}
}
