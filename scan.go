package dynamo

import (
	"fmt"
	"strings"

	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/service/dynamodb"
)

type Scan struct {
	table    Table
	startKey *map[string]*dynamodb.AttributeValue
	index    string

	projection string
	filter     string // TODO
	limit      int64
	segments   int // TODO
}

func (table Table) Scan() *Scan {
	return &Scan{
		table: table,
	}
}

func (s *Scan) Index(name string) *Scan {
	s.index = name
	return s
}

func (q *Scan) Project(expr ...string) *Scan {
	q.projection = strings.Join(expr, ", ")
	return q
}

func (q *Scan) Filter(expr string) *Scan {
	q.filter = expr
	return q
}

func (q *Scan) Iter() Iter {
	return &scanIter{
		scan:      q,
		unmarshal: unmarshalItem,
	}
}

func (q *Scan) All(out interface{}) error {
	itr := &scanIter{
		scan:      q,
		unmarshal: unmarshalAppend,
	}
	for itr.Next(out) {

	}
	return itr.Err()
}

func (q *Scan) scanInput() *dynamodb.ScanInput {
	input := &dynamodb.ScanInput{
		ExclusiveStartKey: q.startKey,
		TableName:         aws.String(q.table.Name),
	}
	if q.index != "" {
		input.IndexName = aws.String(q.index)
	}
	if q.projection != "" {
		input.ProjectionExpression = aws.String(q.projection)
	}
	if q.filter != "" {
		input.FilterExpression = aws.String(q.filter)
	}
	if q.limit > 0 {
		input.Limit = &q.limit
	}
	return input
}

// scanIter is the iterator for Scan operations
type scanIter struct {
	scan   *Scan
	input  *dynamodb.ScanInput
	output *dynamodb.ScanOutput
	err    error
	idx    int

	unmarshal func(*map[string]*dynamodb.AttributeValue, interface{}) error
}

func (itr *scanIter) Next(out interface{}) bool {
	// stop if we have an error
	if itr.err != nil {
		return false
	}

	// can we use results we already have?
	if itr.output != nil && itr.idx < len(itr.output.Items) {
		item := itr.output.Items[itr.idx]
		itr.err = itr.unmarshal(item, out)
		itr.idx++
		return itr.err == nil
	}

	// new scan
	if itr.input == nil {
		itr.input = itr.scan.scanInput()
	}
	if itr.output != nil && itr.idx >= len(itr.output.Items) {
		// have we exhausted all results?
		if itr.output.LastEvaluatedKey == nil {
			return false
		}

		// no, prepare next request and reset index
		itr.input.ExclusiveStartKey = itr.output.LastEvaluatedKey
		itr.idx = 0
	}

	itr.err = retry(func() error {
		var err error
		itr.output, err = itr.scan.table.db.client.Scan(itr.input)
		return err
	})

	if itr.err != nil || len(itr.output.Items) == 0 {
		return false
	}

	// ran out of results
	if len(itr.output.Items) == 0 {
		return false
	}

	itr.err = itr.unmarshal(itr.output.Items[itr.idx], out)
	itr.idx++
	fmt.Println("err", itr.err)
	return itr.err == nil
}

func (itr *scanIter) Err() error {
	return itr.err
}
