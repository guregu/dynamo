package dynamo

import (
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Scan struct {
	table    Table
	startKey map[string]*dynamodb.AttributeValue
	index    string

	projection string
	filter     string
	limit      int64
	segments   int // TODO

	subber

	err error
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

func (s *Scan) Project(attribs ...string) *Scan {
	expr, err := s.subExpr(strings.Join(attribs, ", "), nil)
	s.setError(err)
	s.projection = expr
	return s
}

func (s *Scan) Filter(expr string, args ...interface{}) *Scan {
	expr, err := s.subExpr(expr, args...)
	s.setError(err)
	s.filter = expr
	return s
}

func (s *Scan) Iter() Iter {
	return &scanIter{
		scan:      s,
		unmarshal: unmarshalItem,
		err:       s.err,
	}
}

func (s *Scan) All(out interface{}) error {
	itr := &scanIter{
		scan:      s,
		unmarshal: unmarshalAppend,
		err:       s.err,
	}
	for itr.Next(out) {
	}
	return itr.Err()
}

func (s *Scan) scanInput() *dynamodb.ScanInput {
	input := &dynamodb.ScanInput{
		ExclusiveStartKey:         s.startKey,
		TableName:                 &s.table.Name,
		ExpressionAttributeNames:  s.nameExpr,
		ExpressionAttributeValues: s.valueExpr,
	}
	if s.index != "" {
		input.IndexName = &s.index
	}
	if s.projection != "" {
		input.ProjectionExpression = &s.projection
	}
	if s.filter != "" {
		input.FilterExpression = &s.filter
	}
	if s.limit > 0 {
		input.Limit = &s.limit
	}
	return input
}

func (s *Scan) setError(err error) {
	if s.err == nil {
		s.err = err
	}
}

// scanIter is the iterator for Scan operations
type scanIter struct {
	scan   *Scan
	input  *dynamodb.ScanInput
	output *dynamodb.ScanOutput
	err    error
	idx    int

	unmarshal func(map[string]*dynamodb.AttributeValue, interface{}) error
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

	itr.err = itr.unmarshal(itr.output.Items[itr.idx], out)
	itr.idx++
	return itr.err == nil
}

func (itr *scanIter) Err() error {
	return itr.err
}
