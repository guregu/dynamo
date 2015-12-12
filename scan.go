package dynamo

import (
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Scan is a request to scan all the data in a table.
// See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html
type Scan struct {
	table    Table
	startKey map[string]*dynamodb.AttributeValue
	index    string

	projection string
	filter     string
	consistent bool
	limit      int64 // TODO
	segments   int   // TODO

	subber

	err error
}

// Scan creates a new request to scan this table.
func (table Table) Scan() *Scan {
	return &Scan{
		table: table,
	}
}

// Index specifies the name of the index that Scan will operate on.
func (s *Scan) Index(name string) *Scan {
	s.index = name
	return s
}

// Project limits the result attributes to the given paths.
func (s *Scan) Project(paths ...string) *Scan {
	expr, err := s.subExpr(strings.Join(paths, ", "), nil)
	s.setError(err)
	s.projection = expr
	return s
}

// Filter takes an expression that all results will be evaluated against.
// Use single quotes to specificy reserved names inline (like 'Count').
// Use the placeholder ? within the expression to substitute values, and use $ for names.
// You need to use quoted or placeholder names when the name is a reserved word in DynamoDB.
func (s *Scan) Filter(expr string, args ...interface{}) *Scan {
	expr, err := s.subExpr(expr, args...)
	s.setError(err)
	s.filter = expr
	return s
}

// Consistent will, if on is true, make this scan use a strongly consistent read.
// Scans are eventually consistent by default.
// Strongly consistent reads are more resource-heavy than eventually consistent reads.
func (s *Scan) Consistent(on bool) *Scan {
	s.consistent = on
	return s
}

// Iter returns a results iterator for this request.
func (s *Scan) Iter() Iter {
	return &scanIter{
		scan:      s,
		unmarshal: unmarshalItem,
		err:       s.err,
	}
}

// All executes this request and unmarshals all results to out, which must be a pointer to a slice.
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
		TableName:                 &s.table.name,
		ConsistentRead:            &s.consistent,
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

	unmarshal unmarshalFunc
}

// Next tries to unmarshal the next result into out.
// Returns false when it is complete or if it runs into an error.
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

// Err returns the error encountered, if any.
// You should check this after Next is finished.
func (itr *scanIter) Err() error {
	return itr.err
}
