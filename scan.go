package dynamo

import (
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Scan is a request to scan all the data in a table.
// See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html
type Scan struct {
	table    Table
	startKey map[string]*dynamodb.AttributeValue
	index    string

	projection  string
	filters     []string
	consistent  bool
	limit       int64
	searchLimit int64

	subber

	err error
	cc  *ConsumedCapacity
}

// Scan creates a new request to scan this table.
func (table Table) Scan() *Scan {
	return &Scan{
		table: table,
	}
}

// StartFrom makes this scan continue from a previous one.
// Use Scan.Iter's LastEvaluatedKey.
func (s *Scan) StartFrom(key PagingKey) *Scan {
	s.startKey = key
	return s
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
// Multiple calls to Filter will be combined with AND.
func (s *Scan) Filter(expr string, args ...interface{}) *Scan {
	expr, err := s.subExpr(expr, args...)
	s.setError(err)
	s.filters = append(s.filters, expr)
	return s
}

// Consistent will, if on is true, make this scan use a strongly consistent read.
// Scans are eventually consistent by default.
// Strongly consistent reads are more resource-heavy than eventually consistent reads.
func (s *Scan) Consistent(on bool) *Scan {
	s.consistent = on
	return s
}

// Limit specifies the maximum amount of results to return.
func (s *Scan) Limit(limit int64) *Scan {
	s.limit = limit
	return s
}

// SearchLimit specifies a maximum amount of results to evaluate.
// Use this along with StartFrom and Iter's LastEvaluatedKey to split up results.
// Note that DynamoDB limits result sets to 1MB.
func (s *Scan) SearchLimit(limit int64) *Scan {
	s.searchLimit = limit
	return s
}

// ConsumedCapacity will measure the throughput capacity consumed by this operation and add it to cc.
func (s *Scan) ConsumedCapacity(cc *ConsumedCapacity) *Scan {
	s.cc = cc
	return s
}

// Iter returns a results iterator for this request.
func (s *Scan) Iter() PagingIter {
	return &scanIter{
		scan:      s,
		unmarshal: unmarshalItem,
		err:       s.err,
	}
}

// All executes this request and unmarshals all results to out, which must be a pointer to a slice.
func (s *Scan) All(out interface{}) error {
	ctx, cancel := defaultContext()
	defer cancel()
	_, err := s.AllWithLastEvaluatedKeyContext(ctx, out)
	return err
}

// AllWithContext executes this request and unmarshals all results to out, which must be a pointer to a slice.
func (s *Scan) AllWithContext(ctx aws.Context, out interface{}) error {
	_, err := s.AllWithLastEvaluatedKeyContext(ctx, out)
	return err
}

// AllWithLastEvaluatedKey executes this request and unmarshals all results to out, which must be a pointer to a slice.
// It returns a key you can use with StartWith to continue this query.
func (s *Scan) AllWithLastEvaluatedKey(ctx aws.Context, out interface{}) (PagingKey, error) {
	ctx, cancel := defaultContext()
	defer cancel()
	return s.AllWithLastEvaluatedKeyContext(ctx, out)
}

// AllWithLastEvaluatedKeyContext executes this request and unmarshals all results to out, which must be a pointer to a slice.
// It returns a key you can use with StartWith to continue this query.
func (s *Scan) AllWithLastEvaluatedKeyContext(ctx aws.Context, out interface{}) (PagingKey, error) {
	itr := &scanIter{
		scan:      s,
		unmarshal: unmarshalAppend,
		err:       s.err,
	}
	for itr.NextWithContext(ctx, out) {
	}
	return itr.LastEvaluatedKey(), itr.Err()
}

func (s *Scan) scanInput() *dynamodb.ScanInput {
	input := &dynamodb.ScanInput{
		ExclusiveStartKey:         s.startKey,
		TableName:                 &s.table.name,
		ConsistentRead:            &s.consistent,
		ExpressionAttributeNames:  s.nameExpr,
		ExpressionAttributeValues: s.valueExpr,
	}
	if s.limit > 0 {
		if len(s.filters) == 0 {
			input.Limit = &s.limit
		}
	}
	if s.searchLimit > 0 {
		input.Limit = &s.searchLimit
	}
	if s.index != "" {
		input.IndexName = &s.index
	}
	if s.projection != "" {
		input.ProjectionExpression = &s.projection
	}
	if len(s.filters) > 0 {
		filter := strings.Join(s.filters, " AND ")
		input.FilterExpression = &filter
	}
	if s.cc != nil {
		input.ReturnConsumedCapacity = aws.String(dynamodb.ReturnConsumedCapacityIndexes)
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
	n      int64

	unmarshal unmarshalFunc
}

// Next tries to unmarshal the next result into out.
// Returns false when it is complete or if it runs into an error.
func (itr *scanIter) Next(out interface{}) bool {
	ctx, cancel := defaultContext()
	defer cancel()
	return itr.NextWithContext(ctx, out)
}

func (itr *scanIter) NextWithContext(ctx aws.Context, out interface{}) bool {
	// stop if we have an error
	if itr.err != nil {
		return false
	}

	// stop if exceed limit
	if itr.scan.limit > 0 && itr.n == itr.scan.limit {
		return false
	}

	// can we use results we already have?
	if itr.output != nil && itr.idx < len(itr.output.Items) {
		item := itr.output.Items[itr.idx]
		itr.err = itr.unmarshal(item, out)
		itr.idx++
		itr.n++
		return itr.err == nil
	}

	// new scan
	if itr.input == nil {
		itr.input = itr.scan.scanInput()
	}
	if itr.output != nil && itr.idx >= len(itr.output.Items) {
		// have we exhausted all results?
		if itr.output.LastEvaluatedKey == nil || itr.scan.searchLimit > 0 {
			return false
		}

		// no, prepare next request and reset index
		itr.input.ExclusiveStartKey = itr.output.LastEvaluatedKey
		itr.idx = 0
	}

	itr.err = retry(ctx, func() error {
		var err error
		itr.output, err = itr.scan.table.db.client.ScanWithContext(ctx, itr.input)
		return err
	})

	if itr.err != nil {
		return false
	}

	if itr.scan.cc != nil {
		addConsumedCapacity(itr.scan.cc, itr.output.ConsumedCapacity)
	}

	if len(itr.output.Items) == 0 {
		if itr.output.LastEvaluatedKey != nil {
			return itr.NextWithContext(ctx, out)
		}
		return false
	}

	itr.err = itr.unmarshal(itr.output.Items[itr.idx], out)
	itr.idx++
	itr.n++
	return itr.err == nil
}

// Err returns the error encountered, if any.
// You should check this after Next is finished.
func (itr *scanIter) Err() error {
	return itr.err
}

// LastEvaluatedKey returns a key that can be used to continue this scan.
// Use with SearchLimit for best results.
func (itr *scanIter) LastEvaluatedKey() PagingKey {
	if itr.output != nil {
		return itr.output.LastEvaluatedKey
	}
	return nil
}
