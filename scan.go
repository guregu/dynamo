package dynamo

import (
	"context"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"golang.org/x/sync/errgroup"
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

	segment       int64
	totalSegments int64

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
// Ignored by ParallelIter and friends, pass multiple keys to ParallelIterStartFrom instead.
func (s *Scan) StartFrom(key PagingKey) *Scan {
	s.startKey = key
	return s
}

// Index specifies the name of the index that Scan will operate on.
func (s *Scan) Index(name string) *Scan {
	s.index = name
	return s
}

// Segment specifies the Segment and Total Segments to operate on in a manual parallel scan.
// This is useful if you want to control the parallel scans by yourself instead of using ParallelIter.
// Ignored by ParallelIter and friends.
func (s *Scan) Segment(segment int64, totalSegments int64) *Scan {
	s.segment = segment
	s.totalSegments = totalSegments
	return s
}

func (s *Scan) newSegments(segments int64, leks []PagingKey) []*scanIter {
	iters := make([]*scanIter, segments)
	lekLen := int64(len(leks))
	for i := int64(0); i < segments; i++ {
		seg := *s
		var cc *ConsumedCapacity
		if s.cc != nil {
			cc = new(ConsumedCapacity)
		}
		seg.Segment(i, segments).ConsumedCapacity(cc)
		if i < lekLen {
			lek := leks[i]
			if lek == nil {
				continue
			}
			seg.StartFrom(leks[i])
		} else {
			seg.StartFrom(nil)
		}
		iters[i] = seg.Iter().(*scanIter)
	}
	return iters
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
	expr, err := s.subExprN(expr, args...)
	s.setError(err)
	s.filters = append(s.filters, wrapExpr(expr))
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

// IterParallel returns a results iterator for this request, running the given number of segments in parallel.
// Canceling the context given here will cancel the processing of all segments.
func (s *Scan) IterParallel(ctx context.Context, segments int64) ParallelIter {
	iters := s.newSegments(segments, nil)
	ps := newParallelScan(iters, s.cc, false, unmarshalItem)
	go ps.run(ctx)
	return ps
}

// IterParallelFrom returns a results iterator continued from a previous ParallelIter's LastEvaluatedKeys.
// Canceling the context given here will cancel the processing of all segments.
func (s *Scan) IterParallelStartFrom(ctx context.Context, keys []PagingKey) ParallelIter {
	iters := s.newSegments(int64(len(keys)), keys)
	ps := newParallelScan(iters, s.cc, false, unmarshalItem)
	go ps.run(ctx)
	return ps
}

// All executes this request and unmarshals all results to out, which must be a pointer to a slice.
func (s *Scan) All(out interface{}) error {
	ctx, cancel := defaultContext()
	defer cancel()
	return s.AllWithContext(ctx, out)
}

// AllWithContext executes this request and unmarshals all results to out, which must be a pointer to a slice.
func (s *Scan) AllWithContext(ctx context.Context, out interface{}) error {
	itr := &scanIter{
		scan:      s,
		unmarshal: unmarshalAppend,
		err:       s.err,
	}
	for itr.NextWithContext(ctx, out) {
	}
	return itr.Err()
}

// AllWithLastEvaluatedKey executes this request and unmarshals all results to out, which must be a pointer to a slice.
// It returns a key you can use with StartWith to continue this query.
func (s *Scan) AllWithLastEvaluatedKey(out interface{}) (PagingKey, error) {
	ctx, cancel := defaultContext()
	defer cancel()
	return s.AllWithLastEvaluatedKeyContext(ctx, out)
}

// AllWithLastEvaluatedKeyContext executes this request and unmarshals all results to out, which must be a pointer to a slice.
// It returns a key you can use with StartWith to continue this query.
func (s *Scan) AllWithLastEvaluatedKeyContext(ctx context.Context, out interface{}) (PagingKey, error) {
	itr := &scanIter{
		scan:      s,
		unmarshal: unmarshalAppend,
		err:       s.err,
	}
	for itr.NextWithContext(ctx, out) {
	}
	return itr.LastEvaluatedKey(), itr.Err()
}

// AllParallel executes this request by running the given number of segments in parallel, then unmarshaling all results to out, which must be a pointer to a slice.
func (s *Scan) AllParallel(ctx context.Context, segments int64, out interface{}) error {
	iters := s.newSegments(segments, nil)
	ps := newParallelScan(iters, s.cc, true, unmarshalAppend)
	go ps.run(ctx)
	for ps.NextWithContext(ctx, out) {
	}
	return ps.Err()
}

// AllParallelWithLastEvaluatedKeys executes this request by running the given number of segments in parallel, then unmarshaling all results to out, which must be a pointer to a slice.
// Returns a slice of LastEvalutedKeys that can be used to continue the query later.
func (s *Scan) AllParallelWithLastEvaluatedKeys(ctx context.Context, segments int64, out interface{}) ([]PagingKey, error) {
	iters := s.newSegments(segments, nil)
	ps := newParallelScan(iters, s.cc, false, unmarshalAppend)
	go ps.run(ctx)
	for ps.NextWithContext(ctx, out) {
	}
	return ps.LastEvaluatedKeys(), ps.Err()
}

// AllParallelStartFrom executes this request by continuing parallel scans from the given LastEvaluatedKeys, then unmarshaling all results to out, which must be a pointer to a slice.
// Returns a new slice of LastEvaluatedKeys after the scan finishes.
func (s *Scan) AllParallelStartFrom(ctx context.Context, keys []PagingKey, out interface{}) ([]PagingKey, error) {
	iters := s.newSegments(int64(len(keys)), keys)
	ps := newParallelScan(iters, s.cc, false, unmarshalAppend)
	go ps.run(ctx)
	for ps.NextWithContext(ctx, out) {
	}
	return ps.LastEvaluatedKeys(), ps.Err()
}

// Count executes this request and returns the number of items matching the scan.
// It takes into account the filter, limit, search limit, and all other parameters given.
// It may return a higher count than the limits.
func (s *Scan) Count() (int64, error) {
	ctx, cancel := defaultContext()
	defer cancel()
	return s.CountWithContext(ctx)
}

// CountWithContext executes this request and returns the number of items matching the scan.
// It takes into account the filter, limit, search limit, and all other parameters given.
// It may return a higher count than the limits.
func (s *Scan) CountWithContext(ctx context.Context) (int64, error) {
	if s.err != nil {
		return 0, s.err
	}
	var count, scanned int64
	input := s.scanInput()
	input.Select = aws.String(dynamodb.SelectCount)
	for {
		var out *dynamodb.ScanOutput
		err := s.table.db.retry(ctx, func() error {
			var err error
			out, err = s.table.db.client.ScanWithContext(ctx, input)
			return err
		})
		if err != nil {
			return count, err
		}

		count += *out.Count
		scanned += *out.ScannedCount

		if s.cc != nil {
			addConsumedCapacity(s.cc, out.ConsumedCapacity)
		}

		if s.limit > 0 && count >= s.limit {
			break
		}
		if s.searchLimit > 0 && scanned >= s.searchLimit {
			break
		}
		if out.LastEvaluatedKey == nil {
			break
		}

		input.ExclusiveStartKey = out.LastEvaluatedKey
	}
	return count, nil
}

func (s *Scan) scanInput() *dynamodb.ScanInput {
	input := &dynamodb.ScanInput{
		ExclusiveStartKey:         s.startKey,
		TableName:                 &s.table.name,
		ConsistentRead:            &s.consistent,
		ExpressionAttributeNames:  s.nameExpr,
		ExpressionAttributeValues: s.valueExpr,
	}
	if s.totalSegments > 0 {
		input.Segment = &s.segment
		input.TotalSegments = &s.totalSegments
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
func (itr *scanIter) Next(out interface{}) bool {
	ctx, cancel := defaultContext()
	defer cancel()
	return itr.NextWithContext(ctx, out)
}

func (itr *scanIter) NextWithContext(ctx context.Context, out interface{}) bool {
redo:
	// stop if we have an error
	if ctx.Err() != nil {
		itr.err = ctx.Err()
	}
	if itr.err != nil {
		return false
	}

	// stop if exceed limit
	if itr.scan.limit > 0 && itr.n == itr.scan.limit {
		// proactively grab the keys for LEK inferral, but don't count it as a real error yet to keep backwards compat
		itr.keys, itr.keyErr = itr.scan.table.primaryKeys(ctx, itr.exLEK, itr.exESK, itr.scan.index)
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

	// new scan
	if itr.input == nil {
		itr.input = itr.scan.scanInput()
	}
	if len(itr.input.ExclusiveStartKey) > len(itr.exESK) {
		itr.exESK = itr.input.ExclusiveStartKey
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

	itr.err = itr.scan.table.db.retry(ctx, func() error {
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
	if len(itr.output.LastEvaluatedKey) > len(itr.exLEK) {
		itr.exLEK = itr.output.LastEvaluatedKey
	}

	if len(itr.output.Items) == 0 {
		if itr.output.LastEvaluatedKey != nil {
			goto redo
		}
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
func (itr *scanIter) Err() error {
	return itr.err
}

// LastEvaluatedKey returns a key that can be used to continue this scan.
// Use with SearchLimit for best results.
func (itr *scanIter) LastEvaluatedKey() PagingKey {
	if itr.output != nil {
		// if we've hit the end of our results, we can use the real LEK
		if itr.idx == len(itr.output.Items) {
			return itr.output.LastEvaluatedKey
		}

		// figure out the primary keys if needed
		if itr.keys == nil && itr.keyErr == nil {
			ctx, _ := defaultContext() // TODO(v2): take context instead of using the default
			itr.keys, itr.keyErr = itr.scan.table.primaryKeys(ctx, itr.exLEK, itr.exESK, itr.scan.index)
		}
		if itr.keyErr != nil {
			// primaryKeys can fail if the credentials lack DescribeTable permissions
			// in order to preserve backwards compatibility, we fall back to the old behavior and warn
			// see: https://github.com/guregu/dynamo/pull/187#issuecomment-1045183901
			// TODO(v2): rejigger this API.
			itr.scan.table.db.log("dynamo: Warning:", itr.keyErr, "Returning a later LastEvaluatedKey.")
			return itr.output.LastEvaluatedKey
		}

		// we can't use the real LEK, so we need to infer the LEK from the last item we saw
		lek, err := lekify(itr.last, itr.keys)
		// unfortunately, this API can't return an error so a warning is the best we can do...
		// this matches old behavior before the LEK was automatically generated
		// TODO(v2): fix this.
		if err != nil {
			itr.scan.table.db.log("dynamo: Warning:", err, "Returning a later LastEvaluatedKey.")
			return itr.output.LastEvaluatedKey
		}
		return lek
	}
	return nil
}

type parallelScan struct {
	iters []*scanIter
	items chan map[string]*dynamodb.AttributeValue

	leks []PagingKey
	cc   *ConsumedCapacity
	err  error
	mu   *sync.Mutex

	unmarshal unmarshalFunc
}

func newParallelScan(iters []*scanIter, cc *ConsumedCapacity, skipLEK bool, unmarshal unmarshalFunc) *parallelScan {
	ps := &parallelScan{
		iters:     iters,
		items:     make(chan map[string]*dynamodb.AttributeValue),
		cc:        cc,
		mu:        new(sync.Mutex),
		unmarshal: unmarshal,
	}
	if !skipLEK {
		ps.leks = make([]PagingKey, len(ps.iters))
	}
	return ps
}

func (ps *parallelScan) run(ctx context.Context) {
	grp, ctx := errgroup.WithContext(ctx)
	for i, iter := range ps.iters {
		i, iter := i, iter
		if iter == nil {
			continue
		}
		grp.Go(func() error {
			var item map[string]*dynamodb.AttributeValue
			for iter.NextWithContext(ctx, &item) {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case ps.items <- item:
				}

				if ps.leks != nil {
					lek := iter.LastEvaluatedKey()
					ps.mu.Lock()
					ps.leks[i] = lek
					ps.mu.Unlock()
				}
			}

			if ps.cc != nil && iter.scan.cc != nil {
				ps.mu.Lock()
				mergeConsumedCapacity(ps.cc, iter.scan.cc)
				ps.mu.Unlock()
			}

			return iter.Err()
		})
	}
	err := grp.Wait()
	if err != nil {
		ps.setError(err)
	}
	close(ps.items)
}

func (ps *parallelScan) Next(out interface{}) bool {
	ctx, cancel := defaultContext()
	defer cancel()
	return ps.NextWithContext(ctx, out)
}

func (ps *parallelScan) NextWithContext(ctx context.Context, out interface{}) bool {
	select {
	case <-ctx.Done():
		ps.setError(ctx.Err())
		return false
	case item := <-ps.items:
		if item == nil {
			return false
		}
		if err := ps.unmarshal(item, out); err != nil {
			ps.setError(err)
			return false
		}
		return true
	}
}

func (ps *parallelScan) setError(err error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.err == nil {
		ps.err = err
	}
}

func (ps *parallelScan) Err() error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.err
}

func (ps *parallelScan) LastEvaluatedKeys() []PagingKey {
	keys := make([]PagingKey, len(ps.leks))
	ps.mu.Lock()
	defer ps.mu.Unlock()
	copy(keys, ps.leks)
	return keys
}
