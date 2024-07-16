package dynamo

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"golang.org/x/sync/errgroup"
)

// Scan is a request to scan all the data in a table.
// See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html
type Scan struct {
	table    Table
	startKey Item
	index    string

	projection  string
	filters     []string
	consistent  bool
	limit       int
	searchLimit int32
	reqLimit    int

	segment       int32
	totalSegments int32

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
// totalSegments must be less than MaxInt32 due to API limits.
func (s *Scan) Segment(segment int, totalSegments int) *Scan {
	s.segment = int32(segment)
	s.totalSegments = int32(totalSegments)
	if totalSegments > math.MaxInt32 {
		s.setError(fmt.Errorf("dynamo: total segments in Scan must be less than or equal to %d (got %d)", math.MaxInt32, totalSegments))
	}
	return s
}

func (s *Scan) newSegments(segments int, leks []PagingKey) []*scanIter {
	iters := make([]*scanIter, segments)
	lekLen := len(leks)
	for i := int(0); i < segments; i++ {
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
func (s *Scan) Limit(limit int) *Scan {
	s.limit = limit
	return s
}

// SearchLimit specifies the maximum amount of results to evaluate.
// Use this along with StartFrom and Iter's LastEvaluatedKey to split up results.
// Note that DynamoDB limits result sets to 1MB.
// SearchLimit > 0 implies RequestLimit(1).
func (s *Scan) SearchLimit(limit int) *Scan {
	s.searchLimit = int32(min(limit, math.MaxInt32))
	return s
}

// RequestLimit specifies the maximum amount of requests to make against DynamoDB's API.
// A limit of zero or less means unlimited requests.
func (s *Scan) RequestLimit(limit int) *Scan {
	s.reqLimit = limit
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
func (s *Scan) IterParallel(ctx context.Context, segments int) ParallelIter {
	iters := s.newSegments(segments, nil)
	ps := newParallelScan(iters, s.cc, false, unmarshalItem)
	go ps.run(ctx)
	return ps
}

// IterParallelFrom returns a results iterator continued from a previous ParallelIter's LastEvaluatedKeys.
// Canceling the context given here will cancel the processing of all segments.
func (s *Scan) IterParallelStartFrom(ctx context.Context, keys []PagingKey) ParallelIter {
	iters := s.newSegments(len(keys), keys)
	ps := newParallelScan(iters, s.cc, false, unmarshalItem)
	go ps.run(ctx)
	return ps
}

// All executes this request and unmarshals all results to out, which must be a pointer to a slice.
func (s *Scan) All(ctx context.Context, out interface{}) error {
	itr := &scanIter{
		scan:      s,
		unmarshal: unmarshalAppendTo(out),
		err:       s.err,
	}
	for itr.Next(ctx, out) {
	}
	return itr.Err()
}

// AllWithLastEvaluatedKey executes this request and unmarshals all results to out, which must be a pointer to a slice.
// It returns a key you can use with StartWith to continue this query.
func (s *Scan) AllWithLastEvaluatedKey(ctx context.Context, out interface{}) (PagingKey, error) {
	itr := &scanIter{
		scan:      s,
		unmarshal: unmarshalAppendTo(out),
		err:       s.err,
	}
	for itr.Next(ctx, out) {
	}
	lek, err := itr.LastEvaluatedKey(ctx)
	return lek, errors.Join(itr.Err(), err)
}

// AllParallel executes this request by running the given number of segments in parallel, then unmarshaling all results to out, which must be a pointer to a slice.
func (s *Scan) AllParallel(ctx context.Context, segments int, out interface{}) error {
	iters := s.newSegments(segments, nil)
	ps := newParallelScan(iters, s.cc, true, unmarshalAppendTo(out))
	go ps.run(ctx)
	for ps.Next(ctx, out) {
	}
	return ps.Err()
}

// AllParallelWithLastEvaluatedKeys executes this request by running the given number of segments in parallel, then unmarshaling all results to out, which must be a pointer to a slice.
// Returns a slice of LastEvalutedKeys that can be used to continue the query later.
func (s *Scan) AllParallelWithLastEvaluatedKeys(ctx context.Context, segments int, out interface{}) ([]PagingKey, error) {
	iters := s.newSegments(segments, nil)
	ps := newParallelScan(iters, s.cc, false, unmarshalAppendTo(out))
	go ps.run(ctx)
	for ps.Next(ctx, out) {
	}
	leks, err := ps.LastEvaluatedKeys(ctx)
	return leks, errors.Join(ps.Err(), err)
}

// AllParallelStartFrom executes this request by continuing parallel scans from the given LastEvaluatedKeys, then unmarshaling all results to out, which must be a pointer to a slice.
// Returns a new slice of LastEvaluatedKeys after the scan finishes.
func (s *Scan) AllParallelStartFrom(ctx context.Context, keys []PagingKey, out interface{}) ([]PagingKey, error) {
	iters := s.newSegments(len(keys), keys)
	ps := newParallelScan(iters, s.cc, false, unmarshalAppendTo(out))
	go ps.run(ctx)
	for ps.Next(ctx, out) {
	}
	leks, err := ps.LastEvaluatedKeys(ctx)
	return leks, errors.Join(ps.Err(), err)
}

// Count executes this request and returns the number of items matching the scan.
// It takes into account the filter, limit, search limit, and all other parameters given.
// It may return a higher count than the limits.
func (s *Scan) Count(ctx context.Context) (int, error) {
	if s.err != nil {
		return 0, s.err
	}
	var count int
	var scanned int32
	input := s.scanInput()
	input.Select = types.SelectCount
	var reqs int
	for {
		var out *dynamodb.ScanOutput
		err := s.table.db.retry(ctx, func() error {
			var err error
			out, err = s.table.db.client.Scan(ctx, input)
			s.cc.incRequests()
			return err
		})
		if err != nil {
			return 0, err
		}
		reqs++

		count += int(out.Count)
		scanned += out.ScannedCount
		s.cc.add(out.ConsumedCapacity)

		if out.LastEvaluatedKey == nil ||
			(s.limit > 0 && count >= s.limit) ||
			(s.searchLimit > 0 && scanned >= s.searchLimit) ||
			(s.reqLimit > 0 && reqs >= s.reqLimit) {
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
			limit := int32(min(s.limit, math.MaxInt32))
			input.Limit = &limit
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
		input.ReturnConsumedCapacity = types.ReturnConsumedCapacityIndexes
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
func (itr *scanIter) Next(ctx context.Context, out interface{}) bool {
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
		// have we hit the request limit?
		if itr.scan.reqLimit > 0 && itr.reqs == itr.scan.reqLimit {
			return false
		}

		// no, prepare next request and reset index
		itr.input.ExclusiveStartKey = itr.output.LastEvaluatedKey
		itr.idx = 0
	}

	itr.err = itr.scan.table.db.retry(ctx, func() error {
		var err error
		itr.output, err = itr.scan.table.db.client.Scan(ctx, itr.input)
		itr.scan.cc.incRequests()
		return err
	})

	if itr.err != nil {
		return false
	}
	itr.scan.cc.add(itr.output.ConsumedCapacity)
	if len(itr.output.LastEvaluatedKey) > len(itr.exLEK) {
		itr.exLEK = itr.output.LastEvaluatedKey
	}
	itr.reqs++

	if len(itr.output.Items) == 0 {
		if itr.scan.reqLimit > 0 && itr.reqs == itr.scan.reqLimit {
			return false
		}
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
func (itr *scanIter) LastEvaluatedKey(ctx context.Context) (PagingKey, error) {
	if itr.output != nil {
		// if we've hit the end of our results, we can use the real LEK
		if itr.idx == len(itr.output.Items) {
			return itr.output.LastEvaluatedKey, nil
		}

		// figure out the primary keys if needed
		if itr.keys == nil && itr.keyErr == nil {
			itr.keys, itr.keyErr = itr.scan.table.primaryKeys(ctx, itr.exLEK, itr.exESK, itr.scan.index)
		}
		if itr.keyErr != nil {
			// primaryKeys can fail if the credentials lack DescribeTable permissions
			// in order to preserve backwards compatibility, we fall back to the old behavior and warn
			// see: https://github.com/guregu/dynamo/pull/187#issuecomment-1045183901
			return itr.output.LastEvaluatedKey, fmt.Errorf("dynamo: failed to determine LastEvaluatedKey in scan: %w", itr.keyErr)
		}

		// we can't use the real LEK, so we need to infer the LEK from the last item we saw
		lek, err := lekify(itr.last, itr.keys)
		if err != nil {
			return itr.output.LastEvaluatedKey, fmt.Errorf("dynamo: failed to infer LastEvaluatedKey in scan: %w", err)
		}
		return lek, nil
	}
	return nil, nil
}

type parallelScan struct {
	iters []*scanIter
	items chan Item

	leks   []PagingKey
	lekErr error

	cc  *ConsumedCapacity
	err error
	mu  *sync.Mutex

	unmarshal unmarshalFunc
}

func newParallelScan(iters []*scanIter, cc *ConsumedCapacity, skipLEK bool, unmarshal unmarshalFunc) *parallelScan {
	ps := &parallelScan{
		iters:     iters,
		items:     make(chan Item),
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
			var item Item
			for iter.Next(ctx, &item) {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case ps.items <- item:
					// reset the map, so we don't overwrite the one we've already sent
					item = nil
				}

				if ps.leks != nil {
					lek, err := iter.LastEvaluatedKey(ctx)
					ps.mu.Lock()
					ps.leks[i] = lek
					if err != nil && ps.lekErr == nil {
						ps.lekErr = err
					}
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

func (ps *parallelScan) Next(ctx context.Context, out interface{}) bool {
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

func (ps *parallelScan) LastEvaluatedKeys(_ context.Context) ([]PagingKey, error) {
	keys := make([]PagingKey, len(ps.leks))
	ps.mu.Lock()
	defer ps.mu.Unlock()
	copy(keys, ps.leks)
	return keys, ps.lekErr
}
