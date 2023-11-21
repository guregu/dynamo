package dynamo

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/cenkalti/backoff/v4"
)

// DynamoDB API limit, 100 operations per request
const maxGetOps = 100

// Batch stores the names of the hash key and range key
// for creating new batches.
type Batch struct {
	table             Table
	hashKey, rangeKey string
	err               error
}

// Batch creates a new batch with the given hash key name, and range key name if provided.
// For purely Put batches, neither is necessary.
func (table Table) Batch(hashAndRangeKeyName ...string) Batch {
	b := Batch{
		table: table,
	}
	switch len(hashAndRangeKeyName) {
	case 0:
	case 1:
		b.hashKey = hashAndRangeKeyName[0]
	case 2:
		b.hashKey = hashAndRangeKeyName[0]
		b.rangeKey = hashAndRangeKeyName[1]
	default:
		b.err = fmt.Errorf("dynamo: batch: you may only provide the name of a range key and hash key. too many keys")
	}
	return b
}

// BatchGet is a BatchGetItem operation.
type BatchGet struct {
	batch      Batch
	reqs       []*Query
	projection string
	consistent bool

	subber
	err error
	cc  *ConsumedCapacity
}

// Get creates a new batch get item request with the given keys.
//
//	table.Batch("ID", "Month").
//		Get(dynamo.Keys{1, "2015-10"}, dynamo.Keys{42, "2015-12"}, dynamo.Keys{42, "1992-02"}).
//		All(&results)
func (b Batch) Get(keys ...Keyed) *BatchGet {
	bg := &BatchGet{
		batch: b,
		err:   b.err,
	}
	bg.add(keys)
	return bg
}

// And adds more keys to be gotten.
func (bg *BatchGet) And(keys ...Keyed) *BatchGet {
	bg.add(keys)
	return bg
}

func (bg *BatchGet) add(keys []Keyed) {
	for _, key := range keys {
		if key == nil {
			bg.setError(errors.New("dynamo: batch: the Keyed interface must not be nil"))
			break
		}
		get := bg.batch.table.Get(bg.batch.hashKey, key.HashKey())
		if rk := key.RangeKey(); bg.batch.rangeKey != "" && rk != nil {
			get.Range(bg.batch.rangeKey, Equal, rk)
			bg.setError(get.err)
		}
		bg.reqs = append(bg.reqs, get)
	}
}

// Project limits the result attributes to the given paths.
func (bg *BatchGet) Project(paths ...string) *BatchGet {
	var expr string
	for i, p := range paths {
		if i != 0 {
			expr += ", "
		}
		name, err := bg.escape(p)
		bg.setError(err)
		expr += name
	}
	bg.projection = expr
	return bg
}

// Consistent will, if on is true, make this batch use a strongly consistent read.
// Reads are eventually consistent by default.
// Strongly consistent reads are more resource-heavy than eventually consistent reads.
func (bg *BatchGet) Consistent(on bool) *BatchGet {
	bg.consistent = on
	return bg
}

// ConsumedCapacity will measure the throughput capacity consumed by this operation and add it to cc.
func (bg *BatchGet) ConsumedCapacity(cc *ConsumedCapacity) *BatchGet {
	bg.cc = cc
	return bg
}

// All executes this request and unmarshals all results to out, which must be a pointer to a slice.
func (bg *BatchGet) All(out interface{}) error {
	iter := newBGIter(bg, unmarshalAppendTo(out), bg.err)
	for iter.Next(out) {
	}
	return iter.Err()
}

// AllWithContext executes this request and unmarshals all results to out, which must be a pointer to a slice.
func (bg *BatchGet) AllWithContext(ctx context.Context, out interface{}) error {
	iter := newBGIter(bg, unmarshalAppendTo(out), bg.err)
	for iter.NextWithContext(ctx, out) {
	}
	return iter.Err()
}

// Iter returns a results iterator for this batch.
func (bg *BatchGet) Iter() Iter {
	return newBGIter(bg, unmarshalItem, bg.err)
}

func (bg *BatchGet) input(start int) *dynamodb.BatchGetItemInput {
	if start >= len(bg.reqs) {
		return nil // done
	}
	end := start + maxGetOps
	if end > len(bg.reqs) {
		end = len(bg.reqs)
	}

	in := &dynamodb.BatchGetItemInput{
		RequestItems: make(map[string]*dynamodb.KeysAndAttributes, 1),
	}

	if bg.projection != "" {
		for _, get := range bg.reqs[start:end] {
			get.Project(get.projection)
			bg.setError(get.err)
		}
	}
	if bg.cc != nil {
		in.ReturnConsumedCapacity = aws.String(dynamodb.ReturnConsumedCapacityIndexes)
	}

	var kas *dynamodb.KeysAndAttributes
	for _, get := range bg.reqs[start:end] {
		if kas == nil {
			kas = get.keysAndAttribs()
			continue
		}
		kas.Keys = append(kas.Keys, get.keys())
	}
	if bg.projection != "" {
		kas.ProjectionExpression = &bg.projection
		kas.ExpressionAttributeNames = bg.nameExpr
	}
	if bg.consistent {
		kas.ConsistentRead = &bg.consistent
	}
	in.RequestItems[bg.batch.table.Name()] = kas
	return in
}

func (bg *BatchGet) setError(err error) {
	if bg.err == nil {
		bg.err = err
	}
}

// bgIter is the iterator for Batch Get operations
type bgIter struct {
	bg        *BatchGet
	input     *dynamodb.BatchGetItemInput
	output    *dynamodb.BatchGetItemOutput
	err       error
	idx       int
	total     int
	processed int
	backoff   *backoff.ExponentialBackOff
	unmarshal unmarshalFunc
}

func newBGIter(bg *BatchGet, fn unmarshalFunc, err error) *bgIter {
	if err == nil && len(bg.reqs) == 0 {
		err = ErrNoInput
	}

	iter := &bgIter{
		bg:        bg,
		err:       err,
		backoff:   backoff.NewExponentialBackOff(),
		unmarshal: fn,
	}
	iter.backoff.MaxElapsedTime = 0
	return iter
}

// Next tries to unmarshal the next result into out.
// Returns false when it is complete or if it runs into an error.
func (itr *bgIter) Next(out interface{}) bool {
	ctx, cancel := defaultContext()
	defer cancel()
	return itr.NextWithContext(ctx, out)
}

func (itr *bgIter) NextWithContext(ctx context.Context, out interface{}) bool {
	// stop if we have an error
	if ctx.Err() != nil {
		itr.err = ctx.Err()
	}
	if itr.err != nil {
		return false
	}

	tableName := itr.bg.batch.table.Name()

redo:
	// can we use results we already have?
	if itr.output != nil && itr.idx < len(itr.output.Responses[tableName]) {
		items := itr.output.Responses[tableName]
		item := items[itr.idx]
		itr.err = itr.unmarshal(item, out)
		itr.idx++
		itr.total++
		return itr.err == nil
	}

	// new bg
	if itr.input == nil {
		itr.input = itr.bg.input(itr.processed)
	}

	if itr.output != nil && itr.idx >= len(itr.output.Responses[tableName]) {
		var unprocessed int
		if itr.output.UnprocessedKeys != nil && itr.output.UnprocessedKeys[tableName] != nil {
			unprocessed = len(itr.output.UnprocessedKeys[tableName].Keys)
		}
		itr.processed += len(itr.input.RequestItems[tableName].Keys) - unprocessed
		// have we exhausted all results?
		if len(itr.output.UnprocessedKeys) == 0 {
			// yes, try to get next inner batch of 100 items
			if itr.input = itr.bg.input(itr.processed); itr.input == nil {
				// we're done, no more input
				if itr.err == nil && itr.total == 0 {
					itr.err = ErrNotFound
				}
				return false
			}
		} else {
			// no, prepare a new request with the remaining keys
			itr.input.RequestItems = itr.output.UnprocessedKeys
			// we need to sleep here a bit as per the official docs
			if err := aws.SleepWithContext(ctx, itr.backoff.NextBackOff()); err != nil {
				// timed out
				itr.err = err
				return false
			}
		}
		itr.idx = 0
	}

	itr.err = itr.bg.batch.table.db.retry(ctx, func() error {
		var err error
		itr.output, err = itr.bg.batch.table.db.client.BatchGetItemWithContext(ctx, itr.input)
		return err
	})
	if itr.err != nil {
		return false
	}
	if itr.bg.cc != nil {
		for _, cc := range itr.output.ConsumedCapacity {
			addConsumedCapacity(itr.bg.cc, cc)
		}
	}

	// we've got unprocessed results, marshal one
	goto redo
}

// Err returns the error encountered, if any.
// You should check this after Next is finished.
func (itr *bgIter) Err() error {
	return itr.err
}
