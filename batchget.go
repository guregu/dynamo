package dynamo

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/time"
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
	batch       Batch
	reqs        []*Query
	projections map[string][]string // table → paths
	projection  []string            // default paths
	consistent  bool

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
	return bg.And(keys...)
}

// And adds more keys to be gotten from the default table.
// To get items from other tables, use [BatchGet.From] or [BatchGet.FromRange].
func (bg *BatchGet) And(keys ...Keyed) *BatchGet {
	return bg.add(bg.batch.table, bg.batch.hashKey, bg.batch.rangeKey, keys...)
}

// From adds more keys to be gotten from the given table.
// The given table's primary key must be a hash key (partition key) only.
// For tables with a range key (sort key) primary key, use [BatchGet.FromRange].
func (bg *BatchGet) From(table Table, hashKey string, keys ...Keyed) *BatchGet {
	return bg.add(table, hashKey, "", keys...)
}

// FromRange adds more keys to be gotten from the given table.
// For tables without a range key (sort key) primary key, use [BatchGet.From].
func (bg *BatchGet) FromRange(table Table, hashKey, rangeKey string, keys ...Keyed) *BatchGet {
	return bg.add(table, hashKey, rangeKey, keys...)
}

func (bg *BatchGet) add(table Table, hashKey string, rangeKey string, keys ...Keyed) *BatchGet {
	for _, key := range keys {
		if key == nil {
			bg.setError(errors.New("dynamo: batch: the Keyed interface must not be nil"))
			break
		}
		get := table.Get(hashKey, key.HashKey())
		if rk := key.RangeKey(); rangeKey != "" && rk != nil {
			get.Range(rangeKey, Equal, rk)
			bg.setError(get.err)
		}
		bg.reqs = append(bg.reqs, get)
	}
	return bg
}

// Project limits the result attributes to the given paths.
// This will apply to all tables, but can be overriden by [BatchGet.ProjectTable] to set specific per-table projections.
func (bg *BatchGet) Project(paths ...string) *BatchGet {
	bg.projection = paths
	return bg
}

// Project limits the result attributes to the given paths for the given table.
func (bg *BatchGet) ProjectTable(table Table, paths ...string) *BatchGet {
	return bg.project(table.Name(), paths...)
}

func (bg *BatchGet) project(table string, paths ...string) *BatchGet {
	if bg.projections == nil {
		bg.projections = make(map[string][]string)
	}
	bg.projections[table] = paths
	return bg
}

func (bg *BatchGet) projectionFor(table string) []string {
	if proj := bg.projections[table]; proj != nil {
		return proj
	}
	if bg.projection != nil {
		return bg.projection
	}
	return nil
}

// Merge copies operations and settings from src to this batch get.
func (bg *BatchGet) Merge(srcs ...*BatchGet) *BatchGet {
	for _, src := range srcs {
		bg.reqs = append(bg.reqs, src.reqs...)
		bg.consistent = bg.consistent || src.consistent
		this := bg.batch.table.Name()
		for table, proj := range src.projections {
			if this == table {
				continue
			}
			bg.mergeProjection(table, proj)
		}
		if len(src.projection) > 0 {
			if that := src.batch.table.Name(); that != this {
				bg.mergeProjection(that, src.projection)
			}
		}
	}
	return bg
}

func (bg *BatchGet) mergeProjection(table string, proj []string) {
	current := bg.projections[table]
	merged := current
	for _, path := range proj {
		if !slices.Contains(current, path) {
			merged = append(merged, path)
		}
	}
	bg.project(table, merged...)
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
func (bg *BatchGet) All(ctx context.Context, out interface{}) error {
	iter := newBGIter(bg, unmarshalAppendTo(out), nil, bg.err)
	for iter.Next(ctx, out) {
	}
	return iter.Err()
}

// Iter returns a results iterator for this batch.
func (bg *BatchGet) Iter() Iter {
	return newBGIter(bg, unmarshalItem, nil, bg.err)
}

// IterWithTable is like [BatchGet.Iter], but will update the value pointed by tablePtr after each iteration.
// This can be useful when getting from multiple tables to determine which table the latest item came from.
//
// For example, you can utilize this iterator to read the results into different structs.
//
//	widgetBatch := widgetsTable.Batch("UserID").Get(dynamo.Keys{userID})
//	sprocketBatch := sprocketsTable.Batch("UserID").Get(dynamo.Keys{userID})
//
//	var table string
//	iter := widgetBatch.Merge(sprocketBatch).IterWithTable(&table)
//
//	// now we will use the table iterator to unmarshal the values into their respective types
//	var s sprocket
//	var w widget
//	var tmp map[string]types.AttributeValue
//	for iter.Next(ctx, &tmp) {
//		if table == "Widgets" {
//			err := dynamo.UnmarshalItem(tmp, &w)
//			if err != nil {
//				fmt.Println(err)
//			}
//		} else if table == "Sprockets" {
//			err := dynamo.UnmarshalItem(tmp, &s)
//			if err != nil {
//				fmt.Println(err)
//			}
//		} else {
//			fmt.Printf("Unexpected Table: %s\n", table)
//		}
//	}
//
//	if iter.Err() != nil {
//		fmt.Println(iter.Err())
//	}
func (bg *BatchGet) IterWithTable(tablePtr *string) Iter {
	return newBGIter(bg, unmarshalItem, tablePtr, bg.err)
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
		RequestItems: make(map[string]types.KeysAndAttributes),
	}

	for _, get := range bg.reqs[start:end] {
		if proj := bg.projectionFor(get.table.Name()); proj != nil {
			get.Project(proj...)
			bg.setError(get.err)
		}
	}
	if bg.cc != nil {
		in.ReturnConsumedCapacity = types.ReturnConsumedCapacityIndexes
	}

	for _, get := range bg.reqs[start:end] {
		table := get.table.Name()
		kas, ok := in.RequestItems[table]
		if !ok {
			kas = get.keysAndAttribs()
			if bg.consistent {
				kas.ConsistentRead = &bg.consistent
			}
			in.RequestItems[table] = kas
			continue
		}
		kas.Keys = append(kas.Keys, get.keys())
		in.RequestItems[table] = kas
	}
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
	track     *string // table out value
	input     *dynamodb.BatchGetItemInput
	output    *dynamodb.BatchGetItemOutput
	got       []batchGot
	err       error
	idx       int
	total     int
	processed int
	backoff   *backoff.ExponentialBackOff
	unmarshal unmarshalFunc
}

type batchGot struct {
	table string
	item  Item
}

func newBGIter(bg *BatchGet, fn unmarshalFunc, track *string, err error) *bgIter {
	if err == nil && len(bg.reqs) == 0 {
		err = ErrNoInput
	}

	iter := &bgIter{
		bg:        bg,
		track:     track,
		err:       err,
		backoff:   backoff.NewExponentialBackOff(),
		unmarshal: fn,
	}
	iter.backoff.MaxElapsedTime = 0
	return iter
}

// Next tries to unmarshal the next result into out.
// Returns false when it is complete or if it runs into an error.
func (itr *bgIter) Next(ctx context.Context, out interface{}) bool {
	// stop if we have an error
	if ctx.Err() != nil {
		itr.err = ctx.Err()
	}
	if itr.err != nil {
		return false
	}

redo:
	// can we use results we already have?
	if itr.output != nil && itr.idx < len(itr.got) {
		got := itr.got[itr.idx]
		itr.err = itr.unmarshal(got.item, out)
		itr.idx++
		itr.total++
		itr.trackTable(got.table)
		return itr.err == nil
	}

	// new bg
	if itr.input == nil {
		itr.input = itr.bg.input(itr.processed)
	}

	if itr.output != nil && itr.idx >= len(itr.got) {
		for _, req := range itr.input.RequestItems {
			itr.processed += len(req.Keys)
		}
		if itr.output.UnprocessedKeys != nil {
			for _, keys := range itr.output.UnprocessedKeys {
				itr.processed -= len(keys.Keys)
			}
		}
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
			if err := time.SleepWithContext(ctx, itr.backoff.NextBackOff()); err != nil {
				// timed out
				itr.err = err
				return false
			}
		}
		itr.idx = 0
	}

	itr.err = itr.bg.batch.table.db.retry(ctx, func() error {
		var err error
		itr.output, err = itr.bg.batch.table.db.client.BatchGetItem(ctx, itr.input)
		itr.bg.cc.incRequests()
		return err
	})
	if itr.err != nil {
		return false
	}
	if itr.bg.cc != nil {
		for i := range itr.output.ConsumedCapacity {
			itr.bg.cc.add(&itr.output.ConsumedCapacity[i])
		}
	}

	itr.got = itr.got[:0]
	for table, resp := range itr.output.Responses {
		for _, item := range resp {
			itr.got = append(itr.got, batchGot{
				table: table,
				item:  item,
			})
		}
	}

	// we've got unprocessed results, marshal one
	goto redo
}

func (itr *bgIter) trackTable(next string) {
	if itr.track == nil {
		return
	}
	*itr.track = next
}

// Err returns the error encountered, if any.
// You should check this after Next is finished.
func (itr *bgIter) Err() error {
	return itr.err
}
