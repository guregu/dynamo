package dynamo

import (
	"context"
	"math"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/time"
	"github.com/cenkalti/backoff/v4"
)

// DynamoDB API limit, 25 operations per request
const maxWriteOps = 25

// BatchWrite is a BatchWriteItem operation.
type BatchWrite struct {
	batch Batch
	ops   []batchWrite
	err   error
	cc    *ConsumedCapacity
}

type batchWrite struct {
	table string
	op    types.WriteRequest
}

// Write creates a new batch write request, to which
// puts and deletes can be added.
func (b Batch) Write() *BatchWrite {
	return &BatchWrite{
		batch: b,
		err:   b.err,
	}
}

// Put adds put operations for items to this batch using the default table.
func (bw *BatchWrite) Put(items ...interface{}) *BatchWrite {
	return bw.PutIn(bw.batch.table, items...)
}

// PutIn adds put operations for items to this batch using the given table.
// This can be useful for writing to multiple different tables.
func (bw *BatchWrite) PutIn(table Table, items ...interface{}) *BatchWrite {
	name := table.Name()
	for _, item := range items {
		encoded, err := marshalItem(item)
		bw.setError(err)
		bw.ops = append(bw.ops, batchWrite{
			table: name,
			op: types.WriteRequest{PutRequest: &types.PutRequest{
				Item: encoded,
			}},
		})
	}
	return bw
}

// Delete adds delete operations for the given keys to this batch, using the default table.
func (bw *BatchWrite) Delete(keys ...Keyed) *BatchWrite {
	return bw.deleteIn(bw.batch.table, bw.batch.hashKey, bw.batch.rangeKey, keys...)
}

// DeleteIn adds delete operations for the given keys to this batch, using the given table.
// hashKey must be the name of the primary key hash (partition) attribute.
// This function is for tables with a hash key (partition key) only.
// For tables including a range key (sort key) primary key, use [BatchWrite.DeleteInRange] instead.
func (bw *BatchWrite) DeleteIn(table Table, hashKey string, keys ...Keyed) *BatchWrite {
	return bw.deleteIn(table, hashKey, "", keys...)
}

// DeleteInRange adds delete operations for the given keys to this batch, using the given table.
// hashKey must be the name of the primary key hash (parition) attribute, rangeKey must be the name of the primary key range (sort) attribute.
// This function is for tables with a hash key (partition key) and range key (sort key).
// For tables without a range key primary key, use [BatchWrite.DeleteIn] instead.
func (bw *BatchWrite) DeleteInRange(table Table, hashKey, rangeKey string, keys ...Keyed) *BatchWrite {
	return bw.deleteIn(table, hashKey, rangeKey, keys...)
}

func (bw *BatchWrite) deleteIn(table Table, hashKey, rangeKey string, keys ...Keyed) *BatchWrite {
	name := table.Name()
	for _, key := range keys {
		del := table.Delete(hashKey, key.HashKey())
		if rk := key.RangeKey(); rangeKey != "" && rk != nil {
			del.Range(rangeKey, rk)
			bw.setError(del.err)
		}
		bw.ops = append(bw.ops, batchWrite{
			table: name,
			op: types.WriteRequest{DeleteRequest: &types.DeleteRequest{
				Key: del.key(),
			}},
		})
	}
	return bw
}

// Merge copies operations from src to this batch.
func (bw *BatchWrite) Merge(srcs ...*BatchWrite) *BatchWrite {
	for _, src := range srcs {
		bw.ops = append(bw.ops, src.ops...)
	}
	return bw
}

// ConsumedCapacity will measure the throughput capacity consumed by this operation and add it to cc.
func (bw *BatchWrite) ConsumedCapacity(cc *ConsumedCapacity) *BatchWrite {
	bw.cc = cc
	return bw
}

// Run executes this batch.
// For batches with more than 25 operations, an error could indicate that
// some records have been written and some have not. Consult the wrote
// return amount to figure out which operations have succeeded.
func (bw *BatchWrite) Run(ctx context.Context) (wrote int, err error) {
	if bw.err != nil {
		return 0, bw.err
	}
	if len(bw.ops) == 0 {
		return 0, ErrNoInput
	}

	// TODO: this could be made to be more efficient,
	// by combining unprocessed items with the next request.

	boff := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
	batches := int(math.Ceil(float64(len(bw.ops)) / maxWriteOps))
	for i := 0; i < batches; i++ {
		start, end := i*maxWriteOps, (i+1)*maxWriteOps
		if end > len(bw.ops) {
			end = len(bw.ops)
		}
		ops := bw.ops[start:end]
		for {
			var res *dynamodb.BatchWriteItemOutput
			req := bw.input(ops)
			err := bw.batch.table.db.retry(ctx, func() error {
				var err error
				res, err = bw.batch.table.db.client.BatchWriteItem(ctx, req)
				bw.cc.incRequests()
				return err
			})
			if err != nil {
				return wrote, err
			}
			if bw.cc != nil {
				for i := range res.ConsumedCapacity {
					bw.cc.add(&res.ConsumedCapacity[i])
				}
			}

			wrote += len(ops)
			if len(res.UnprocessedItems) == 0 {
				break
			}

			ops = ops[:0]
			for tableName, unprocessed := range res.UnprocessedItems {
				wrote -= len(unprocessed)
				for _, op := range unprocessed {
					ops = append(ops, batchWrite{
						table: tableName,
						op:    op,
					})
				}
			}

			// need to sleep when re-requesting, per spec
			if err := time.SleepWithContext(ctx, boff.NextBackOff()); err != nil {
				// timed out
				return wrote, err
			}
		}
	}

	return wrote, nil
}

func (bw *BatchWrite) input(ops []batchWrite) *dynamodb.BatchWriteItemInput {
	items := make(map[string][]types.WriteRequest)
	for _, op := range ops {
		items[op.table] = append(items[op.table], op.op)
	}
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: items,
	}
	if bw.cc != nil {
		input.ReturnConsumedCapacity = types.ReturnConsumedCapacityIndexes
	}
	return input
}

func (bw *BatchWrite) setError(err error) {
	if bw.err == nil {
		bw.err = err
	}
}
