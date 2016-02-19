package dynamo

import (
	"math"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/cenkalti/backoff"
)

// DynamoDB API limit, 25 operations per request
const maxWriteOps = 25

// BatchWrite is a BatchWriteItem operation.
type BatchWrite struct {
	batch Batch
	ops   []*dynamodb.WriteRequest
	err   error
}

// Write creates a new batch write request, to which
// puts and deletes can be added.
func (b Batch) Write() *BatchWrite {
	return &BatchWrite{
		batch: b,
		err:   b.err,
	}
}

// Put adds put operations for items to this batch.
func (bw *BatchWrite) Put(items ...interface{}) *BatchWrite {
	for _, item := range items {
		encoded, err := marshalItem(item)
		bw.setError(err)
		bw.ops = append(bw.ops, &dynamodb.WriteRequest{PutRequest: &dynamodb.PutRequest{
			Item: encoded,
		}})
	}
	return bw
}

// Delete adds delete operations for the given keys to this batch.
func (bw *BatchWrite) Delete(keys ...Keyed) *BatchWrite {
	for _, key := range keys {
		del := bw.batch.table.Delete(bw.batch.hashKey, key.HashKey())
		if rk := key.RangeKey(); bw.batch.rangeKey != "" && rk != nil {
			del.Range(bw.batch.rangeKey, rk)
			bw.setError(del.err)
		}
		bw.ops = append(bw.ops, &dynamodb.WriteRequest{DeleteRequest: &dynamodb.DeleteRequest{
			Key: del.key(),
		}})
	}
	return bw
}

// Run executes this batch.
// For batches with more than 25 operations, an error could indicate that
// some records have been written and some have not. Consult the wrote
// return amount to figure out which operations have succeeded.
func (bw *BatchWrite) Run() (wrote int, err error) {
	if bw.err != nil {
		return 0, bw.err
	}

	// TODO: this could be made to be more efficient,
	// by combining unprocessed items with the next request.

	boff := backoff.NewExponentialBackOff()
	boff.MaxElapsedTime = 0
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
			err := retry(func() error {
				var err error
				res, err = bw.batch.table.db.client.BatchWriteItem(req)
				return err
			})
			if err != nil {
				return wrote, err
			}

			unprocessed := res.UnprocessedItems[bw.batch.table.Name()]
			wrote += len(ops) - len(unprocessed)
			if len(unprocessed) == 0 {
				break
			}
			ops = unprocessed
			// need to sleep when re-requesting, per spec
			time.Sleep(boff.NextBackOff())
		}
	}

	return wrote, nil
}

func (bw *BatchWrite) input(ops []*dynamodb.WriteRequest) *dynamodb.BatchWriteItemInput {
	return &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			bw.batch.table.Name(): ops,
		},
	}
}

func (bw *BatchWrite) setError(err error) {
	if bw.err == nil {
		bw.err = err
	}
}
