package dynamo

import (
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/cenkalti/backoff"
)

// TODO: chunk into 25-item requests

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
func (bw *BatchWrite) Run() error {
	if bw.err != nil {
		return bw.err
	}
	boff := backoff.NewExponentialBackOff()
	boff.MaxElapsedTime = 0

	loopSize := len(bw.ops) / 25

	i := 0

	for i < loopSize+1 {
		var res *dynamodb.BatchWriteItemOutput
		start := i * 25
		end := start + 25

		if i == loopSize {
			end = len(bw.ops)
		}

		req := bw.input(start, end)

		err := retry(func() error {
			var err error
			res, err = bw.batch.table.db.client.BatchWriteItem(req)
			return err
		})
		if err != nil {
			return err
		}

		time.Sleep(boff.NextBackOff())
		i++
	}

	return nil
}

func (bw *BatchWrite) input(start int, end int) *dynamodb.BatchWriteItemInput {
	return &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			bw.batch.table.Name(): bw.ops[start:end],
		},
	}
}

func (bw *BatchWrite) setError(err error) {
	if bw.err == nil {
		bw.err = err
	}
}
