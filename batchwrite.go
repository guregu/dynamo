package dynamo

import (
	"errors"
	"math"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/cenkalti/backoff"
	multierror "github.com/hashicorp/go-multierror"
)

// DynamoDB API limit, 25 operations per request
const maxWriteOps = 25

// BatchWrite is a BatchWriteItem operation.
type BatchWrite struct {
	batch Batch
	ops   []*dynamodb.WriteRequest
	err   error
	cc    *ConsumedCapacity
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

// ConsumedCapacity will measure the throughput capacity consumed by this operation and add it to cc.
func (bw *BatchWrite) ConsumedCapacity(cc *ConsumedCapacity) *BatchWrite {
	bw.cc = cc
	return bw
}

// Structure passed to the concurrent batch write operation
type batchRequest struct {
	ctx aws.Context
	ops []*dynamodb.WriteRequest
}

// Structure returned after a concurrent batch operation
type batchResponse struct {
	Result *dynamodb.BatchWriteItemOutput
	Error  error
	Wrote  int
}

// Config used when calling RunConcurrently
type batchWriteConfig struct {
	poolSize int
}

// Parameter type to be passed to RunConcurrently
type BatchWriteOption func(*batchWriteConfig)

// Sets the default config
func defaults(cfg *batchWriteConfig) {
	cfg.poolSize = 10
}

// Sets the pool size to process the request
func WithPoolSize(poolSize int) BatchWriteOption {
	return func(cfg *batchWriteConfig) {
		cfg.poolSize = poolSize
	}
}

func (bw *BatchWrite) writeBatch(ctx aws.Context, ops []*dynamodb.WriteRequest) batchResponse {

	boff := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
	wrote := 0

	for {
		var res *dynamodb.BatchWriteItemOutput
		req := bw.input(ops)
		err := retry(ctx, func() error {
			var err error
			res, err = bw.batch.table.db.client.BatchWriteItemWithContext(ctx, req)
			return err
		})
		if err != nil {
			return batchResponse{
				Result: res,
				Error:  err,
				Wrote:  0,
			}
		}
		if bw.cc != nil {
			for _, cc := range res.ConsumedCapacity {
				addConsumedCapacity(bw.cc, cc)
			}
		}

		unprocessed := res.UnprocessedItems[bw.batch.table.Name()]
		wrote = len(ops) - len(unprocessed)
		if len(unprocessed) == 0 {
			return batchResponse{
				Result: res,
				Error:  err,
				Wrote:  wrote,
			}
		}
		ops = unprocessed

		// need to sleep when re-requesting, per spec
		if err := aws.SleepWithContext(ctx, boff.NextBackOff()); err != nil {
			return batchResponse{
				Result: nil,
				Error:  err,
				Wrote:  wrote,
			}
		}
	}
}

func (bw *BatchWrite) writeBatchWorker(worker int, requests <-chan batchRequest, response chan<- batchResponse) {
	for request := range requests {
		response <- bw.writeBatch(request.ctx, request.ops)
	}
}

func splitBatches(requests []*dynamodb.WriteRequest) (batches [][]*dynamodb.WriteRequest) {
	batches = [][]*dynamodb.WriteRequest{}
	requestsLength := len(requests)
	for i := 0; i < requestsLength; i += maxWriteOps {
		end := i + maxWriteOps
		if end > requestsLength {
			end = requestsLength
		}
		batches = append(batches, requests[i:end])
	}
	return batches
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

// RunConcurrently executes this batch concurrently with the pool size specified.
// By default, the pool size is 10
func (bw *BatchWrite) RunConcurrently(opts ...BatchWriteOption) (wrote int, err error) {
	ctx, cancel := defaultContext()
	defer cancel()
	return bw.RunConcurrentlyWithContext(ctx, opts...)
}

func (bw *BatchWrite) RunConcurrentlyWithContext(ctx aws.Context, opts ...BatchWriteOption) (wrote int, err error) {

	if bw.err != nil {
		return 0, bw.err
	}

	cfg := new(batchWriteConfig)
	defaults(cfg)
	for _, fn := range opts {
		fn(cfg)
	}

	// TODO : Can split the batches and run them concurrently ?
	batches := splitBatches(bw.ops)
	totalBatches := len(batches)

	requests := make(chan batchRequest, totalBatches)
	response := make(chan batchResponse, totalBatches)
	defer close(response)

	// Create the workers
	for i := 0; i < cfg.poolSize; i++ {
		go bw.writeBatchWorker(i, requests, response)
	}

	// Push the write requests
	for i := 0; i < totalBatches; i++ {
		requests <- batchRequest{
			ctx: ctx,
			ops: batches[i],
		}
	}
	close(requests)

	// Capture the response
	wrote = 0
	batchCounter := 0
	for {
		select {
		case batchResponse, ok := <-response:
			if !ok {
				err = multierror.Append(err, errors.New("channel unexpectedly closed"))
				return wrote, err
			}
			if batchResponse.Error != nil {
				err = multierror.Append(err, batchResponse.Error)
			}
			wrote += batchResponse.Wrote
			batchCounter++
			if batchCounter == totalBatches {
				return wrote, err
			}
		case <-ctx.Done():
			err = multierror.Append(err, ctx.Err())
			return wrote, err
		}
	}
}

// Run executes this batch.
// For batches with more than 25 operations, an error could indicate that
// some records have been written and some have not. Consult the wrote
// return amount to figure out which operations have succeeded.
func (bw *BatchWrite) Run() (wrote int, err error) {
	ctx, cancel := defaultContext()
	defer cancel()
	// TODO : Perhaps use RunConcurrentlyWithContext(dynamo.WithPoolSize(1)) instead ?
	return bw.RunWithContext(ctx)
}

func (bw *BatchWrite) RunWithContext(ctx aws.Context) (wrote int, err error) {
	if bw.err != nil {
		return 0, bw.err
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
			err := retry(ctx, func() error {
				var err error
				res, err = bw.batch.table.db.client.BatchWriteItemWithContext(ctx, req)
				return err
			})
			if err != nil {
				return wrote, err
			}
			if bw.cc != nil {
				for _, cc := range res.ConsumedCapacity {
					addConsumedCapacity(bw.cc, cc)
				}
			}

			unprocessed := res.UnprocessedItems[bw.batch.table.Name()]
			wrote += len(ops) - len(unprocessed)
			if len(unprocessed) == 0 {
				break
			}
			ops = unprocessed

			// need to sleep when re-requesting, per spec
			if err := aws.SleepWithContext(ctx, boff.NextBackOff()); err != nil {
				// timed out
				return wrote, err
			}
		}
	}

	return wrote, nil
}

func (bw *BatchWrite) input(ops []*dynamodb.WriteRequest) *dynamodb.BatchWriteItemInput {
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			bw.batch.table.Name(): ops,
		},
	}
	if bw.cc != nil {
		input.ReturnConsumedCapacity = aws.String(dynamodb.ReturnConsumedCapacityIndexes)
	}
	return input
}

func (bw *BatchWrite) setError(err error) {
	if bw.err == nil {
		bw.err = err
	}
}
