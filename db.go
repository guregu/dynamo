// Package dynamo offers a rich DynamoDB client.
package dynamo

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"

	"github.com/guregu/dynamo/v2/dynamodbiface"
)

// DB is a DynamoDB client.
type DB struct {
	client dynamodbiface.DynamoDBAPI
	// table description cache for LEK inference
	descs *sync.Map // table name → Description
}

// New creates a new client with the given configuration.
// If Retryer is configured, retrying responsibility will be delegated to it.
// If MaxRetries is configured, the maximum number of retry attempts will be limited to the specified value
// (0 for no retrying, -1 for default behavior of unlimited retries).
func New(cfg aws.Config, options ...func(*dynamodb.Options)) *DB {
	client := dynamodb.NewFromConfig(cfg, options...)
	return NewFromIface(client)
}

// NewFromIface creates a new client with the given interface.
func NewFromIface(client dynamodbiface.DynamoDBAPI) *DB {
	db := &DB{
		client: client,
		descs:  new(sync.Map),
	}
	return db
}

// Client returns this DB's internal client used to make API requests.
func (db *DB) Client() dynamodbiface.DynamoDBAPI {
	return db.client
}

func (db *DB) loadDesc(name string) (desc Description, ok bool) {
	if descv, exists := db.descs.Load(name); exists {
		desc, ok = descv.(Description)
	}
	return
}

func (db *DB) storeDesc(desc Description) {
	db.descs.Store(desc.Name, desc)
}

// ListTables is a request to list tables.
// See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ListTables.html
type ListTables struct {
	db *DB
}

// ListTables begins a new request to list all tables.
func (db *DB) ListTables() *ListTables {
	return &ListTables{db: db}
}

// All returns every table or an error.
func (lt *ListTables) All(ctx context.Context) ([]string, error) {
	var tables []string
	itr := lt.Iter()
	var name string
	for itr.Next(ctx, &name) {
		tables = append(tables, name)
	}
	return tables, itr.Err()
}

type ltIter struct {
	lt     *ListTables
	result *dynamodb.ListTablesOutput
	idx    int
	err    error
}

// Iter returns an iterator of table names.
// This iterator's Next functions will only accept type *string as their out parameter.
func (lt *ListTables) Iter() Iter {
	return &ltIter{lt: lt}
}

func (itr *ltIter) Next(ctx context.Context, out interface{}) bool {
	if ctx.Err() != nil {
		itr.err = ctx.Err()
	}
	if itr.err != nil {
		return false
	}

	if _, ok := out.(*string); !ok {
		itr.err = fmt.Errorf("dynamo: list tables: iter out must be *string, got %T", out)
		return false
	}

	if itr.result != nil {
		if itr.idx < len(itr.result.TableNames) {
			*out.(*string) = itr.result.TableNames[itr.idx]
			itr.idx++
			return true
		}

		// no more tables
		if itr.result.LastEvaluatedTableName == nil {
			return false
		}
	}

	itr.err = itr.lt.db.retry(ctx, func() error {
		res, err := itr.lt.db.client.ListTables(ctx, itr.input())
		if err != nil {
			return err
		}
		itr.result = res
		return nil
	})
	if itr.err != nil {
		return false
	}

	if len(itr.result.TableNames) == 0 {
		return false
	}

	*out.(*string) = itr.result.TableNames[0]
	itr.idx = 1
	return true
}

func (itr *ltIter) Err() error {
	return itr.err
}

func (itr *ltIter) input() *dynamodb.ListTablesInput {
	input := &dynamodb.ListTablesInput{}
	if itr.result != nil {
		input.ExclusiveStartTableName = itr.result.LastEvaluatedTableName
	}
	return input
}

// Iter is an iterator for request results.
type Iter interface {
	// Next tries to unmarshal the next result into out.
	// Returns false when it is complete or if it runs into an error.
	Next(ctx context.Context, out interface{}) bool
	// Err returns the error encountered, if any.
	// You should check this after Next is finished.
	Err() error
}

// PagingIter is an iterator of request results that can also return a key
// used for splitting results.
type PagingIter interface {
	Iter
	// LastEvaluatedKey returns a key that can be passed to StartFrom in Query or Scan.
	// Combined with SearchLimit, it is useful for paginating partial results.
	LastEvaluatedKey(context.Context) (PagingKey, error)
}

// PagingIter is an iterator of combined request results from multiple iterators running in parallel.
type ParallelIter interface {
	Iter
	// LastEvaluatedKeys returns each parallel segment's last evaluated key in order of segment number.
	// The slice will be the same size as the number of segments, and the keys can be nil.
	LastEvaluatedKeys(context.Context) ([]PagingKey, error)
}

// PagingKey is a key used for splitting up partial results.
// Get a PagingKey from a PagingIter and pass it to StartFrom in Query or Scan.
type PagingKey Item

// IsCondCheckFailed returns true if the given error is a "conditional check failed" error.
// This corresponds with a ConditionalCheckFailedException in most APIs,
// or a TransactionCanceledException with a ConditionalCheckFailed cancellation reason in transactions.
func IsCondCheckFailed(err error) bool {
	var txe *types.TransactionCanceledException
	if errors.As(err, &txe) {
		for _, cr := range txe.CancellationReasons {
			if cr.Code != nil && *cr.Code == "ConditionalCheckFailed" {
				return true
			}
		}
		return false
	}

	var ae smithy.APIError
	if errors.As(err, &ae) && ae.ErrorCode() == "ConditionalCheckFailedException" {
		return true
	}

	return false
}

// Unmarshals an item from a ConditionalCheckFailedException into `out`, with the same behavior as [UnmarshalItem].
// The return value boolean `match` will be true if condCheckErr is a ConditionalCheckFailedException,
// otherwise false if it is nil or a different error.
func UnmarshalItemFromCondCheckFailed(condCheckErr error, out any) (match bool, err error) {
	if condCheckErr == nil {
		return false, nil
	}
	var cfe *types.ConditionalCheckFailedException
	if errors.As(condCheckErr, &cfe) {
		if cfe.Item == nil {
			return true, fmt.Errorf("dynamo: ConditionalCheckFailedException does not contain item (is IncludeItemInCondCheckFail disabled?): %w", condCheckErr)
		}
		return true, UnmarshalItem(cfe.Item, out)
	}
	return false, condCheckErr
}

// Unmarshals items from a TransactionCanceledException by appending them to `out`, which must be a pointer to a slice.
// The return value boolean `match` will be true if txCancelErr is a TransactionCanceledException with at least one ConditionalCheckFailed cancellation reason,
// otherwise false if it is nil or a different error.
func UnmarshalItemsFromTxCondCheckFailed(txCancelErr error, out any) (match bool, err error) {
	if txCancelErr == nil {
		return false, nil
	}
	unmarshal := unmarshalAppendTo(out)
	var txe *types.TransactionCanceledException
	if errors.As(txCancelErr, &txe) {
		for _, cr := range txe.CancellationReasons {
			if cr.Code != nil && *cr.Code == "ConditionalCheckFailed" {
				if cr.Item == nil {
					return true, fmt.Errorf("dynamo: TransactionCanceledException.CancellationReasons does not contain item (is IncludeItemInCondCheckFail disabled?): %w", txCancelErr)
				}
				if err = unmarshal(cr.Item, out); err != nil {
					return true, err
				}
				match = true
			}
		}
		return match, nil
	}
	return false, txCancelErr
}
