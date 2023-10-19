// Package dynamo offers a rich DynamoDB client.
package dynamo

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// DB is a DynamoDB client.
type DB struct {
	client   dynamodbiface.DynamoDBAPI
	logger   aws.Logger
	retryer  request.Retryer
	retryMax int
}

// New creates a new client with the given configuration.
// If Retryer is configured, retrying responsibility will be delegated to it.
// If MaxRetries is configured, the maximum number of retry attempts will be limited to the specified value
// (0 for no retrying, -1 for default behavior of unlimited retries).
// MaxRetries is ignored if Retryer is set.
func New(p client.ConfigProvider, cfgs ...*aws.Config) *DB {
	cfg := p.ClientConfig(dynamodb.EndpointsID, cfgs...)
	return newDB(dynamodb.New(p, cfgs...), cfg.Config)
}

// NewFromIface creates a new client with the given interface.
func NewFromIface(client dynamodbiface.DynamoDBAPI) *DB {
	if c, ok := client.(*dynamodb.DynamoDB); ok {
		return newDB(c, &c.Config)
	}
	return newDB(client, &aws.Config{})
}

func newDB(client dynamodbiface.DynamoDBAPI, cfg *aws.Config) *DB {
	db := &DB{
		client:   client,
		logger:   cfg.Logger,
		retryMax: -1,
	}

	if db.logger == nil {
		db.logger = aws.NewDefaultLogger()
	}

	if retryer, ok := cfg.Retryer.(request.Retryer); ok {
		db.retryer = retryer
	} else if cfg.MaxRetries != nil {
		db.retryMax = *cfg.MaxRetries
	}

	return db
}

// Client returns this DB's internal client used to make API requests.
func (db *DB) Client() dynamodbiface.DynamoDBAPI {
	return db.client
}

// TODO: should we expose these, or come up with a better interface?
// They could be useful in conjunction with NewFromIface, but SetRetryer would be misleading;
// dynamo expects it to be called from within the dynamodbapi interface.
// Probably best to create a forward-compatible (v2-friendly) configuration API instead.

// func (db *DB) SetRetryer(retryer request.Retryer) {
// 	db.retryer = retryer
// }

// func (db *DB) SetMaxRetries(max int) *DB {
// 	db.retryMax = max
// 	return db
// }

// func (db *DB) SetLogger(logger aws.Logger) *DB {
// 	if logger == nil {
// 		db.logger = noopLogger{}
// 		return db
// 	}
// 	db.logger = logger
// 	return db
// }

func (db *DB) log(v ...interface{}) {
	db.logger.Log(v...)
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
func (lt *ListTables) All() ([]string, error) {
	ctx, cancel := defaultContext()
	defer cancel()
	return lt.AllWithContext(ctx)
}

// AllWithContext returns every table or an error.
func (lt *ListTables) AllWithContext(ctx context.Context) ([]string, error) {
	var tables []string
	itr := lt.Iter()
	var name string
	for itr.NextWithContext(ctx, &name) {
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

func (itr *ltIter) Next(out interface{}) bool {
	ctx, cancel := defaultContext()
	defer cancel()
	return itr.NextWithContext(ctx, out)
}

func (itr *ltIter) NextWithContext(ctx context.Context, out interface{}) bool {
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
			*out.(*string) = *itr.result.TableNames[itr.idx]
			itr.idx++
			return true
		}

		// no more tables
		if itr.result.LastEvaluatedTableName == nil {
			return false
		}
	}

	itr.err = itr.lt.db.retry(ctx, func() error {
		res, err := itr.lt.db.client.ListTablesWithContext(ctx, itr.input())
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

	*out.(*string) = *itr.result.TableNames[0]
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
	Next(out interface{}) bool
	// NextWithContext tries to unmarshal the next result into out.
	// Returns false when it is complete or if it runs into an error.
	NextWithContext(ctx context.Context, out interface{}) bool
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
	LastEvaluatedKey() PagingKey
}

// PagingIter is an iterator of combined request results from multiple iterators running in parallel.
type ParallelIter interface {
	Iter
	// LastEvaluatedKeys returns each parallel segment's last evaluated key in order of segment number.
	// The slice will be the same size as the number of segments, and the keys can be nil.
	LastEvaluatedKeys() []PagingKey
}

// PagingKey is a key used for splitting up partial results.
// Get a PagingKey from a PagingIter and pass it to StartFrom in Query or Scan.
type PagingKey map[string]*dynamodb.AttributeValue

// IsCondCheckFailed returns true if the given error is a "conditional check failed" error.
// This corresponds with a ConditionalCheckFailedException in most APIs,
// or a TransactionCanceledException with a ConditionalCheckFailed cancellation reason in transactions.
func IsCondCheckFailed(err error) bool {
	var txe *dynamodb.TransactionCanceledException
	if errors.As(err, &txe) {
		for _, cr := range txe.CancellationReasons {
			if cr.Code != nil && *cr.Code == "ConditionalCheckFailed" {
				return true
			}
		}
		return false
	}

	var ae awserr.Error
	if errors.As(err, &ae) && ae.Code() == "ConditionalCheckFailedException" {
		return true
	}

	return false
}

// type noopLogger struct{}

// func (noopLogger) Log(...interface{}) {}
