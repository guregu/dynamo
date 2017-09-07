// Package dynamo offers a rich DynamoDB client.
package dynamo

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// DB is a DynamoDB client.
type DB struct {
	Client dynamodbiface.DynamoDBAPI
}

// New creates a new client with the given configuration.
func New(p client.ConfigProvider, cfgs ...*aws.Config) *DB {
	db := &DB{
		Client: dynamodb.New(p, cfgs...),
	}
	return db
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
func (lt *ListTables) AllWithContext(ctx aws.Context) ([]string, error) {
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

func (itr *ltIter) NextWithContext(ctx aws.Context, out interface{}) bool {
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

	itr.err = retry(ctx, func() error {
		res, err := itr.lt.db.Client.ListTables(itr.input())
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
	NextWithContext(ctx aws.Context, out interface{}) bool
	// Err returns the error encountered, if any.
	// You should check this after Next is finished.
	Err() error
}
