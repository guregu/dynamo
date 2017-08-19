package dynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Status is an enumeration of table and index statuses.
type Status string

const (
	// The table or index is ready for use.
	ActiveStatus Status = "ACTIVE"
	// The table or index is being created.
	CreatingStatus = "CREATING"
	// The table or index is being updated.
	UpdatingStatus = "UPDATING"
	// The table or index is being deleted.
	DeletingStatus = "DELETING"
)

// Table is a DynamoDB table.
type Table struct {
	name string
	db   *DB
}

// Table returns a Table handle specified by name.
func (db *DB) Table(name string) Table {
	return Table{
		name: name,
		db:   db,
	}
}

// Name returns this table's name.
func (table Table) Name() string {
	return table.name
}

// DeleteTable is a request to delete a table.
// See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteTable.html
type DeleteTable struct {
	table Table
}

// DeleteTable begins a new request to delete this table.
func (table Table) DeleteTable() *DeleteTable {
	return &DeleteTable{table: table}
}

// Run executes this request and deletes the table.
func (dt *DeleteTable) Run() error {
	ctx, cancel := defaultContext()
	defer cancel()
	return dt.RunWithContext(ctx)
}

// RunWithContext executes this request and deletes the table.
func (dt *DeleteTable) RunWithContext(ctx aws.Context) error {
	input := dt.input()
	return retry(ctx, func() error {
		_, err := dt.table.db.client.DeleteTableWithContext(ctx, input)
		return err
	})
}

func (dt *DeleteTable) input() *dynamodb.DeleteTableInput {
	name := dt.table.Name()
	return &dynamodb.DeleteTableInput{
		TableName: &name,
	}
}

/*
	Update table througput
*/

type UpdateTable struct {
	table Table
}

type UpdateTableDescription struct {
	dynamodb.UpdateTableOutput
}

func (table Table) UpdateTable() *UpdateTable {
	return &UpdateTable{table: table}
}

// Run executes this request and describe the table.
func (dt *UpdateTable) UpdateThroughput(r int64, w int64) (UpdateTableDescription, error) {
	ctx, cancel := defaultContext()
	defer cancel()
	return dt.RunWithContext(ctx, r, w)
}

func (dt *UpdateTable) RunWithContext(ctx aws.Context, r int64, w int64) (UpdateTableDescription, error) {
	input := dt.input(r, w)
	result, err := dt.table.db.client.UpdateTableWithContext(ctx, input)

	var tableDescription = UpdateTableDescription{}
	// copier.Copy(&tableDescription, &result)

	return tableDescription, err
}

func (dt *UpdateTable) input(r int64, w int64) *dynamodb.UpdateTableInput {
	name := dt.table.Name()

	return &dynamodb.UpdateTableInput{
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(r),
			WriteCapacityUnits: aws.Int64(w),
		},
		TableName: &name,
	}
}
