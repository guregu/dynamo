package dynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// UpdateTable is a request to change a table's settings.
// See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateTable.html
type UpdateTable struct {
	table Table
	r, w  int64 // throughput

	disableStream bool
	streamView    StreamView

	updateIdx map[string]Throughput
}

// UpdateTable makes changes to this table's settings.
func (table Table) UpdateTable() *UpdateTable {
	return &UpdateTable{
		table:     table,
		updateIdx: make(map[string]Throughput),
	}
}

// Provision sets this table's read and write throughput capacity.
func (ut *UpdateTable) Provision(read, write int64) *UpdateTable {
	ut.r, ut.w = read, write
	return ut
}

// ProvisionIndex updates a global secondary index's read and write throughput capacity.
func (ut *UpdateTable) ProvisionIndex(name string, read, write int64) *UpdateTable {
	ut.updateIdx[name] = Throughput{Read: read, Write: write}
	return ut
}

// Stream enables streaming and sets the stream view type.
func (ut *UpdateTable) Stream(view StreamView) *UpdateTable {
	ut.streamView = view
	return ut
}

// DisableStream disables this table's stream.
func (ut *UpdateTable) DisableStream() *UpdateTable {
	ut.disableStream = true
	return ut
}

// Run executes this request and describes the table.
func (ut *UpdateTable) Run() (Description, error) {
	ctx, cancel := defaultContext()
	defer cancel()
	return ut.RunWithContext(ctx)
}

func (ut *UpdateTable) RunWithContext(ctx aws.Context) (Description, error) {
	input := ut.input()

	var result *dynamodb.UpdateTableOutput
	err := retry(ctx, func() error {
		var err error
		result, err = ut.table.db.client.UpdateTableWithContext(ctx, input)
		return err
	})
	if err != nil {
		return Description{}, err
	}

	return newDescription(result.TableDescription), nil
}

func (ut *UpdateTable) input() *dynamodb.UpdateTableInput {
	input := &dynamodb.UpdateTableInput{
		TableName: aws.String(ut.table.Name()),
	}

	if ut.r != 0 || ut.w != 0 {
		input.ProvisionedThroughput = &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(ut.r),
			WriteCapacityUnits: aws.Int64(ut.w),
		}
	}

	if ut.disableStream {
		input.StreamSpecification = &dynamodb.StreamSpecification{
			StreamEnabled: aws.Bool(false),
		}
	} else if ut.streamView != "" {
		input.StreamSpecification = &dynamodb.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: aws.String((string)(ut.streamView)),
		}
	}

	for index, thru := range ut.updateIdx {
		up := &dynamodb.GlobalSecondaryIndexUpdate{Update: &dynamodb.UpdateGlobalSecondaryIndexAction{
			IndexName: &index,
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  &thru.Read,
				WriteCapacityUnits: &thru.Write,
			},
		}}
		input.GlobalSecondaryIndexUpdates = append(input.GlobalSecondaryIndexUpdates, up)
	}

	return input
}
