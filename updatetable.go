package dynamo

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// UpdateTable is a request to change a table's settings.
// See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateTable.html
type UpdateTable struct {
	table Table
	r, w  int64 // throughput

	billingMode types.BillingMode

	disableStream bool
	streamView    StreamView

	updateIdx map[string]Throughput
	createIdx []Index
	deleteIdx []string
	ads       []types.AttributeDefinition

	err error
}

// UpdateTable makes changes to this table's settings.
func (table Table) UpdateTable() *UpdateTable {
	return &UpdateTable{
		table:     table,
		updateIdx: make(map[string]Throughput),
	}
}

// OnDemand sets this table to use on-demand (pay per request) billing mode if enabled is true.
// If enabled is false, this table will be changed to provisioned billing mode.
func (ut *UpdateTable) OnDemand(enabled bool) *UpdateTable {
	if enabled {
		ut.billingMode = types.BillingModePayPerRequest
	} else {
		ut.billingMode = types.BillingModeProvisioned
	}
	return ut
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

// CreateIndex adds a new secondary global index.
// You must specify the index name, keys, key types, projection.
// If this table is not on-demand you must also specify throughput.
func (ut *UpdateTable) CreateIndex(index Index) *UpdateTable {
	if index.Name == "" {
		ut.err = errors.New("dynamo: update table: missing index name")
	}
	if index.HashKey == "" {
		ut.err = errors.New("dynamo: update table: missing hash key")
	}
	if index.HashKeyType == "" {
		ut.err = errors.New("dynamo: update table: missing hash key type")
	}
	if index.RangeKey != "" && index.RangeKeyType == "" {
		ut.err = errors.New("dynamo: update table: missing range key type")
	}
	if index.ProjectionType == "" {
		ut.err = errors.New("dynamo: update table: missing projection type")
	}

	ut.addAD(index.HashKey, index.HashKeyType)
	if index.RangeKey != "" {
		ut.addAD(index.RangeKey, index.RangeKeyType)
	}

	ut.createIdx = append(ut.createIdx, index)
	return ut
}

// DeleteIndex deletes the specified index.
func (ut *UpdateTable) DeleteIndex(name string) *UpdateTable {
	ut.deleteIdx = append(ut.deleteIdx, name)
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

func (ut *UpdateTable) RunWithContext(ctx context.Context) (Description, error) {
	if ut.err != nil {
		return Description{}, ut.err
	}

	input := ut.input()

	var result *dynamodb.UpdateTableOutput
	err := retry(ctx, func() error {
		var err error
		result, err = ut.table.db.client.UpdateTable(ctx, input)
		return err
	})
	if err != nil {
		return Description{}, err
	}

	return newDescription(result.TableDescription), nil
}

func (ut *UpdateTable) input() *dynamodb.UpdateTableInput {
	input := &dynamodb.UpdateTableInput{
		TableName:            aws.String(ut.table.Name()),
		AttributeDefinitions: ut.ads,
		BillingMode:          ut.billingMode,
	}

	if ut.r != 0 || ut.w != 0 {
		input.ProvisionedThroughput = &types.ProvisionedThroughput{
			ReadCapacityUnits:  &ut.r,
			WriteCapacityUnits: &ut.w,
		}
	}

	if ut.disableStream {
		input.StreamSpecification = &types.StreamSpecification{
			StreamEnabled: aws.Bool(false),
		}
	} else if ut.streamView != "" {
		input.StreamSpecification = &types.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: types.StreamViewType(ut.streamView),
		}
	}

	for index, thru := range ut.updateIdx {
		up := types.GlobalSecondaryIndexUpdate{Update: &types.UpdateGlobalSecondaryIndexAction{
			IndexName: aws.String(index),
			ProvisionedThroughput: &types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(thru.Read),
				WriteCapacityUnits: aws.Int64(thru.Write),
			},
		}}
		input.GlobalSecondaryIndexUpdates = append(input.GlobalSecondaryIndexUpdates, up)
	}
	for _, index := range ut.createIdx {
		up := types.GlobalSecondaryIndexUpdate{Create: createIndexAction(index)}
		input.GlobalSecondaryIndexUpdates = append(input.GlobalSecondaryIndexUpdates, up)
	}
	for _, del := range ut.deleteIdx {
		up := types.GlobalSecondaryIndexUpdate{Delete: &types.DeleteGlobalSecondaryIndexAction{
			IndexName: aws.String(del),
		}}
		input.GlobalSecondaryIndexUpdates = append(input.GlobalSecondaryIndexUpdates, up)
	}
	return input
}

func (ut *UpdateTable) addAD(name string, typ KeyType) {
	for _, ad := range ut.ads {
		if *ad.AttributeName == name {
			return
		}
	}

	ut.ads = append(ut.ads, types.AttributeDefinition{
		AttributeName: &name,
		AttributeType: types.ScalarAttributeType(typ),
	})
}

func createIndexAction(index Index) *types.CreateGlobalSecondaryIndexAction {
	ks := []types.KeySchemaElement{
		{
			AttributeName: &index.HashKey,
			KeyType:       types.KeyTypeHash,
		},
	}
	if index.RangeKey != "" {
		ks = append(ks, types.KeySchemaElement{
			AttributeName: &index.RangeKey,
			KeyType:       types.KeyTypeRange,
		})
	}
	add := &types.CreateGlobalSecondaryIndexAction{
		IndexName: &index.Name,
		KeySchema: ks,
		Projection: &types.Projection{
			ProjectionType: types.ProjectionType(index.ProjectionType),
		},
	}
	if index.Throughput.Read > 0 && index.Throughput.Write > 0 {
		add.ProvisionedThroughput = &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(index.Throughput.Read),
			WriteCapacityUnits: aws.Int64(index.Throughput.Write),
		}
	}
	if index.ProjectionType == IncludeProjection {
		add.Projection.NonKeyAttributes = index.ProjectionAttribs
	}
	return add
}
