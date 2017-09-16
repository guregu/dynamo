package dynamo

import (
	"errors"

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
	createIdx []Index
	deleteIdx []string
	ads       []*dynamodb.AttributeDefinition

	err error
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

// CreateIndex adds a new secondary global index.
// You must specify the index name, keys, key types, projection, and throughput.
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
	if index.Throughput.Read < 1 || index.Throughput.Write < 1 {
		ut.err = errors.New("dynamo: update table: throughput read and write must be 1 or more")
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

func (ut *UpdateTable) RunWithContext(ctx aws.Context) (Description, error) {
	if ut.err != nil {
		return Description{}, ut.err
	}

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
		TableName:            aws.String(ut.table.Name()),
		AttributeDefinitions: ut.ads,
	}

	if ut.r != 0 || ut.w != 0 {
		input.ProvisionedThroughput = &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  &ut.r,
			WriteCapacityUnits: &ut.w,
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
			IndexName: aws.String(index),
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(thru.Read),
				WriteCapacityUnits: aws.Int64(thru.Write),
			},
		}}
		input.GlobalSecondaryIndexUpdates = append(input.GlobalSecondaryIndexUpdates, up)
	}
	for _, index := range ut.createIdx {
		up := &dynamodb.GlobalSecondaryIndexUpdate{Create: createIndexAction(index)}
		input.GlobalSecondaryIndexUpdates = append(input.GlobalSecondaryIndexUpdates, up)
	}
	for _, del := range ut.deleteIdx {
		up := &dynamodb.GlobalSecondaryIndexUpdate{Delete: &dynamodb.DeleteGlobalSecondaryIndexAction{
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

	ut.ads = append(ut.ads, &dynamodb.AttributeDefinition{
		AttributeName: &name,
		AttributeType: aws.String((string)(typ)),
	})
}

func createIndexAction(index Index) *dynamodb.CreateGlobalSecondaryIndexAction {
	ks := []*dynamodb.KeySchemaElement{
		{
			AttributeName: &index.HashKey,
			KeyType:       aws.String(dynamodb.KeyTypeHash),
		},
	}
	if index.RangeKey != "" {
		ks = append(ks, &dynamodb.KeySchemaElement{
			AttributeName: &index.RangeKey,
			KeyType:       aws.String(dynamodb.KeyTypeRange),
		})
	}
	add := &dynamodb.CreateGlobalSecondaryIndexAction{
		IndexName: &index.Name,
		KeySchema: ks,
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(index.Throughput.Read),
			WriteCapacityUnits: aws.Int64(index.Throughput.Write),
		},
		Projection: &dynamodb.Projection{
			ProjectionType: aws.String((string)(index.ProjectionType)),
		},
	}
	if index.ProjectionType == IncludeProjection {
		add.Projection.NonKeyAttributes = aws.StringSlice(index.ProjectionAttribs)
	}
	return add
}
