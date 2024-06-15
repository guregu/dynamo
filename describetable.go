package dynamo

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Description contains information about a table.
type Description struct {
	Name    string
	ARN     string
	Status  Status
	Created time.Time

	// Attribute name of the hash key (a.k.a. partition key).
	HashKey     string
	HashKeyType KeyType
	// Attribute name of the range key (a.k.a. sort key) or blank if nonexistant.
	RangeKey     string
	RangeKeyType KeyType

	// Provisioned throughput for this table.
	Throughput Throughput
	// OnDemand is true if on-demand (pay per request) billing mode is enabled.
	OnDemand bool

	// The number of items of the table, updated every 6 hours.
	Items int64
	// The size of this table in bytes, updated every 6 hours.
	Size int64

	// Global secondary indexes.
	GSI []Index
	// Local secondary indexes.
	LSI []Index

	StreamEnabled     bool
	StreamView        StreamView
	LatestStreamARN   string
	LatestStreamLabel string

	SSEDescription SSEDescription
}

func (d Description) Active() bool {
	return d.Status == ActiveStatus
}

type Throughput struct {
	// Read capacity units.
	Read int64
	// Write capacity units.
	Write int64

	// Time at which throughput was last increased for this table.
	LastInc time.Time
	// Time at which throughput was last decreased for this table.
	LastDec time.Time
	// The number of throughput decreases in this UTC calendar day.
	DecsToday int64
}

type Index struct {
	Name        string
	ARN         string
	Status      Status
	Backfilling bool // only for GSI

	// Local is true when this index is a local secondary index, otherwise it is a global secondary index.
	Local bool

	// Attribute name of the hash key (a.k.a. partition key).
	HashKey     string
	HashKeyType KeyType
	// Attribute name of the range key (a.k.a. sort key) or blank if nonexistant.
	RangeKey     string
	RangeKeyType KeyType

	// The provisioned throughput for this index.
	Throughput Throughput

	Items int64
	Size  int64

	ProjectionType IndexProjection
	// Non-key attributes for this index's projection (if ProjectionType is IncludeProjection).
	ProjectionAttribs []string
}

func newDescription(table *types.TableDescription) Description {
	desc := Description{
		Name: *table.TableName,
	}

	if table.TableArn != nil {
		desc.ARN = *table.TableArn
	}
	if table.TableStatus != "" {
		desc.Status = Status(table.TableStatus)
	}
	if table.CreationDateTime != nil {
		desc.Created = *table.CreationDateTime
	}

	desc.HashKey, desc.RangeKey = schemaKeys(table.KeySchema)
	desc.HashKeyType = lookupADType(table.AttributeDefinitions, desc.HashKey)
	desc.RangeKeyType = lookupADType(table.AttributeDefinitions, desc.RangeKey)

	if table.BillingModeSummary != nil && table.BillingModeSummary.BillingMode != "" {
		desc.OnDemand = table.BillingModeSummary.BillingMode == types.BillingModePayPerRequest
	}

	if table.ProvisionedThroughput != nil {
		desc.Throughput = newThroughput(table.ProvisionedThroughput)
	}

	if table.ItemCount != nil {
		desc.Items = *table.ItemCount
	}
	if table.TableSizeBytes != nil {
		desc.Size = *table.TableSizeBytes
	}

	for _, index := range table.GlobalSecondaryIndexes {
		idx := Index{
			Name:       *index.IndexName,
			ARN:        *index.IndexArn,
			Status:     Status(index.IndexStatus),
			Throughput: newThroughput(index.ProvisionedThroughput),
		}
		if index.Projection != nil && index.Projection.ProjectionType != "" {
			idx.ProjectionType = IndexProjection(index.Projection.ProjectionType)
			idx.ProjectionAttribs = index.Projection.NonKeyAttributes
		}
		if index.Backfilling != nil {
			idx.Backfilling = *index.Backfilling
		}
		idx.HashKey, idx.RangeKey = schemaKeys(index.KeySchema)
		idx.HashKeyType = lookupADType(table.AttributeDefinitions, idx.HashKey)
		idx.RangeKeyType = lookupADType(table.AttributeDefinitions, idx.RangeKey)
		if index.ItemCount != nil {
			idx.Items = *index.ItemCount
		}
		if index.IndexSizeBytes != nil {
			idx.Size = *index.IndexSizeBytes
		}
		desc.GSI = append(desc.GSI, idx)
	}
	for _, index := range table.LocalSecondaryIndexes {
		idx := Index{
			Status:     ActiveStatus, // local secondary index is always active (technically, it has no status)
			Local:      true,
			Throughput: desc.Throughput, // has the same throughput as the table
		}
		if index.IndexName != nil {
			idx.Name = *index.IndexName
		}
		if index.IndexArn != nil {
			idx.ARN = *index.IndexArn
		}
		if index.Projection != nil && index.Projection.ProjectionType != "" {
			idx.ProjectionType = IndexProjection(index.Projection.ProjectionType)
			idx.ProjectionAttribs = index.Projection.NonKeyAttributes
		}
		idx.HashKey, idx.RangeKey = schemaKeys(index.KeySchema)
		idx.HashKeyType = lookupADType(table.AttributeDefinitions, idx.HashKey)
		idx.RangeKeyType = lookupADType(table.AttributeDefinitions, idx.RangeKey)
		if index.ItemCount != nil {
			idx.Items = *index.ItemCount
		}
		if index.IndexSizeBytes != nil {
			idx.Size = *index.IndexSizeBytes
		}
		desc.LSI = append(desc.LSI, idx)
	}

	if table.StreamSpecification != nil {
		if table.StreamSpecification.StreamEnabled != nil {
			desc.StreamEnabled = *table.StreamSpecification.StreamEnabled
		}
		if table.StreamSpecification.StreamViewType != "" {
			desc.StreamView = StreamView(table.StreamSpecification.StreamViewType)
		}
	}
	if table.LatestStreamArn != nil {
		desc.LatestStreamARN = *table.LatestStreamArn
	}
	if table.LatestStreamLabel != nil {
		desc.LatestStreamLabel = *table.LatestStreamLabel
	}

	if table.SSEDescription != nil {
		sseDesc := SSEDescription{}
		if table.SSEDescription.InaccessibleEncryptionDateTime != nil {
			sseDesc.InaccessibleEncryptionDateTime = *table.SSEDescription.InaccessibleEncryptionDateTime
		}
		if table.SSEDescription.KMSMasterKeyArn != nil {
			sseDesc.KMSMasterKeyARN = *table.SSEDescription.KMSMasterKeyArn
		}
		if table.SSEDescription.SSEType != "" {
			sseDesc.SSEType = table.SSEDescription.SSEType
		}
		if table.SSEDescription.Status != "" {
			sseDesc.Status = table.SSEDescription.Status
		}
		desc.SSEDescription = sseDesc
	}

	return desc
}

func (desc Description) keys(index string) map[string]struct{} {
	keys := make(map[string]struct{})
	keys[desc.HashKey] = struct{}{}
	if desc.RangeKey != "" {
		keys[desc.RangeKey] = struct{}{}
	}
	if index != "" {
		for _, gsi := range desc.GSI {
			if gsi.Name == index {
				keys[gsi.HashKey] = struct{}{}
				if gsi.RangeKey != "" {
					keys[gsi.RangeKey] = struct{}{}
				}
				return keys
			}
		}
		for _, lsi := range desc.LSI {
			if lsi.Name == index {
				keys[lsi.HashKey] = struct{}{}
				if lsi.RangeKey != "" {
					keys[lsi.RangeKey] = struct{}{}
				}
				return keys
			}
		}
		return nil
	}
	return keys
}

// DescribeTable is a request for information about a table and its indexes.
// See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DescribeTable.html
type DescribeTable struct {
	table Table
}

// Describe begins a new request to describe this table.
func (table Table) Describe() *DescribeTable {
	return &DescribeTable{table: table}
}

// Run executes this request and describe the table.
func (dt *DescribeTable) Run(ctx context.Context) (Description, error) {
	input := dt.input()

	var result *dynamodb.DescribeTableOutput
	err := dt.table.db.retry(ctx, func() error {
		var err error
		result, err = dt.table.db.client.DescribeTable(ctx, input)
		return err
	})
	if err != nil {
		return Description{}, err
	}

	desc := newDescription(result.Table)
	dt.table.db.storeDesc(desc)
	return desc, nil
}

func (dt *DescribeTable) input() *dynamodb.DescribeTableInput {
	name := dt.table.Name()
	return &dynamodb.DescribeTableInput{
		TableName: &name,
	}
}

func newThroughput(td *types.ProvisionedThroughputDescription) Throughput {
	if td == nil {
		return Throughput{}
	}

	thru := Throughput{
		Read:  *td.ReadCapacityUnits,
		Write: *td.WriteCapacityUnits,
	}
	if td.LastIncreaseDateTime != nil {
		thru.LastInc = *td.LastIncreaseDateTime
	}
	if td.LastDecreaseDateTime != nil {
		thru.LastDec = *td.LastDecreaseDateTime
	}
	if td.NumberOfDecreasesToday != nil {
		thru.DecsToday = *td.NumberOfDecreasesToday
	}
	return thru
}

func schemaKeys(schema []types.KeySchemaElement) (hashKey, rangeKey string) {
	for _, ks := range schema {
		switch ks.KeyType {
		case types.KeyTypeHash:
			hashKey = *ks.AttributeName
		case types.KeyTypeRange:
			rangeKey = *ks.AttributeName
		}
	}
	return
}

func lookupADType(ads []types.AttributeDefinition, name string) KeyType {
	if name == "" {
		return ""
	}
	for _, ad := range ads {
		if *ad.AttributeName == name {
			return KeyType(ad.AttributeType)
		}
	}
	return ""
}
