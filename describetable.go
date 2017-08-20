package dynamo

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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

func newDescription(table *dynamodb.TableDescription) Description {
	desc := Description{
		Name: *table.TableName,
	}

	if table.TableArn != nil {
		desc.ARN = *table.TableArn
	}
	if table.TableStatus != nil {
		desc.Status = Status(*table.TableStatus)
	}
	if table.CreationDateTime != nil {
		desc.Created = *table.CreationDateTime
	}

	desc.HashKey, desc.RangeKey = schemaKeys(table.KeySchema)
	desc.HashKeyType = lookupADType(table.AttributeDefinitions, desc.HashKey)
	desc.RangeKeyType = lookupADType(table.AttributeDefinitions, desc.RangeKey)

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
			Status:     Status(*index.IndexStatus),
			Throughput: newThroughput(index.ProvisionedThroughput),
		}
		if index.Projection != nil && index.Projection.ProjectionType != nil {
			idx.ProjectionType = IndexProjection(*index.Projection.ProjectionType)
			idx.ProjectionAttribs = aws.StringValueSlice(index.Projection.NonKeyAttributes)
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
			Name:       *index.IndexName,
			ARN:        *index.IndexArn,
			Status:     ActiveStatus, // local secondary index is always active (technically, it has no status)
			Local:      true,
			Throughput: desc.Throughput, // has the same throughput as the table
		}
		if index.Projection != nil && index.Projection.ProjectionType != nil {
			idx.ProjectionType = IndexProjection(*index.Projection.ProjectionType)
			idx.ProjectionAttribs = aws.StringValueSlice(index.Projection.NonKeyAttributes)
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
		if table.StreamSpecification.StreamViewType != nil {
			desc.StreamView = StreamView(*table.StreamSpecification.StreamViewType)
		}
	}
	if table.LatestStreamArn != nil {
		desc.LatestStreamARN = *table.LatestStreamArn
	}
	if table.LatestStreamLabel != nil {
		desc.LatestStreamLabel = *table.LatestStreamLabel
	}

	return desc
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
func (dt *DescribeTable) Run() (Description, error) {
	ctx, cancel := defaultContext()
	defer cancel()
	return dt.RunWithContext(ctx)
}

func (dt *DescribeTable) RunWithContext(ctx aws.Context) (Description, error) {
	input := dt.input()

	var result *dynamodb.DescribeTableOutput
	err := retry(ctx, func() error {
		var err error
		result, err = dt.table.db.client.DescribeTableWithContext(ctx, input)
		return err
	})
	if err != nil {
		return Description{}, err
	}

	return newDescription(result.Table), nil
}

func (dt *DescribeTable) input() *dynamodb.DescribeTableInput {
	name := dt.table.Name()
	return &dynamodb.DescribeTableInput{
		TableName: &name,
	}
}

func newThroughput(td *dynamodb.ProvisionedThroughputDescription) Throughput {
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

func schemaKeys(schema []*dynamodb.KeySchemaElement) (hashKey, rangeKey string) {
	for _, ks := range schema {
		switch *ks.KeyType {
		case dynamodb.KeyTypeHash:
			hashKey = *ks.AttributeName
		case dynamodb.KeyTypeRange:
			rangeKey = *ks.AttributeName
		}
	}
	return
}

func lookupADType(ads []*dynamodb.AttributeDefinition, name string) KeyType {
	if name == "" {
		return ""
	}
	for _, ad := range ads {
		if *ad.AttributeName == name {
			return KeyType(*ad.AttributeType)
		}
	}
	return ""
}
