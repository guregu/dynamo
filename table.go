package dynamo

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Status is an enumeration of table and index statuses.
type Status string

// Table and index statuses.
const (
	// The table or index is ready for use.
	ActiveStatus Status = "ACTIVE"
	// The table or index is being created.
	CreatingStatus Status = "CREATING"
	// The table or index is being updated.
	UpdatingStatus Status = "UPDATING"
	// The table or index is being deleted.
	DeletingStatus Status = "DELETING"

	// NotExistsStatus is a special status you can pass to table.Wait() to wait until a table doesn't exist.
	// DescribeTable will return a ResourceNotFound AWS error instead of this.
	NotExistsStatus Status = "_gone"
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

// Wait blocks until this table's status matches any status provided by want.
// If no statuses are specified, the active status is used.
func (table Table) Wait(ctx context.Context, want ...Status) error {
	if len(want) == 0 {
		want = []Status{ActiveStatus}
	}
	wantGone := false
	for _, status := range want {
		if status == NotExistsStatus {
			wantGone = true
		}
	}

	// I don't know why AWS wants a context _and_ a duration param.
	// Infer it from context; if it's indefinite then set it to something really high (1 day)
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(24 * time.Hour)
	}
	maxDur := time.Until(deadline)

	if wantGone {
		waiter := dynamodb.NewTableNotExistsWaiter(table.db.client)
		return waiter.Wait(ctx, table.Describe().input(), maxDur)
	}

	waiter := dynamodb.NewTableExistsWaiter(table.db.client, func(opts *dynamodb.TableExistsWaiterOptions) {
		fallback := opts.Retryable
		opts.Retryable = func(ctx context.Context, in *dynamodb.DescribeTableInput, out *dynamodb.DescribeTableOutput, err error) (bool, error) {
			if err == nil && out != nil && out.Table != nil {
				status := string(out.Table.TableStatus)
				for _, wantStatus := range want {
					if status == string(wantStatus) {
						return false, nil
					}
				}
			}
			return fallback(ctx, in, out, err)
		}
	})
	return waiter.Wait(ctx, table.Describe().input(), maxDur)
}

// primaryKeys attempts to determine this table's primary keys.
// It will try:
// - output LastEvaluatedKey
// - input ExclusiveStartKey
// - DescribeTable as a last resort (cached inside table)
func (table Table) primaryKeys(ctx context.Context, lek, esk Item, index string) (map[string]struct{}, error) {
	extract := func(item Item) map[string]struct{} {
		keys := make(map[string]struct{}, len(item))
		for k := range item {
			keys[k] = struct{}{}
		}
		return keys
	}

	// do we have canonical keys to use?
	switch {
	case lek != nil:
		return extract(lek), nil
	case esk != nil:
		return extract(esk), nil
	}

	// now we're forced to call DescribeTable

	// do we have a description cached?

	if desc, ok := table.db.loadDesc(table.name); ok {
		keys := desc.keys(index)
		if keys != nil {
			return keys, nil
		}
		// nil keys mean the table has changed since we cached it (index added), or something has gone horribly wrong
		// so let's continue...
	}

	keys := make(map[string]struct{})
	err := table.db.retry(ctx, func() error {
		desc, err := table.Describe().Run(ctx)
		if err != nil {
			return err
		}
		keys = desc.keys(index)
		if keys == nil {
			return fmt.Errorf("dynamo: unknown index %s on table %s", index, table.Name())
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}

func lekify(item Item, keys map[string]struct{}) (Item, error) {
	if item == nil {
		// this shouldn't happen because in queries without results, a LastEvaluatedKey should be given to us by AWS
		return nil, fmt.Errorf("dynamo: can't determine LastEvaluatedKey: no keys or results")
	}
	if keys == nil {
		return nil, fmt.Errorf("dynamo: can't determine LastEvaluatedKey: failed to infer primary keys")
	}
	lek := make(Item, len(keys))
	for k := range keys {
		v, ok := item[k]
		if !ok {
			return nil, fmt.Errorf("dynamo: can't determine LastEvaluatedKey: primary key attribute is missing from result: %s; add it to your projection or use SearchLimit instead of Limit", k)
		}
		lek[k] = v
	}
	return lek, nil
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
func (dt *DeleteTable) Run(ctx context.Context) error {
	input := dt.input()
	return dt.table.db.retry(ctx, func() error {
		_, err := dt.table.db.client.DeleteTable(ctx, input)
		return err
	})
}

// Wait executes this request and blocks until the table is finished deleting.
func (dt *DeleteTable) Wait(ctx context.Context) error {
	if err := dt.Run(ctx); err != nil {
		return err
	}
	return dt.table.Wait(ctx, NotExistsStatus)
}

func (dt *DeleteTable) input() *dynamodb.DeleteTableInput {
	name := dt.table.Name()
	return &dynamodb.DeleteTableInput{
		TableName: &name,
	}
}

// ConsumedCapacity represents the amount of throughput capacity consumed during an operation.
type ConsumedCapacity struct {
	// Total is the total number of capacity units consumed during this operation.
	Total float64
	// Read is the total number of read capacity units consumed during this operation.
	// This seems to be only set for transactions.
	Read float64
	// Write is the total number of write capacity units consumed during this operation.
	// This seems to be only set for transactions.
	Write float64

	// GSI is a map of Global Secondary Index names to total consumed capacity units.
	GSI map[string]float64
	// GSIRead is a map of Global Secondary Index names to consumed read capacity units.
	// This seems to be only set for transactions.
	GSIRead map[string]float64
	// GSIWrite is a map of Global Secondary Index names to consumed write capacity units.
	// This seems to be only set for transactions.
	GSIWrite map[string]float64

	// LSI is a map of Local Secondary Index names to total consumed capacity units.
	LSI map[string]float64
	// LSIRead is a map of Local Secondary Index names to consumed read capacity units.
	// This seems to be only set for transactions.
	LSIRead map[string]float64
	// LSIWrite is a map of Local Secondary Index names to consumed write capacity units.
	// This seems to be only set for transactions.
	LSIWrite map[string]float64

	// Table is the amount of total throughput consumed by the table.
	Table float64
	// TableRead is the amount of read throughput consumed by the table.
	// This seems to be only set for transactions.
	TableRead float64
	// TableWrite is the amount of write throughput consumed by the table.
	// This seems to be only set for transactions.
	TableWrite float64
	// TableName is the name of the table affected by this operation.
	TableName string

	// Requests is the number of SDK requests made against DynamoDB's API.
	Requests int
}

func (cc *ConsumedCapacity) add(raw *types.ConsumedCapacity) {
	if cc == nil || raw == nil {
		return
	}
	if raw.CapacityUnits != nil {
		cc.Total += *raw.CapacityUnits
	}
	if raw.ReadCapacityUnits != nil {
		cc.Read += *raw.ReadCapacityUnits
	}
	if raw.WriteCapacityUnits != nil {
		cc.Write += *raw.WriteCapacityUnits
	}
	if len(raw.GlobalSecondaryIndexes) > 0 {
		if cc.GSI == nil {
			cc.GSI = make(map[string]float64, len(raw.GlobalSecondaryIndexes))
		}
		for name, consumed := range raw.GlobalSecondaryIndexes {
			cc.GSI[name] = cc.GSI[name] + *consumed.CapacityUnits
			if consumed.ReadCapacityUnits != nil {
				if cc.GSIRead == nil {
					cc.GSIRead = make(map[string]float64, len(raw.GlobalSecondaryIndexes))
				}
				cc.GSIRead[name] = cc.GSIRead[name] + *consumed.ReadCapacityUnits
			}
			if consumed.WriteCapacityUnits != nil {
				if cc.GSIWrite == nil {
					cc.GSIWrite = make(map[string]float64, len(raw.GlobalSecondaryIndexes))
				}
				cc.GSIWrite[name] = cc.GSIWrite[name] + *consumed.WriteCapacityUnits
			}
		}
	}
	if len(raw.LocalSecondaryIndexes) > 0 {
		if cc.LSI == nil {
			cc.LSI = make(map[string]float64, len(raw.LocalSecondaryIndexes))
		}
		for name, consumed := range raw.LocalSecondaryIndexes {
			cc.LSI[name] = cc.LSI[name] + *consumed.CapacityUnits
			if consumed.ReadCapacityUnits != nil {
				if cc.LSIRead == nil {
					cc.LSIRead = make(map[string]float64, len(raw.LocalSecondaryIndexes))
				}
				cc.LSIRead[name] = cc.LSIRead[name] + *consumed.ReadCapacityUnits
			}
			if consumed.WriteCapacityUnits != nil {
				if cc.LSIWrite == nil {
					cc.LSIWrite = make(map[string]float64, len(raw.LocalSecondaryIndexes))
				}
				cc.LSIWrite[name] = cc.LSIWrite[name] + *consumed.WriteCapacityUnits
			}
		}
	}
	if raw.Table != nil {
		cc.Table += *raw.Table.CapacityUnits
		if raw.Table.ReadCapacityUnits != nil {
			cc.TableRead += *raw.Table.ReadCapacityUnits
		}
		if raw.Table.WriteCapacityUnits != nil {
			cc.TableWrite += *raw.Table.WriteCapacityUnits
		}
	}
	if raw.TableName != nil {
		cc.TableName = *raw.TableName
	}
}

func (cc *ConsumedCapacity) incRequests() {
	if cc == nil {
		return
	}
	cc.Requests++
}

func mergeConsumedCapacity(dst, src *ConsumedCapacity) {
	if dst == nil || src == nil {
		return
	}
	dst.Total += src.Total
	dst.Read += src.Read
	dst.Write += src.Write
	if len(src.GSI) > 0 {
		if dst.GSI == nil {
			dst.GSI = make(map[string]float64, len(src.GSI))
		}
		for name, consumed := range src.GSI {
			dst.GSI[name] = dst.GSI[name] + consumed
		}
	}
	if len(src.GSIRead) > 0 {
		if dst.GSIRead == nil {
			dst.GSIRead = make(map[string]float64, len(src.GSIRead))
		}
		for name, consumed := range src.GSIRead {
			dst.GSIRead[name] = dst.GSIRead[name] + consumed
		}
	}
	if len(src.GSIWrite) > 0 {
		if dst.GSIWrite == nil {
			dst.GSIWrite = make(map[string]float64, len(src.GSIWrite))
		}
		for name, consumed := range src.GSIWrite {
			dst.GSIWrite[name] = dst.GSIWrite[name] + consumed
		}
	}
	if len(src.LSI) > 0 {
		if dst.LSI == nil {
			dst.LSI = make(map[string]float64, len(src.LSI))
		}
		for name, consumed := range src.LSI {
			dst.LSI[name] = dst.LSI[name] + consumed
		}
	}
	if len(src.LSIRead) > 0 {
		if dst.LSIRead == nil {
			dst.LSIRead = make(map[string]float64, len(src.LSIRead))
		}
		for name, consumed := range src.LSIRead {
			dst.LSIRead[name] = dst.LSIRead[name] + consumed
		}
	}
	if len(src.LSIWrite) > 0 {
		if dst.LSIWrite == nil {
			dst.LSIWrite = make(map[string]float64, len(src.LSIWrite))
		}
		for name, consumed := range src.LSIWrite {
			dst.LSIWrite[name] = dst.LSIWrite[name] + consumed
		}
	}
	dst.Table += src.Table
	dst.TableRead += src.TableRead
	dst.TableWrite += src.TableWrite
	if dst.TableName == "" && src.TableName != "" {
		dst.TableName = src.TableName
	}
	dst.Requests += src.Requests
}
