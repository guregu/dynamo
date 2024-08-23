package dynamo

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ErrNoInput is returned when APIs that can take multiple inputs are run with zero inputs.
// For example, in a transaction with no operations.
var ErrNoInput = errors.New("dynamo: no input items")

type getTxOp interface {
	getTxItem() (types.TransactGetItem, error)
}

// GetTx is a transaction to retrieve items.
// It can contain up to 100 operations and works across multiple tables.
// GetTx is analogous to TransactGetItems in DynamoDB's API.
// See: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactGetItems.html
type GetTx struct {
	db           *DB
	items        []getTxOp
	unmarshalers map[getTxOp]interface{}
	cc           *ConsumedCapacity
}

// GetTx begins a new get transaction.
func (db *DB) GetTx() *GetTx {
	return &GetTx{
		db: db,
	}
}

// Get adds a get request to this transaction.
func (tx *GetTx) Get(q *Query) *GetTx {
	tx.items = append(tx.items, q)
	return tx
}

// GetOne adds a get request to this transaction, and specifies out to which the results are marshaled.
// Out must be a pointer. You can use this multiple times in one transaction.
func (tx *GetTx) GetOne(q *Query, out interface{}) *GetTx {
	if tx.unmarshalers == nil {
		tx.unmarshalers = make(map[getTxOp]interface{})
	}
	tx.items = append(tx.items, q)
	tx.unmarshalers[q] = out
	return tx
}

// ConsumedCapacity will measure the throughput capacity consumed by this transaction and add it to cc.
func (tx *GetTx) ConsumedCapacity(cc *ConsumedCapacity) *GetTx {
	tx.cc = cc
	return tx
}

// Run executes this transaction and unmarshals everything specified by GetOne.
func (tx *GetTx) Run(ctx context.Context) error {
	input, err := tx.input()
	if err != nil {
		return err
	}
	var resp *dynamodb.TransactGetItemsOutput
	err = tx.db.retry(ctx, func() error {
		var err error
		resp, err = tx.db.client.TransactGetItems(ctx, input)
		tx.cc.incRequests()
		if tx.cc != nil && resp != nil {
			for i := range resp.ConsumedCapacity {
				tx.cc.add(&resp.ConsumedCapacity[i])
			}
		}
		return err
	})
	if err != nil {
		return err
	}
	if isResponsesEmpty(resp.Responses) {
		return ErrNotFound
	}
	return tx.unmarshal(resp)
}

func (tx *GetTx) unmarshal(resp *dynamodb.TransactGetItemsOutput) error {
	for i, item := range resp.Responses {
		if item.Item == nil {
			continue
		}
		if target := tx.unmarshalers[tx.items[i]]; target != nil {
			if err := UnmarshalItem(item.Item, target); err != nil {
				return err
			}
		}
	}
	return nil
}

// All executes this transaction and unmarshals every value to out, which must be a pointer to a slice.
func (tx *GetTx) All(ctx context.Context, out interface{}) error {
	input, err := tx.input()
	if err != nil {
		return err
	}
	var resp *dynamodb.TransactGetItemsOutput
	err = tx.db.retry(ctx, func() error {
		var err error
		resp, err = tx.db.client.TransactGetItems(ctx, input)
		tx.cc.incRequests()
		if tx.cc != nil && resp != nil {
			for i := range resp.ConsumedCapacity {
				tx.cc.add(&resp.ConsumedCapacity[i])
			}
		}
		return err
	})
	if err != nil {
		return err
	}
	if isResponsesEmpty(resp.Responses) {
		return ErrNotFound
	}
	if err := tx.unmarshal(resp); err != nil {
		return err
	}
	push := unmarshalAppendTo(out)
	for _, item := range resp.Responses {
		if item.Item == nil {
			continue
		}
		if err := push(item.Item, out); err != nil {
			return err
		}
	}
	return nil
}

func (tx *GetTx) input() (*dynamodb.TransactGetItemsInput, error) {
	if len(tx.items) == 0 {
		return nil, ErrNoInput
	}
	input := &dynamodb.TransactGetItemsInput{}
	for _, item := range tx.items {
		tgi, err := item.getTxItem()
		if err != nil {
			return nil, err
		}
		input.TransactItems = append(input.TransactItems, tgi)
	}
	if tx.cc != nil {
		input.ReturnConsumedCapacity = types.ReturnConsumedCapacityIndexes
	}
	return input, nil
}

type writeTxOp interface {
	writeTxItem() (*types.TransactWriteItem, error)
}

// WriteTx is a transaction to delete, put, update, and check items.
// It can contain up to 100 operations and works across multiple tables.
// Two operations cannot target the same item.
// WriteTx is analogous to TransactWriteItems in DynamoDB's API.
// See: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html
type WriteTx struct {
	db         *DB
	items      []writeTxOp
	token      string
	onCondFail types.ReturnValuesOnConditionCheckFailure
	cc         *ConsumedCapacity
	err        error
}

// WriteTx begins a new write transaction.
func (db *DB) WriteTx() *WriteTx {
	return &WriteTx{
		db: db,
	}
}

// Delete adds a new delete operation to this transaction.
func (tx *WriteTx) Delete(d *Delete) *WriteTx {
	tx.items = append(tx.items, d)
	return tx
}

// Put adds a put operation to this transaction.
func (tx *WriteTx) Put(p *Put) *WriteTx {
	tx.items = append(tx.items, p)
	return tx
}

// Update adds an update operation to this transaction.
func (tx *WriteTx) Update(u *Update) *WriteTx {
	tx.items = append(tx.items, u)
	return tx
}

// Check adds a conditional check to this transaction.
func (tx *WriteTx) Check(check *ConditionCheck) *WriteTx {
	tx.items = append(tx.items, check)
	return tx
}

// IncludeAllItemsInCondCheckFail specifies whether an item write that fails its condition check should include the item itself in the error.
// Such items can be extracted using [UnmarshalItemsFromTxCondCheckFailed].
//
// By default, the individual settings for each item are respected.
// Calling this will override all individual settings.
func (tx *WriteTx) IncludeAllItemsInCondCheckFail(enabled bool) *WriteTx {
	if enabled {
		tx.onCondFail = types.ReturnValuesOnConditionCheckFailureAllOld
	} else {
		tx.onCondFail = types.ReturnValuesOnConditionCheckFailureNone
	}
	return tx
}

// Idempotent marks this transaction as idempotent when enabled is true.
// This automatically generates a unique idempotency token for you.
// An idempotent transaction ran multiple times will have the same effect as being run once.
// An idempotent request is only good for 10 minutes, after that it will be considered a new request.
func (tx *WriteTx) Idempotent(enabled bool) *WriteTx {
	if tx.token != "" && enabled {
		return tx
	}

	if enabled {
		token, err := newIdempotencyToken()
		tx.setError(err)
		tx.token = token
	} else {
		tx.token = ""
	}
	return tx
}

func newIdempotencyToken() (string, error) {
	var b [16]byte
	_, err := rand.Read(b[:])
	return hex.EncodeToString(b[:]), err
}

// IdempotentWithToken marks this transaction as idempotent and explicitly specifies the token value.
// If token is empty, idempotency will be disabled instead.
// Unless you have special circumstances that require a custom token, consider using Idempotent to generate a token for you.
// An idempotent transaction ran multiple times will have the same effect as being run once.
// An idempotent request (token) is only good for 10 minutes, after that it will be considered a new request.
func (tx *WriteTx) IdempotentWithToken(token string) *WriteTx {
	tx.token = token
	return tx
}

// ConsumedCapacity will measure the throughput capacity consumed by this transaction and add it to cc.
func (tx *WriteTx) ConsumedCapacity(cc *ConsumedCapacity) *WriteTx {
	tx.cc = cc
	return tx
}

// Run executes this transaction.
func (tx *WriteTx) Run(ctx context.Context) error {
	if tx.err != nil {
		return tx.err
	}
	input, err := tx.input()
	if err != nil {
		return err
	}
	err = tx.db.retry(ctx, func() error {
		out, err := tx.db.client.TransactWriteItems(ctx, input)
		tx.cc.incRequests()
		if out != nil {
			for i := range out.ConsumedCapacity {
				tx.cc.add(&out.ConsumedCapacity[i])
			}
		}
		return err
	})
	return err
}

func (tx *WriteTx) input() (*dynamodb.TransactWriteItemsInput, error) {
	if len(tx.items) == 0 {
		return nil, ErrNoInput
	}
	input := &dynamodb.TransactWriteItemsInput{}
	for _, item := range tx.items {
		wti, err := item.writeTxItem()
		if err != nil {
			return nil, err
		}
		setTWIReturnType(wti, tx.onCondFail)
		input.TransactItems = append(input.TransactItems, *wti)
	}
	if tx.token != "" {
		input.ClientRequestToken = aws.String(tx.token)
	}
	if tx.cc != nil {
		input.ReturnConsumedCapacity = types.ReturnConsumedCapacityIndexes
	}
	return input, nil
}

func setTWIReturnType(wti *types.TransactWriteItem, ret types.ReturnValuesOnConditionCheckFailure) {
	if ret == "" {
		return
	}
	switch {
	case wti.ConditionCheck != nil:
		wti.ConditionCheck.ReturnValuesOnConditionCheckFailure = ret
	case wti.Delete != nil:
		wti.Delete.ReturnValuesOnConditionCheckFailure = ret
	case wti.Put != nil:
		wti.Put.ReturnValuesOnConditionCheckFailure = ret
	case wti.Update != nil:
		wti.Update.ReturnValuesOnConditionCheckFailure = ret
	}
}

func (tx *WriteTx) setError(err error) {
	if tx.err == nil {
		tx.err = err
	}
}

func isResponsesEmpty(resps []types.ItemResponse) bool {
	for _, resp := range resps {
		if resp.Item != nil {
			return false
		}
	}
	return true
}
