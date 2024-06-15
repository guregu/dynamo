package dynamo

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// TODO: delete this

func (db *DB) retry(_ context.Context, f func() error) error {
	return f()
}

// RetryTxConflicts is an option for [github.com/aws/aws-sdk-go-v2/aws/retry.NewStandard]
// that adds retrying behavior for TransactionConflict within TransactionCanceledException errors.
// See also: [github.com/aws/aws-sdk-go-v2/config.WithRetryer].
func RetryTxConflicts(opts *retry.StandardOptions) {
	opts.Retryables = append(opts.Retryables, retry.IsErrorRetryableFunc(shouldRetryTx))
}

func shouldRetryTx(err error) aws.Ternary {
	var txe *types.TransactionCanceledException
	if errors.As(err, &txe) {
		retry := aws.FalseTernary
		for _, reason := range txe.CancellationReasons {
			if reason.Code == nil {
				continue
			}
			switch *reason.Code {
			case "ValidationError", "ConditionalCheckFailed", "ItemCollectionSizeLimitExceeded":
				return aws.FalseTernary
			case "ThrottlingError", "ProvisionedThroughputExceeded", "TransactionConflict":
				retry = aws.TrueTernary
			}
		}
		return retry
	}
	return aws.UnknownTernary
}
