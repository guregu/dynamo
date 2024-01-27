package dynamo

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	awstime "github.com/aws/smithy-go/time"
	"github.com/cenkalti/backoff/v4"
)

func (db *DB) retry(ctx context.Context, f func() error) error {
	// if a custom retryer has been set, the SDK will retry for us
	if db.retryer != nil {
		return f()
	}

	var err error
	var next time.Duration
	b := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
	for i := 0; db.retryMax < 0 || i <= db.retryMax; i++ {
		if err = f(); err == nil {
			return nil
		}

		if !canRetry(err) {
			return err
		}

		if next = b.NextBackOff(); next == backoff.Stop {
			return err
		}
		if err := awstime.SleepWithContext(ctx, next); err != nil {
			return err
		}
	}
	return err
}

// errRetry is a sentinel error to retry, should never be returned to user
var errRetry = errors.New("dynamo: retry")

func canRetry(err error) bool {
	if errors.Is(err, errRetry) {
		return true
	}

	var txe *types.TransactionCanceledException
	if errors.As(err, &txe) {
		retry := false
		for _, reason := range txe.CancellationReasons {
			if reason.Code == nil {
				continue
			}
			switch *reason.Code {
			case "ValidationError", "ConditionalCheckFailed", "ItemCollectionSizeLimitExceeded":
				return false
			case "ThrottlingError", "ProvisionedThroughputExceeded", "TransactionConflict":
				retry = true
			}
		}
		return retry
	}

	var aerr smithy.APIError
	if errors.As(err, &aerr) {
		switch aerr.ErrorCode() {
		case "ProvisionedThroughputExceededException",
			"ThrottlingException":
			return true
		}
	}

	var rerr *http.ResponseError
	if errors.As(err, &rerr) {
		switch rerr.HTTPStatusCode() {
		case 500, 503:
			return true
		}
	}

	return false
}
