package dynamo

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/cenkalti/backoff/v4"
)

// RetryTimeout defines the maximum amount of time that requests will
// attempt to automatically retry for. In other words, this is the maximum
// amount of time that dynamo operations will block.
// RetryTimeout is only considered by methods that do not take a context.
// Higher values are better when using tables with lower throughput.
var RetryTimeout = 1 * time.Minute

func defaultContext() (context.Context, context.CancelFunc) {
	if RetryTimeout == 0 {
		return aws.BackgroundContext(), (func() {})
	}
	return context.WithDeadline(aws.BackgroundContext(), time.Now().Add(RetryTimeout))
}

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

		if err := aws.SleepWithContext(ctx, next); err != nil {
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

	if txe, ok := err.(*dynamodb.TransactionCanceledException); ok && txe.StatusCode() == 400 {
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

	if ae, ok := err.(awserr.RequestFailure); ok {
		switch ae.StatusCode() {
		case 500, 503:
			return true
		case 400:
			switch ae.Code() {
			case "ProvisionedThroughputExceededException",
				"ThrottlingException":
				return true
			}
		}
	}
	return false
}
