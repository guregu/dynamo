package dynamo

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/cenkalti/backoff"
	"golang.org/x/net/context"
)

// RetryTimeout defines the maximum amount of time that requests will
// attempt to automatically retry for. In other words, this is the maximum
// amount of time that dynamo operations will block.
// RetryTimeout is only considered by methods that do not take a context.
// Higher values are better when using tables with lower throughput.
var RetryTimeout = 1 * time.Minute

func defaultContext() (aws.Context, context.CancelFunc) {
	if RetryTimeout == 0 {
		return aws.BackgroundContext(), (func() {})
	}
	return context.WithDeadline(aws.BackgroundContext(), time.Now().Add(RetryTimeout))
}

func retry(ctx aws.Context, f func() error) error {
	var err error
	var next time.Duration
	b := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
	for {
		if err = f(); err == nil {
			return nil
		}

		if !canRetry(err) {
			return err
		}

		if next = b.NextBackOff(); next == backoff.Stop {
			return err
		}

		if err = aws.SleepWithContext(ctx, next); err != nil {
			return err
		}
	}
}

func canRetry(err error) bool {
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
