package dynamo

import (
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/cenkalti/backoff"
)

// RetryTimeout defines the maximum amount of time that requests will
// attempt to automatically retry for. In other words, this is the maximum
// amount of time that dynamo operations will block.
// Higher values are better when using tables with lower throughput.
var RetryTimeout = 1 * time.Minute

func retry(f func() error) error {
	var err error
	var next time.Duration
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = RetryTimeout

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

		time.Sleep(next)
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
