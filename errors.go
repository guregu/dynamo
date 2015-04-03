package dynamo

import (
	"log"
	"time"

	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/cenkalti/backoff"
)

func retry(f func() error) error {
	var err error
	var next time.Duration
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = retryTimeout
	b.Reset()

	for {
		if err = f(); err == nil {
			return nil
		}

		if !canRetry(err) {
			return err
		}

		if next = b.NextBackOff(); next == backoff.Stop {
			log.Println("backoff: giving up", b.GetElapsedTime())
			return err
		}

		log.Println("sleeping fr", next)
		time.Sleep(next)
	}
}

func canRetry(err error) bool {
	if ae, ok := err.(aws.APIError); ok {
		switch ae.StatusCode {
		case 500, 503:
			return true
		case 400:
			switch ae.Code {
			case "ProvisionedThroughputExceededException",
				"ThrottlingException":
				return true
			}
		}
	}
	return false
}
