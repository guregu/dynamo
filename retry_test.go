package dynamo

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestRetryMax(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	test := func(max int) (string, func(t *testing.T)) {
		name := fmt.Sprintf("max(%d)", max)
		return name, func(t *testing.T) {
			t.Parallel()
			t.Helper()
			db := New(aws.Config{
				RetryMaxAttempts: max,
				Credentials:      dummyCreds,
			})

			var runs int
			err := db.retry(context.Background(), func() error {
				runs++
				return &types.ProvisionedThroughputExceededException{}
			})
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if want := max + 1; runs != want {
				t.Error("wrong number of runs. want:", want, "got:", runs)
			}
		}
	}
	// t.Run(test(0)) // behavior changed from v1
	t.Run(test(1))
	t.Run(test(3))
}

func TestRetryCustom(t *testing.T) {
	t.Parallel()
	retryer := func() aws.Retryer {
		return retry.NewStandard(func(so *retry.StandardOptions) {
			so.MaxAttempts = 1
		})
	}
	db := New(aws.Config{
		Retryer:     retryer,
		Credentials: dummyCreds,
	})

	var runs int
	err := db.retry(context.Background(), func() error {
		runs++
		return &types.ProvisionedThroughputExceededException{}
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if want := 1; runs != want {
		t.Error("wrong number of runs. want:", want, "got:", runs)
	}
}
