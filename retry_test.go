package dynamo

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

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
