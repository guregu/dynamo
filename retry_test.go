package dynamo

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func TestRetryMax(t *testing.T) {
	test := func(max int) (string, func(t *testing.T)) {
		name := fmt.Sprintf("max(%d)", max)
		return name, func(t *testing.T) {
			t.Parallel()
			t.Helper()
			sesh, err := session.NewSession(&aws.Config{
				MaxRetries:  aws.Int(max),
				Credentials: dummyCreds,
			})
			if err != nil {
				t.Fatal(err)
			}
			db := New(sesh)

			var runs int
			err = db.retry(context.Background(), func() error {
				runs++
				return awserr.NewRequestFailure(
					awserr.New(dynamodb.ErrCodeProvisionedThroughputExceededException, "dummy error", nil),
					400,
					fmt.Sprintf("try-%d", runs),
				)
			})
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if want := max + 1; runs != want {
				t.Error("wrong number of runs. want:", want, "got:", runs)
			}
		}
	}
	t.Run(test(0))
	t.Run(test(1))
	t.Run(test(3))
}

func TestRetryCustom(t *testing.T) {
	t.Parallel()
	sesh, err := session.NewSession(&aws.Config{
		Retryer:     client.NoOpRetryer{},
		MaxRetries:  aws.Int(10), // should be ignored (superseded by Retryer)
		Credentials: dummyCreds,
	})
	if err != nil {
		t.Fatal(err)
	}
	db := New(sesh)

	var runs int
	err = db.retry(context.Background(), func() error {
		runs++
		return awserr.NewRequestFailure(
			awserr.New(dynamodb.ErrCodeProvisionedThroughputExceededException, "dummy error", nil),
			400,
			fmt.Sprintf("try-%d", runs),
		)
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if want := 1; runs != want {
		t.Error("wrong number of runs. want:", want, "got:", runs)
	}
}
