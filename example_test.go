package dynamo_test

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/guregu/dynamo/v2"
)

func ExampleNew() {
	// Basic setup example.
	// See: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/config for more on configuration options.
	const region = "us-west-2"
	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion(region),
	)
	if err != nil {
		log.Fatal(err)
	}
	db := dynamo.New(cfg)
	// use the db
	_ = db
}

func ExampleNew_local_endpoint() {
	// Example of connecting to a DynamoDB local instance.
	// See: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html
	const endpoint = "http://localhost:8000"
	resolver := aws.EndpointResolverWithOptionsFunc(
		func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{URL: endpoint}, nil
		},
	)
	// credentials can be anything, but must be set
	creds := credentials.NewStaticCredentialsProvider("dummy", "dummy", "")
	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion("local"), // region can also be anything
		config.WithEndpointResolverWithOptions(resolver),
		config.WithCredentialsProvider(creds),
	)
	if err != nil {
		log.Fatal(err)
	}
	db := dynamo.New(cfg)
	// use the db
	_ = db
}

func ExampleRetryTx() {
	// `dynamo.RetryTx` is an option you can pass to retry.NewStandard.
	// It will automatically retry canceled transactions.
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRetryer(func() aws.Retryer {
		return retry.NewStandard(dynamo.RetryTx)
	}))
	if err != nil {
		log.Fatal(err)
	}
	db := dynamo.New(cfg)
	// use the db
	_ = db
}
