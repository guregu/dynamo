module github.com/guregu/dynamo

require (
	github.com/aws/aws-sdk-go v1.48.10
	github.com/cenkalti/backoff/v4 v4.2.1
	golang.org/x/sync v0.5.0
)

require github.com/jmespath/go-jmespath v0.4.0 // indirect

go 1.20

retract (
	v1.22.0 // See issues: #228, #230
)