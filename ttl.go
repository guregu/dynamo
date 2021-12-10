package dynamo

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// UpdateTTL is a request to enable or disable a table's time to live functionality.
// Note that when time to live is enabled, items will typically be deleted within 48 hours
// and items that are expired but not yet deleted will still appear in your database.
// See: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateTimeToLive.html
type UpdateTTL struct {
	table   Table
	attrib  string
	enabled bool
}

// UpdateTTL begins a new request to enable or disable this table's time to live.
// The name of the attribute to use for expiring items is specified by attribute.
// TTL will be enabled when enabled is true and disabled when it is false.
// The time to live attribute must be stored as Unix time in seconds.
// Items without this attribute won't be deleted.
func (table Table) UpdateTTL(attribute string, enabled bool) *UpdateTTL {
	return &UpdateTTL{
		table:   table,
		attrib:  attribute,
		enabled: enabled,
	}
}

// Run executes this request.
func (ttl *UpdateTTL) Run() error {
	ctx, cancel := defaultContext()
	defer cancel()
	return ttl.RunWithContext(ctx)
}

// RunWithContext executes this request.
func (ttl *UpdateTTL) RunWithContext(ctx context.Context) error {
	input := ttl.input()

	err := retry(ctx, func() error {
		_, err := ttl.table.db.client.UpdateTimeToLive(ctx, input)
		return err
	})
	return err
}

func (ttl *UpdateTTL) input() *dynamodb.UpdateTimeToLiveInput {
	return &dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String(ttl.table.Name()),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			Enabled:       aws.Bool(ttl.enabled),
			AttributeName: aws.String(ttl.attrib),
		},
	}
}

// DescribeTTL is a request to obtain details about a table's time to live configuration.
type DescribeTTL struct {
	table Table
}

// DescribeTTL begins a new request to obtain details about this table's time to live configuration.
func (table Table) DescribeTTL() *DescribeTTL {
	return &DescribeTTL{table}
}

// Run executes this request and returns details about time to live, or an error.
func (d *DescribeTTL) Run() (TTLDescription, error) {
	ctx, cancel := defaultContext()
	defer cancel()
	return d.RunWithContext(ctx)
}

// RunWithContext executes this request and returns details about time to live, or an error.
func (d *DescribeTTL) RunWithContext(ctx context.Context) (TTLDescription, error) {
	input := d.input()

	var result *dynamodb.DescribeTimeToLiveOutput
	err := retry(ctx, func() error {
		var err error
		result, err = d.table.db.client.DescribeTimeToLive(ctx, input)
		return err
	})
	if err != nil {
		return TTLDescription{}, err
	}

	desc := TTLDescription{
		Status: TTLDisabled,
	}
	if result.TimeToLiveDescription.TimeToLiveStatus != "" {
		desc.Status = TTLStatus(result.TimeToLiveDescription.TimeToLiveStatus)
	}
	if result.TimeToLiveDescription.AttributeName != nil {
		desc.Attribute = *result.TimeToLiveDescription.AttributeName
	}
	return desc, nil
}

func (d *DescribeTTL) input() *dynamodb.DescribeTimeToLiveInput {
	return &dynamodb.DescribeTimeToLiveInput{
		TableName: aws.String(d.table.Name()),
	}
}

// TTLDescription represents time to live configuration details for a table.
type TTLDescription struct {
	// Attribute is the name of the time to live attribute for the table. Empty if disabled.
	Attribute string
	// Status is the table's time to live status.
	Status TTLStatus
}

// Enabled returns true if time to live is enabled (and has finished enabling).
func (td TTLDescription) Enabled() bool {
	return td.Status == TTLEnabled
}

// TTLStatus represents a table's time to live status.
type TTLStatus string

// Possible time to live statuses.
const (
	TTLEnabled   TTLStatus = "ENABLED"
	TTLEnabling  TTLStatus = "ENABLING"
	TTLDisabled  TTLStatus = "DISABLED"
	TTLDisabling TTLStatus = "DISABLING"
)
