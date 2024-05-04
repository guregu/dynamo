package dynamo

import (
	"context"
)

// TODO: delete this

func (db *DB) retry(_ context.Context, f func() error) error {
	return f()
}
