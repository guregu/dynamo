package dynamo

import (
	"golang.org/x/net/context"
)

type ctxKey int

var dbCtxKey ctxKey

// NewContext creates a new context associated with db.
func NewContext(ctx context.Context, db *DB) context.Context {
	return context.WithValue(ctx, dbCtxKey, db)
}

// FromContext retrieves the DB inside the given context, if any.
func FromContext(ctx context.Context) *DB {
	db, _ := ctx.Value(dbCtxKey).(*DB)
	return db
}
