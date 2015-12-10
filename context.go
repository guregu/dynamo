package dynamo

import (
	"golang.org/x/net/context"
)

type ctxKey int

var dbCtxKey ctxKey = 0

func NewContext(ctx context.Context, db *DB) context.Context {
	return context.WithValue(ctx, dbCtxKey, db)
}

func FromContext(ctx context.Context) *DB {
	db, _ := ctx.Value(dbCtxKey).(*DB)
	return db
}
