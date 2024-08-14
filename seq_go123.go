//go:build go1.23

package dynamo

import (
	"context"
	"iter"
)

// Seq returns an item iterator compatible with Go 1.23 `for ... range` loops.
func Seq[V any](ctx context.Context, iter Iter) iter.Seq[V] {
	return func(yield func(V) bool) {
		item := new(V)
		for iter.Next(ctx, item) {
			if !yield(*item) {
				break
			}
			item = new(V)
		}
	}
}

// SeqLEK returns a LastEvaluatedKey and item iterator compatible with Go 1.23 `for ... range` loops.
func SeqLEK[V any](ctx context.Context, iter PagingIter) iter.Seq2[PagingKey, V] {
	return func(yield func(PagingKey, V) bool) {
		item := new(V)
		for iter.Next(ctx, item) {
			lek, err := iter.LastEvaluatedKey(ctx)
			if err != nil {
				if setter, ok := iter.(interface{ SetError(error) }); ok {
					setter.SetError(err)
				}
			}
			if !yield(lek, *item) {
				break
			}
			item = new(V)
		}
	}
}
