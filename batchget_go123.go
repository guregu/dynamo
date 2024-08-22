//go:build go1.23

package dynamo

import (
	"context"
	"iter"
)

type ItemTableIter[V any] interface {
	// Items is a sequence of item and table names.
	// This is a single use iterator.
	// Be sure to check for errors with Err afterwards.
	Items(context.Context) iter.Seq2[V, string]
	// Err must be checked after iterating.
	Err() error
}

// ItemTableIter returns an iterator of (raw item, table name).
// To specify a type, use [BatchGetIter] instead.
//
// For example, you can utilize this iterator to read the results into different structs.
//
//	widgetBatch := widgetsTable.Batch("UserID").Get(dynamo.Keys{userID})
//	sprocketBatch := sprocketsTable.Batch("UserID").Get(dynamo.Keys{userID})
//
//	iter := widgetBatch.Merge(sprocketBatch).ItemTableIter(&table)
//
//	// now we will use the table iterator to unmarshal the values into their respective types
//	var s sprocket
//	var w widget
//	for raw, table := range iter.Items {
//		if table == "Widgets" {
//			err := dynamo.UnmarshalItem(raw, &w)
//			if err != nil {
//				fmt.Println(err)
//			}
//		} else if table == "Sprockets" {
//			err := dynamo.UnmarshalItem(raw, &s)
//			if err != nil {
//				fmt.Println(err)
//			}
//		} else {
//			fmt.Printf("Unexpected Table: %s\n", table)
//		}
//	}
//
//	if iter.Err() != nil {
//		fmt.Println(iter.Err())
//	}
func (bg *BatchGet) ItemTableIter() ItemTableIter[Item] {
	return newBgIter2[Item](bg)
}

type bgIter2[V any] struct {
	Iter
	table string
}

func newBgIter2[V any](bg *BatchGet) *bgIter2[V] {
	iter := new(bgIter2[V])
	iter.Iter = bg.IterWithTable(&iter.table)
	return iter
}

// Items is a sequence of item and table names.
// This is a single use iterator.
// Be sure to check for errors with Err afterwards.
func (iter *bgIter2[V]) Items(ctx context.Context) iter.Seq2[V, string] {
	return func(yield func(V, string) bool) {
		item := new(V)
		for iter.Next(ctx, item) {
			if !yield(*item, iter.table) {
				break
			}
			item = new(V)
		}
	}
}

func BatchGetIter[V any](bg *BatchGet) ItemTableIter[V] {
	return newBgIter2[V](bg)
}
