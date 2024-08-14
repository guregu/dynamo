//go:build go1.23

package dynamo

import (
	"context"
	"testing"
	"time"
)

func TestSeq(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	ctx := context.Background()
	table := testDB.Table(testTableWidgets)

	widgets := []any{
		widget{
			UserID: 1971,
			Time:   time.Date(1971, 4, 00, 0, 0, 0, 0, time.UTC),
			Msg:    "Seq1",
		},
		widget{
			UserID: 1971,
			Time:   time.Date(1971, 4, 10, 0, 0, 0, 0, time.UTC),
			Msg:    "Seq1",
		},
		widget{
			UserID: 1971,
			Time:   time.Date(1971, 4, 20, 0, 0, 0, 0, time.UTC),
			Msg:    "Seq1",
		},
	}

	t.Run("prepare data", func(t *testing.T) {
		if _, err := table.Batch().Write().Put(widgets...).Run(ctx); err != nil {
			t.Fatal(err)
		}
	})

	iter := testDB.Table(testTableWidgets).Get("UserID", 1971).Iter()
	var got []*widget
	var count int
	for item := range Seq[*widget](ctx, iter) {
		t.Log(item)
		item.Count = count
		got = append(got, item)
		count++
	}

	if iter.Err() != nil {
		t.Fatal(iter.Err())
	}

	t.Run("results match", func(t *testing.T) {
		for i, item := range got {
			want := widgets[i].(widget)
			if !item.Time.Equal(want.Time) {
				t.Error("bad result. want:", want.Time, "got:", item.Time)
			}
		}
	})

	t.Run("result item isolation", func(t *testing.T) {
		// make sure that when mutating the result in the `for ... range` loop
		// it only affects one item
		t.Log("got", got)
		for i, item := range got {
			if item.Count != i {
				t.Error("unexpected count. got:", item.Count, "want:", i)
			}
		}
	})
}
