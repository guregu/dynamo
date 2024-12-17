package dynamo

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestGetAllCount(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	ctx := context.TODO()
	table := testDB.Table(testTableWidgets)

	// first, add an item to make sure there is at least one
	item := widget{
		UserID: 42,
		Time:   time.Now().UTC(),
		Msg:    "hello",
		Meta: map[string]string{
			"foo":        "bar",
			"animal.cow": "moo",
		},
		StrPtr: new(string),
	}
	err := table.Put(item).Run(ctx)
	if err != nil {
		t.Error("unexpected error:", err)
	}

	lit := ExpressionLiteral{
		Expression: "#meta.#foo = :bar",
		AttributeNames: aws.StringMap(map[string]string{
			"#meta": "Meta",
			"#foo":  "foo",
		}),
		AttributeValues: Item{
			":bar": &types.AttributeValueMemberS{Value: "bar"},
		},
	}

	// now check if get all and count return the same amount of items
	var result []widget
	var cc1, cc2 ConsumedCapacity
	err = table.Get("UserID", 42).
		Consistent(true).
		Filter("Msg = ?", item.Msg).
		Filter("StrPtr = ?", "").
		Filter("?", lit).
		ConsumedCapacity(&cc1).
		All(ctx, &result)
	if err != nil {
		t.Error("unexpected error:", err)
	}

	ct, err := table.Get("UserID", 42).
		Consistent(true).
		Filter("Msg = ?", item.Msg).
		Filter("StrPtr = ?", "").
		Filter("$", lit). // both $ and ? are OK for literals
		ConsumedCapacity(&cc2).
		Count(ctx)
	if err != nil {
		t.Error("unexpected error:", err)
	}
	if int(ct) != len(result) {
		t.Errorf("count and GetAll don't match. count: %d, get all: %d", ct, len(result))
	}

	if cc1.Total == 0 || cc2.Total == 0 {
		t.Error("blank ConsumedCapacity", cc1, cc2)
	}
	if !reflect.DeepEqual(cc1, cc2) {
		t.Error("ConsumedCapacity not equal", cc1, "≠", cc2)
	}

	// search for our inserted item
	found := false
	for _, w := range result {
		if w.Time.Equal(item.Time) && reflect.DeepEqual(w, item) {
			found = true
			break
		}
	}
	if !found {
		t.Error("exact match of put item not found in get all")
	}

	// query specifically against the inserted item (using GetItem)
	var one widget
	err = table.Get("UserID", 42).Range("Time", Equal, item.Time).Consistent(true).One(ctx, &one)
	if err != nil {
		t.Error("unexpected error:", err)
	}
	if !reflect.DeepEqual(one, item) {
		t.Errorf("bad result for get one. %v ≠ %v", one, item)
	}

	// query specifically against the inserted item (using Query)
	one = widget{}
	err = table.Get("UserID", 42).Range("Time", Equal, item.Time).Filter("Msg = ?", item.Msg).Filter("StrPtr = ?", "").Consistent(true).One(ctx, &one)
	if err != nil {
		t.Error("unexpected error:", err)
	}
	if !reflect.DeepEqual(one, item) {
		t.Errorf("bad result for get one. %v ≠ %v", one, item)
	}

	// trigger ErrTooMany
	one = widget{}
	err = table.Get("UserID", 42).Range("Time", Greater, "0").Consistent(true).One(ctx, &one)
	if !errors.Is(err, ErrTooMany) {
		t.Errorf("bad error from get one. %v ≠ %v", err, ErrTooMany)
	}

	// suppress ErrTooMany with Limit(1)
	one = widget{}
	err = table.Get("UserID", 42).Range("Time", Greater, "0").Consistent(true).Limit(1).One(ctx, &one)
	if err != nil {
		t.Error("unexpected error:", err)
	}
	if one.UserID == 0 {
		t.Errorf("bad result for get one: %v", one)
	}

	// trigger ErrNotFound via SearchLimit + Filter + One
	one = widget{}
	err = table.Get("UserID", 42).Range("Time", Greater, "0").Filter("Msg = ?", item.Msg).Consistent(true).SearchLimit(1).One(ctx, &one)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("bad error from get one. %v ≠ %v", err, ErrNotFound)
	}

	// GetItem + Project
	one = widget{}
	projected := widget{
		UserID: item.UserID,
		Time:   item.Time,
	}
	err = table.Get("UserID", 42).Range("Time", Equal, item.Time).Project("UserID", "Time").Consistent(true).One(ctx, &one)
	if err != nil {
		t.Error("unexpected error:", err)
	}
	if !reflect.DeepEqual(one, projected) {
		t.Errorf("bad result for get one+project. %v ≠ %v", one, projected)
	}

	// GetItem + ProjectExpr
	one = widget{}
	projected = widget{
		UserID: item.UserID,
		Time:   item.Time,
		Meta: map[string]string{
			"foo":        "bar",
			"animal.cow": "moo",
		},
	}
	err = table.Get("UserID", 42).Range("Time", Equal, item.Time).ProjectExpr("UserID, $, Meta.foo, Meta.$", "Time", "animal.cow").Consistent(true).One(ctx, &one)
	if err != nil {
		t.Error("unexpected error:", err)
	}
	if !reflect.DeepEqual(one, projected) {
		t.Errorf("bad result for get one+project. %v ≠ %v", one, projected)
	}
}

func TestQueryPaging(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	ctx := context.TODO()
	table := testDB.Table(testTableWidgets)

	widgets := []interface{}{
		widget{
			UserID: 1969,
			Time:   time.Date(1969, 4, 00, 0, 0, 0, 0, time.UTC),
			Msg:    "first widget",
		},
		widget{
			UserID: 1969,
			Time:   time.Date(1969, 4, 10, 0, 0, 0, 0, time.UTC),
			Msg:    "second widget",
		},
		widget{
			UserID: 1969,
			Time:   time.Date(1969, 4, 20, 0, 0, 0, 0, time.UTC),
			Msg:    "third widget",
		},
	}

	if _, err := table.Batch().Write().Put(widgets...).Run(ctx); err != nil {
		t.Error("couldn't write paging prep data", err)
		return
	}

	itr := table.Get("UserID", 1969).SearchLimit(1).Iter()
	for i := 0; i < len(widgets); i++ {
		var w widget
		itr.Next(ctx, &w)
		if !reflect.DeepEqual(w, widgets[i]) {
			t.Error("bad result:", w, "≠", widgets[i])
		}
		if itr.Err() != nil {
			t.Error("unexpected error", itr.Err())
		}
		more := itr.Next(ctx, &w)
		if more {
			t.Error("unexpected more", more)
		}
		lek, err := itr.LastEvaluatedKey(context.Background())
		if err != nil {
			t.Error("LEK error", err)
		}
		itr = table.Get("UserID", 1969).StartFrom(lek).SearchLimit(1).Iter()
	}
}

func TestQueryMagicLEK(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	ctx := context.Background()
	table := testDB.Table(testTableWidgets)

	widgets := []interface{}{
		widget{
			UserID: 1970,
			Time:   time.Date(1970, 4, 00, 0, 0, 0, 0, time.UTC),
			Msg:    "TestQueryMagicLEK",
		},
		widget{
			UserID: 1970,
			Time:   time.Date(1970, 4, 10, 0, 0, 0, 0, time.UTC),
			Msg:    "TestQueryMagicLEK",
		},
		widget{
			UserID: 1970,
			Time:   time.Date(1970, 4, 20, 0, 0, 0, 0, time.UTC),
			Msg:    "TestQueryMagicLEK",
		},
	}

	t.Run("prepare data", func(t *testing.T) {
		if _, err := table.Batch().Write().Put(widgets...).Run(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("having to use DescribeTable", func(t *testing.T) {
		itr := table.Get("UserID", 1970).Filter("attribute_exists('Msg')").Limit(1).Iter()
		for i := 0; i < len(widgets); i++ {
			var w widget
			itr.Next(ctx, &w)
			if !reflect.DeepEqual(w, widgets[i]) {
				t.Error("bad result:", w, "≠", widgets[i])
			}
			if itr.Err() != nil {
				t.Error("unexpected error", itr.Err())
			}
			more := itr.Next(ctx, &w)
			if more {
				t.Error("unexpected more", more)
			}
			lek, err := itr.LastEvaluatedKey(context.Background())
			if err != nil {
				t.Error("LEK error", lek)
			}
			itr = table.Get("UserID", 1970).StartFrom(lek).Limit(1).Iter()
		}
	})

	t.Run("table cache", func(t *testing.T) {
		pk, err := table.primaryKeys(context.Background(), nil, nil, "")
		if err != nil {
			t.Fatal(err)
		}
		expect := map[string]struct{}{
			"UserID": {},
			"Time":   {},
		}
		if !reflect.DeepEqual(pk, expect) {
			t.Error("unexpected key cache. want:", expect, "got:", pk)
		}
	})

	t.Run("via index", func(t *testing.T) {
		itr := table.Get("Msg", "TestQueryMagicLEK").Index("Msg-Time-index").Filter("UserID = ?", 1970).Limit(1).Iter()
		for i := 0; i < len(widgets); i++ {
			var w widget
			itr.Next(ctx, &w)
			if !reflect.DeepEqual(w, widgets[i]) {
				t.Error("bad result:", w, "≠", widgets[i])
			}
			if itr.Err() != nil {
				t.Error("unexpected error", itr.Err())
			}
			more := itr.Next(ctx, &w)
			if more {
				t.Error("unexpected more", more)
			}
			lek, err := itr.LastEvaluatedKey(context.Background())
			if err != nil {
				t.Error("LEK error", err)
			}
			itr = table.Get("Msg", "TestQueryMagicLEK").Index("Msg-Time-index").Filter("UserID = ?", 1970).StartFrom(lek).Limit(1).Iter()
		}
	})
}

func TestQueryBadKeys(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	table := testDB.Table(testTableWidgets)
	ctx := context.Background()

	t.Run("hash key", func(t *testing.T) {
		var v interface{}
		err := table.Get("UserID", "").Range("Time", Equal, "123").One(ctx, &v)
		if err == nil {
			t.Error("want error, got", err)
		}
	})

	t.Run("range key", func(t *testing.T) {
		var v interface{}
		err := table.Get("UserID", 123).Range("Time", Equal, "").One(ctx, &v)
		if err == nil {
			t.Error("want error, got", err)
		}
	})
}
