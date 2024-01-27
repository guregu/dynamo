package dynamo

import (
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestUpdate(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	table := testDB.Table(testTable)

	type widget2 struct {
		widget
		MySet1 []string            `dynamo:",set"`
		MySet2 map[string]struct{} `dynamo:",set"`
		MySet3 map[int64]struct{}  `dynamo:",set"`
	}

	// first, add an item to make sure there is at least one
	item := widget2{
		widget: widget{
			UserID: 42,
			Time:   time.Now().UTC(),
			Msg:    "hello",
			Count:  0,
			Meta: map[string]string{
				"foo":  "bar",
				"keep": "untouched",
				"nope": "痛",
				"haha": "555 no jam",
			},
		},
		MySet1: []string{"one", "deleteme"},
		MySet2: map[string]struct{}{"a": {}, "b": {}, "bad1": {}, "c": {}, "bad2": {}},
		MySet3: map[int64]struct{}{1: {}, 999: {}, 2: {}, 3: {}, 555: {}},
	}
	err := table.Put(item).Run()
	if err != nil {
		t.Error("unexpected error:", err)
	}

	setLit := ExpressionLiteral{
		Expression: "#meta.#pet = :cat",
		AttributeNames: aws.StringMap(map[string]string{
			"#meta": "Meta",
			"#pet":  "pet",
		}),
		AttributeValues: Item{
			":cat": &types.AttributeValueMemberS{Value: "猫"},
		},
	}
	rmLit := ExpressionLiteral{
		Expression: "#meta.#haha",
		AttributeNames: aws.StringMap(map[string]string{
			"#meta": "Meta",
			"#haha": "haha",
		}),
	}
	ifLit := ExpressionLiteral{
		Expression: "#msg = :hi",
		AttributeNames: aws.StringMap(map[string]string{
			"#msg": "Msg",
		}),
		AttributeValues: Item{
			":hi": &types.AttributeValueMemberS{Value: "hello"},
		},
	}

	// change it a bit and check the result
	var result widget2
	var cc ConsumedCapacity
	err = table.Update("UserID", item.UserID).
		Range("Time", item.Time).
		Set("Msg", "changed").
		SetExpr("Meta.$ = ?", "foo", "baz").
		SetExpr("$", setLit).
		Add("Count", 1).
		Add("Test", []string{"A", "B"}).
		RemoveExpr("Meta.$", "nope").
		RemoveExpr("$", rmLit).
		If("('Count' = ?) OR attribute_not_exists('Count')", 0).
		If("?", ifLit).
		DeleteFromSet("MySet1", "deleteme").
		DeleteFromSet("MySet2", []string{"bad1", "bad2"}).
		DeleteFromSet("MySet3", map[int64]struct{}{999: {}, 555: {}}).
		ConsumedCapacity(&cc).
		Value(&result)

	expected := widget2{
		widget: widget{
			UserID: item.UserID,
			Time:   item.Time,
			Msg:    "changed",
			Count:  1,
			Meta: map[string]string{
				"foo":  "baz",
				"keep": "untouched",
				"pet":  "猫",
			},
		},
		MySet1: []string{"one"},
		MySet2: map[string]struct{}{"a": {}, "b": {}, "c": {}},
		MySet3: map[int64]struct{}{1: {}, 2: {}, 3: {}},
	}
	if err != nil {
		t.Error("unexpected error:", err)
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("bad result. %+v ≠ %+v", result, expected)
	}
	if cc.Total < 1 {
		t.Error("bad consumed capacity", cc)
	}

	// test OnlyUpdatedValue
	var updated widget2
	expected2 := widget2{
		widget: widget{
			Msg:   "changed again",
			Count: 2,
		},
	}
	err = table.Update("UserID", item.UserID).
		Range("Time", item.Time).
		Set("Msg", expected2.Msg).
		Add("Count", 1).
		OnlyUpdatedValue(&updated)
	if err != nil {
		t.Error("unexpected error:", err)
	}
	if !reflect.DeepEqual(updated, expected2) {
		t.Errorf("bad result. %+v ≠ %+v", updated, expected)
	}

	var updatedOld widget2
	err = table.Update("UserID", item.UserID).
		Range("Time", item.Time).
		Set("Msg", "this shouldn't be seen").
		Add("Count", 100).
		OnlyUpdatedOldValue(&updatedOld)
	if err != nil {
		t.Error("unexpected error:", err)
	}
	if !reflect.DeepEqual(updatedOld, expected2) {
		t.Errorf("bad result. %+v ≠ %+v", updatedOld, expected)
	}

	// send an update with a failing condition
	err = table.Update("UserID", item.UserID).
		Range("Time", item.Time).
		Set("Msg", "shouldn't happen").
		Add("Count", 1).
		If("'Count' > ?", 100).
		If("(MeaningOfLife = ?)", 42).
		Value(&result)
	if !IsCondCheckFailed(err) {
		t.Error("expected ConditionalCheckFailedException, not", err)
	}
}

func TestUpdateNil(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	table := testDB.Table(testTable)

	// first, add an item to make sure there is at least one
	item := widget{
		UserID: 4242,
		Time:   time.Now().UTC(),
		Msg:    "delete me",
		Meta: map[string]string{
			"abc": "123",
		},
		Count: 100,
	}
	err := table.Put(item).Run()
	if err != nil {
		t.Error("unexpected error:", err)
		t.FailNow()
	}

	type widget2 struct {
		widget
		MsgPtr *string
	}

	// update Msg with 'nil', which should delete it
	var result widget2
	err = table.Update("UserID", item.UserID).Range("Time", item.Time).
		Set("Msg", "").
		Set("Meta.'abc'", nil).
		Set("Meta.'ok'", (*ptrTextMarshaler)(nil)).
		SetExpr("'Count' = ?", (*textMarshaler)(nil)).
		SetExpr("MsgPtr = ?", "").
		Value(&result)
	if err != nil {
		t.Error("unexpected error:", err)
	}
	expected := widget2{
		widget: widget{
			UserID: item.UserID,
			Time:   item.Time,
			Msg:    "",
			Meta: map[string]string{
				"ok": "null",
			},
		},
		MsgPtr: new(string),
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("bad result. %+v ≠ %+v", result, expected)
	}
}

func TestUpdateSetAutoOmit(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	table := testDB.Table(testTable)

	type widget2 struct {
		widget
		CStr customString
		SPtr *string
	}

	// first, add an item to make sure there is at least one
	str := "delete me ptr"
	item := widget2{
		widget: widget{
			UserID: 11111,
			Time:   time.Now().UTC(),
		},
		CStr: customString("delete me"),
		SPtr: &str,
	}
	err := table.Put(item).Run()
	if err != nil {
		t.Error("unexpected error:", err)
		t.FailNow()
	}

	// update CStr and SPtr with auto-omitted values, so they should be removed
	var result widget2
	err = table.Update("UserID", item.UserID).Range("Time", item.Time).
		Set("CStr", customString("")).
		Set("SPtr", nil).
		Value(&result)
	if err != nil {
		t.Error("unexpected error:", err)
	}
	expected := widget2{
		widget: item.widget,
		CStr:   customString(""),
		SPtr:   nil,
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("bad result. %+v ≠ %+v", result, expected)
	}
}
