package dynamo

import (
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func TestPut(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	table := testDB.Table(testTable)

	type widget2 struct {
		widget
		List []*string
		Set1 []string            `dynamo:",set"`
		Set2 map[string]struct{} `dynamo:",set"`
		Map1 map[string]string
		Map2 map[string]*string
	}

	now := time.Now().UTC()
	item := widget2{
		widget: widget{
			UserID: 42,
			Time:   now,
			Msg:    "old",
			StrPtr: new(string),
		},
		List: []*string{},
	}

	err := table.Put(item).Run()
	if err != nil {
		t.Error("unexpected error:", err)
	}

	newItem := widget2{
		widget: widget{
			UserID: 42,
			Time:   now,
			Msg:    "new",
		},
		List: []*string{aws.String("abc"), aws.String(""), aws.String("def"), nil, aws.String("ghi")},
		Set1: []string{"A", "B", ""},
		Set2: map[string]struct{}{"C": {}, "D": {}, "": {}},
		Map1: map[string]string{"A": "hello", "B": ""},
		Map2: map[string]*string{"C": aws.String("world"), "D": nil, "E": aws.String("")},
	}
	var oldValue widget2
	var cc ConsumedCapacity
	err = table.Put(newItem).ConsumedCapacity(&cc).OldValue(&oldValue)
	if err != nil {
		t.Error("unexpected error:", err)
	}

	if !reflect.DeepEqual(oldValue, item) {
		t.Errorf("bad old value. %#v ≠ %#v", oldValue, item)
	}

	if cc.Total < 1 || cc.Table < 1 || cc.TableName != testTable {
		t.Errorf("bad consumed capacity: %#v", cc)
	}

	// putting the same item: this should fail
	err = table.Put(newItem).If("attribute_not_exists(UserID)").If("attribute_not_exists('Time')").Run()
	if !IsCondCheckFailed(err) {
		t.Error("expected ConditionalCheckFailedException, not", err)
	}
}

func TestPutAndQueryAWSEncoding(t *testing.T) {
	if testDB == nil {
		t.Skip(offlineSkipMsg)
	}
	table := testDB.Table(testTable)

	type awsWidget struct {
		XUserID int               `dynamodbav:"UserID"`
		XTime   string            `dynamodbav:"Time"`
		XMsg    string            `dynamodbav:"Msg"`
		XCount  int               `dynamodbav:"Count"`
		XMeta   map[string]string `dynamodbav:"Meta"`
	}

	now := time.Now().UTC()
	nowtext, err := now.MarshalText()
	if err != nil {
		t.Error(err)
	}
	item := awsWidget{
		XUserID: -1,
		XTime:   string(nowtext),
		XMsg:    "hello world",
	}

	err = table.Put(AWSEncoding(item)).Run()
	if err != nil {
		t.Error(err)
	}

	var result awsWidget
	err = table.Get("UserID", item.XUserID).Range("Time", Equal, item.XTime).Consistent(true).One(AWSEncoding(&result))
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(item, result) {
		t.Errorf("bad aws put/get result. %#v ≠ %#v", item, result)
	}

	var list []awsWidget
	err = table.Get("UserID", item.XUserID).Consistent(true).All(AWSEncoding(&list))
	if err != nil {
		t.Error(err)
	}
	found := false
	for _, x := range list {
		if reflect.DeepEqual(x, item) {
			found = true
			break
		}
	}
	t.Log("awsWidget All: got", len(list), "total.", list)
	if !found {
		t.Error("couldn't find awsWidget in All")
	}
}
