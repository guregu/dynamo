package dynamo

import (
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type UserAction2 struct {
	UserID string    `dynamo:",hash"`
	Time   time.Time `dynamo:",range" index:"UUID-index,range"`
	Seq    int64     `dynamo:"Count" localIndex:"ID-Seq-index,range"`
	UUID   string    `dynamo:"Msg" index:"UUID-index,hash" localIndex:"ID-UUID-index,range"`
	embeddedWithKeys
}

func TestMockOne(t *testing.T) {
	var (
		db       = NewMockDB()
		now      = time.Now().UTC()
		str      = "str"
		testData = []interface{}{
			widget{
				UserID: 111,
				Time:   now,
				Msg:    "message",
				Meta: map[string]string{
					"key1": "value1",
					"key2": "value2",
				},
				StrPtr: &str,
			},
			widget{
				UserID: 111,
				Time:   now.Add(1 * time.Second),
				Count:  1,
			},
			widget{
				UserID: 222,
				Time:   now,
			},
			widget{
				UserID: 333,
				Count:  1,
			},
			widget{
				UserID: 333,
				Count:  2,
			},
			widget{
				Msg:  "uuid1",
				Time: now.Add(-1 * time.Second),
			},
			widget{
				Msg:  "uuid1",
				Time: now.Add(1 * time.Second),
			},
		}
	)

	table, err := db.MockTable(UserAction2{}, testData)
	if err != nil {
		t.Error("unexpected error:", err)
	}

	tests := []struct {
		name  string
		query *Query
		want  widget
	}{
		{
			name:  "get by hashkey and rangekey",
			query: table.Get("UserID", 111).Range("Time", Equal, now),
			want:  testData[0].(widget),
		},
		{
			name:  "get by hashkey only",
			query: table.Get("UserID", 222),
			want:  testData[2].(widget),
		},
		{
			name:  "get by local index",
			query: table.Get("UserID", 333).Range("Count", Less, 2).Index("ID-Seq-index"),
			want:  testData[3].(widget),
		},
		{
			name:  "get by global index",
			query: table.Get("Msg", "uuid1").Range("Time", GreaterOrEqual, now).Index("UUID-index"),
			want:  testData[6].(widget),
		},
		{
			name:  "get sorted one",
			query: table.Get("UserID", 111).Order(Descending),
			want:  testData[1].(widget),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var got widget
			err = test.query.One(&got)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Error("bad value. want:", test.want, "got:", got)
			}
		})
	}
}

func TestMockNext(t *testing.T) {
	var (
		db       = NewMockDB()
		now      = time.Now().UTC()
		testData = []interface{}{
			&widget{
				UserID: 111,
				Time:   now,
				Count:  1,
				Msg:    "message",
			},
			&widget{
				UserID: 111,
				Time:   now.Add(3 * time.Second),
				Count:  3,
			},
			&widget{
				UserID: 111,
				Time:   now.Add(2 * time.Second),
				Count:  2,
			},
			&widget{
				UserID: 111,
				Time:   now.Add(5 * time.Second),
				Count:  5,
				Meta: map[string]string{
					"key1": "value1",
					"key2": "value2",
				},
			},
			&widget{
				UserID: 111,
				Time:   now.Add(4 * time.Second),
				Count:  4,
			},
		}
	)

	table, err := db.MockTable(UserAction2{}, testData)
	if err != nil {
		t.Error("unexpected error:", err)
	}

	t.Run("iterate all", func(t *testing.T) {
		itr := table.Get("UserID", 111).Range("Count", Between, 2, 4).Index("ID-Seq-index").Order(Ascending).Iter()

		var w *widget
		expectedIndexes := []int{2, 1, 4}
		for itr.Next(&w) {
			if !reflect.DeepEqual(w, testData[expectedIndexes[0]]) {
				t.Error("bad result:", w, "≠", testData[expectedIndexes[0]])
			}
			if itr.Err() != nil {
				t.Error("unexpected error", itr.Err())
			}
			expectedIndexes = expectedIndexes[1:]
		}

		if itr.Err() != nil {
			t.Error("unexpected error", itr.Err())
		}
	})

	t.Run("limit", func(t *testing.T) {
		limit := 2
		itr := table.Get("UserID", 111).Limit(int64(limit)).Iter()

		var (
			w       *widget
			counter int
		)
		for itr.Next(&w) {
			if !reflect.DeepEqual(w, testData[counter]) {
				t.Error("bad result:", w, "≠", testData[counter])
			}
			if itr.Err() != nil {
				t.Error("unexpected error", itr.Err())
			}
			counter++
		}

		if itr.Err() != nil {
			t.Error("unexpected error", itr.Err())
		}

		if counter != limit {
			t.Error("invalid limit")
		}

		lastKey := itr.LastEvaluatedKey()
		time, _ := testData[limit-1].(*widget).Time.MarshalText()
		expectedKey := PagingKey(map[string]*dynamodb.AttributeValue{
			"Time":   {S: aws.String(string(time))},
			"UserID": {N: aws.String(strconv.Itoa(testData[limit-1].(*widget).UserID))},
		})
		if !reflect.DeepEqual(expectedKey, lastKey) {
			t.Error("bad result:", expectedKey, "≠", lastKey)
		}
	})

	t.Run("startfrom", func(t *testing.T) {
		itr := table.Get("UserID", 111).Iter()

		var w *widget
		itr.Next(&w)
		lastKey := itr.LastEvaluatedKey()

		itr = table.Get("UserID", 111).StartFrom(lastKey).Iter()
		itr.Next(&w)

		if itr.Err() != nil {
			t.Error("unexpected error", itr.Err())
		}

		if !reflect.DeepEqual(w, testData[1]) {
			t.Error("bad result:", w, "≠", testData[1])
		}
	})

}

func TestMockAll(t *testing.T) {
	var (
		db       = NewMockDB()
		now      = time.Now().UTC()
		testData = []interface{}{
			widget{
				UserID: 111,
				Time:   now,
			},
			widget{
				UserID: 111,
				Time:   now.Add(1 * time.Second),
			},
			widget{
				UserID: 222,
				Msg:    "hello",
			},
			widget{
				UserID: 222,
				Msg:    "prefix hello",
			},
			widget{
				UserID: 222,
				Msg:    "prefix world",
			},
			widget{
				Msg:  "uuid1",
				Time: now.Add(-1 * time.Second),
			},
			widget{
				Msg:  "uuid1",
				Time: now.Add(1 * time.Second),
			},
			widget{
				Msg:  "uuid1",
				Time: now,
			},
		}
	)

	table, err := db.MockTable(UserAction2{}, testData)
	if err != nil {
		t.Error("unexpected error:", err)
	}

	tests := []struct {
		name  string
		query *Query
		want  []widget
	}{
		{
			name:  "gall by hashkey and rangekey",
			query: table.Get("UserID", 111),
			want: []widget{
				testData[0].(widget),
				testData[1].(widget),
			},
		},
		{
			name:  "all by local index",
			query: table.Get("UserID", 222).Range("Msg", BeginsWith, "prefix").Index("ID-UUID-index"),
			want: []widget{
				testData[3].(widget),
				testData[4].(widget),
			},
		},
		{
			name:  "all by global index",
			query: table.Get("Msg", "uuid1").Range("Time", LessOrEqual, now).Index("UUID-index"),
			want: []widget{
				testData[5].(widget),
				testData[7].(widget),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var got []widget
			err = test.query.All(&got)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Error("bad value. want:", test.want, "got:", got)
			}
		})
	}
}
