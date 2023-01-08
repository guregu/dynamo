package dynamo

import (
	"reflect"
	"testing"
	"time"
)

type UserAction2 struct {
	UserID string    `dynamo:",hash"`
	Time   time.Time `dynamo:",range" index:"UUID-index,range"`
	Seq    int64     `dynamo:"Count" localIndex:"ID-Seq-index,range"`
	UUID   string    `dynamo:"Msg" index:"UUID-index,hash"`
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
