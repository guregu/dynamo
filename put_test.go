package dynamo

import (
	"testing"
	"time"

	"github.com/awslabs/aws-sdk-go/aws"
	// "github.com/awslabs/aws-sdk-go/gen/dynamodb"
	"github.com/guregu/toki"
)

func TestPutItem(t *testing.T) {
	creds := aws.DetectCreds("", "", "")
	db := New(creds, "ap-southeast-1", nil)
	hits := db.Table("TestDB")
	i := 777777
	h := hit{
		User:      666,
		Date:      unixTime{time.Now()},
		ContentID: "監獄学園",
		Page:      1,
		SkipThis:  "i should disappear",
		Bonus:     &i,
		TestText:  toki.MustParseTime("1:2:3"),
	}

	err := hits.Put(&h)

	t.Log(err)
	t.Fail()
}
