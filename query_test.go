package dynamo

import (
	"strconv"
	"testing"
	"time"

	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/gen/dynamodb"
)

type hit struct {
	User      int `dynamo:"UserID"`
	Date      unixTime
	ContentID string
	Page      int
}

func TestGetOne(t *testing.T) {
	creds := aws.DetectCreds("", "", "")
	db := New(creds, "ap-southeast-1", nil)
	hits := db.Table("TestDB")
	q := hits.Get("UserID", 613)
	ct, err := q.Count()
	t.Log("count", ct, err)
	t.Fail()
}

func TestGetAll(t *testing.T) {
	creds := aws.DetectCreds("", "", "")
	db := New(creds, "ap-southeast-1", nil)
	hits := db.Table("TestDB")
	q := hits.Get("UserID", 613)

	var records []hit
	err := q.All(&records)

	t.Logf("all %#v %v", records, err)
	t.Fail()
}

type unixTime struct {
	time.Time
}

var _ Unmarshaler = &unixTime{}

func (ut unixTime) MarshalDynamo() (dynamodb.AttributeValue, error) {
	num := strconv.FormatInt(ut.Unix(), 10)
	av := dynamodb.AttributeValue{
		N: aws.String(num),
	}
	return av, nil
}

func (ut *unixTime) UnmarshalDynamo(av dynamodb.AttributeValue) error {
	sec, err := strconv.ParseInt(*av.N, 10, 64)
	if err != nil {
		return err
	}
	*ut = unixTime{time.Unix(sec, 0)}
	return nil
}
