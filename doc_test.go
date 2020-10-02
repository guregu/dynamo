package dynamo_test

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/guregu/dynamo"
)

// yourType is a helper type to be used in the examples below
type yourType struct {
	PK   string `dynamo:"pk,hash"`
	SK   string `dynamo:"sk,hash"`
	Data string `dynamo:"data"`
}

// Table Put example shows how to insert (or replace) a new Item
func ExampleTable_Put() {
	// db table setup
	db := dynamo.New(session.New(), &aws.Config{Region: aws.String("region")})
	dynaTable := db.Table("table")

	dynaTable.Put(yourType{PK: "key", SK: "range key", Data: "your data"}).Run()
}

// Table Update example on soft deleting an Item.
// Other examples show how to ignore these soft-deleted Items
func ExampleTable_Update_softdelete() {
	// db table setup
	db := dynamo.New(session.New(), &aws.Config{Region: aws.String("region")})
	dynaTable := db.Table("table")

	dynaTable.Update("pk", "word").Range("sk", "range_key").AddStringsToSet("deleted_at", time.Now().Format(time.RFC3339)).Run()
}

// Table Update on conditional updating
// This will only add 1 to attribute "count" if "count" = 0 or it does not exists
func ExampleTable_Update_conditional() {
	// db table setup
	db := dynamo.New(session.New(), &aws.Config{Region: aws.String("region")})
	dynaTable := db.Table("table")

	dynaTable.Update("pk", "word").Range("sk", "range key").Add("count", 1).If("'count' = ? OR attribute_not_exists('count')", 0).Run()
}

// Table Scan with filter show how to do a table scan taing a sort key into account, and ignoring soft_deleted items
func ExampleTable_Scan_scanfiltered() {
	// db table setup
	db := dynamo.New(session.New(), &aws.Config{Region: aws.String("region")})
	dynaTable := db.Table("table")

	var values []yourType
	dynaTable.Scan().Filter("sk = ? AND attribute_not_exists('deleted_at')", "range key value").All(&values)
	fmt.Println(values)
}

// Table Batch shows how to iterate over several items with different keys
// In this example, the range key is considered the same, but it could be different as well
func ExampleTable_Batch_getkeys() {
	// db table setup
	db := dynamo.New(session.New(), &aws.Config{Region: aws.String("region")})
	dynaTable := db.Table("table")

	// prepare the request with they key pairs
	var wordKeys []dynamo.Keyed
	words := []string{"a", "b", "c"}
	for word := range words {
		wordKeys = append(wordKeys, dynamo.Keys{word, "range key"})
	}

	// here "pk", "sk" are the names of the hash and range key fields in the DynamoDB table
	iterator := dynaTable.Batch("pk", "sk").Get(wordKeys...).Iter()
	var value yourType
	for iterator.Next(&value) {
		fmt.Println(value)
	}
	if err := iterator.Err(); err != nil {
		fmt.Println(err)
	}
}
