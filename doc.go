// Package dynamo offers a rich DynamoDB client.
/*

dynamo is an expressive client for Go, with an API heavily inspired by mgo. dynamo integrates with the official AWS SDK.

dynamo is still under development, so the API may change rarely. However, breaking changes will be avoided and the API can be considered relatively stable.

*/
//
// Simple Example
//
// This is a simple complete example that you can run
/*
	package dynamo

	import (
		"time"

		"github.com/aws/aws-sdk-go/aws"
		"github.com/aws/aws-sdk-go/aws/session"
		"github.com/guregu/dynamo"
	)

	// Use struct tags much like the standard JSON library,
	// you can embed anonymous structs too!
	type widget struct {
		UserID int       // Hash key, a.k.a. partition key
		Time   time.Time // Range key, a.k.a. sort key

		Msg       string              `dynamo:"Message"`
		Count     int                 `dynamo:",omitempty"`
		Friends   []string            `dynamo:",set"` // Sets
		Set       map[string]struct{} `dynamo:",set"` // Map sets, too!
		SecretKey string              `dynamo:"-"`    // Ignored
		Category  string              `dynamo:"Category"` // Global Secondary Index
		Children  []any               // Lists
	}


	func main() {
		db := dynamo.New(session.New(), &aws.Config{Region: aws.String("us-west-2")})
		table := db.Table("Widgets")

		// put item
		w := widget{UserID: 613, Time: time.Now(), Msg: "hello"}
		err := table.Put(w).Run()

		// update item field
		w.Msg = "hello again"
		m, err := dynamo.MarshalItem(w)
		err = table.Update("UserID", m["UserID"]).
			Set("Msg", m["Msg"]).
			Run()

		// get the same item
		var result widget
		err = table.Get("UserID", w.UserID).
			Range("Time", dynamo.Equal, w.Time).
			Filter("'Count' = ? AND $ = ?", w.Count, "Message", w.Msg). // placeholders in expressions
			One(&result)

		// get by index
		err = table.Get("Category", "hoge").
			Index("category-index").
			One(&result)

		// get all items
		var results []widget
		err = table.Scan().All(&results)
	}
*/
//
// Struct Tags Example
//
// This example shows the usage of tags to define the keys and rename fields
// Use `hash` for the hash key, and `range` or `sort` for the range/sort key
/*
	type yourType struct {
		ID   string `dynamo:"pk,hash"`
		Timestamp   string `dynamo:"sk,hash"`
		Data string `dynamo:"data"`
	}
*/
package dynamo
