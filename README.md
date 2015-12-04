## dynamo [![GoDoc](https://godoc.org/github.com/guregu/dynamo?status.svg)](https://godoc.org/github.com/guregu/dynamo) [![Circle CI](https://circleci.com/gh/guregu/dynamo.svg?style=svg)](https://circleci.com/gh/guregu/dynamo)
`import "github.com/guregu/dynamo"` 

dynamo is an expressive [DynamoDB](https://aws.amazon.com/dynamodb/) client for Go, with an API heavily inspired by [mgo](https://labix.org/mgo). dynamo uses the official [AWS SDK](https://github.com/aws/aws-sdk-go/) for sending requests.

dynamo is still under development, so the API may change!


### Example

```go
package dynamo

import (
	"os"
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
	
	Msg       string   `dynamo:"Message"`
	Count     int      `dynamo:",omitempty"`
	Friends   []string `dynamo:",set"` // Sets 
	SecretKey string   `dynamo:"-"` // Ignored
}

func main() {
	db := dynamo.New(session.New(), &aws.Config{Region: aws.String("us-west-2")})
	table := db.Table("Widgets")
	
	// put item
	w := widget{UserID: 613, Time: time.Now(), Msg: "hello"}
	err := table.Put(w).Run() 
	
	// get the same item 
	var result widget
	err = table.Get("UserID", w.UserID).
				Range("Time", dynamo.Equal, w.Time).
				Filter("'Count' = ? AND $ = ?", w.Count, "Message", w.Msg). // placeholders in expressions
				One(&result)
	
	// get all items
	var results []widget
	err = table.Scan().All(&results)
	// ...
}
```

### Integration tests

By default, tests are run in offline mode. Create a table called `TestDB`, with a Number Parition Key called `UserID` and a String Sort Key called `Time`. Change the table name with the environment variable `DYNAMO_TEST_TABLE`. You must specify `DYNAMO_TEST_REGION`, setting it to the AWS region where your test table is.

 ```bash
DYNAMO_TEST_REGION=us-west-2 go test github.com/guregu/dynamo/... -cover
 ``` 

### License

BSD