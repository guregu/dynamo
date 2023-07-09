## dynamo [![GoDoc](https://godoc.org/github.com/guregu/dynamo?status.svg)](https://godoc.org/github.com/guregu/dynamo)
`import "github.com/guregu/dynamo"`

dynamo is an expressive [DynamoDB](https://aws.amazon.com/dynamodb/) client for Go, with an easy but powerful API. dynamo integrates with the official [AWS SDK](https://github.com/aws/aws-sdk-go/).

This library is stable and versioned with Go modules.

### Example

```go
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

	Msg       string              `dynamo:"Message"`    // Change name in the database
	Count     int                 `dynamo:",omitempty"` // Omits if zero value
	Children  []widget            // List of maps
	Friends   []string            `dynamo:",set"` // Sets
	Set       map[string]struct{} `dynamo:",set"` // Map sets, too!
	SecretKey string              `dynamo:"-"`    // Ignored
}


func main() {
	sess := session.Must(session.NewSession())
	db := dynamo.New(sess, &aws.Config{Region: aws.String("us-west-2")})
	table := db.Table("Widgets")

	// put item
	w := widget{UserID: 613, Time: time.Now(), Msg: "hello"}
	err := table.Put(w).Run()

	// get the same item
	var result widget
	err = table.Get("UserID", w.UserID).
		Range("Time", dynamo.Equal, w.Time).
		One(&result)

	// get all items
	var results []widget
	err = table.Scan().All(&results)

	// use placeholders in filter expressions (see Expressions section below)
	var filtered []widget
	err = table.Scan().Filter("'Count' > ?", 10).All(&filtered)
}
```

### Expressions

dynamo will help you write expressions used to filter results in queries and scans, and add conditions to puts and deletes. 

Attribute names may be written as is if it is not a reserved word, or be escaped with single quotes (`''`). You may also use dollar signs (`$`) as placeholders for attribute names and list indexes. DynamoDB has [very large amount of reserved words](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html) so it may be a good idea to just escape everything.

Question marks (`?`) are used as placeholders for attribute values. DynamoDB doesn't have value literals, so you need to substitute everything.

Please see the [DynamoDB reference on expressions](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference) for more information. The [Comparison Operator and Function Reference](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html) is also handy.

```go
// Using single quotes to escape a reserved word, and a question mark as a value placeholder.
// Finds all items whose date is greater than or equal to lastUpdate.
table.Scan().Filter("'Date' >= ?", lastUpdate).All(&results)

// Using dollar signs as a placeholder for attribute names.
// Deletes the item with an ID of 42 if its score is at or below the cutoff, and its name starts with G.
table.Delete("ID", 42).If("Score <= ? AND begins_with($, ?)", cutoff, "Name", "G").Run()

// Put a new item, only if it doesn't already exist.
table.Put(item{ID: 42}).If("attribute_not_exists(ID)").Run()
```

### Encoding support

dynamo automatically handles the following interfaces:

* [`dynamo.Marshaler`](https://godoc.org/github.com/guregu/dynamo#Marshaler) and [`dynamo.Unmarshaler`](https://godoc.org/github.com/guregu/dynamo#Unmarshaler)
* [`dynamodbattribute.Marshaler`](https://godoc.org/github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute#Marshaler) and [`dynamodbattribute.Unmarshaler`](https://godoc.org/github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute#Unmarshaler)
* [`encoding.TextMarshaler`](https://godoc.org/encoding#TextMarshaler) and [`encoding.TextUnmarshaler`](https://godoc.org/encoding#TextUnmarshaler)

This allows you to define custom encodings and provides built-in support for types such as `time.Time`.

### Struct tags and fields

dynamo handles struct tags similarly to the standard library `encoding/json` package. It uses `dynamo` for the struct tag's name, taking the form of: `dynamo:"attributeName,option1,option2,etc"`. You can omit the attribute name to use the default: `dynamo:",option1,etc"`.

#### Renaming

By default, dynamo will use the name of your fields as the name of the DynamoDB attribute it corresponds do. You can specify a different name with the `dynamo` struct tag like so: `dynamo:"other_name_goes_here"`. If two fields have the same name, dynamo will prioritize the higher-level field.

#### Omission

If you set a field's name to `"-"` (as in `dynamo:"-"`) that field will be ignored. It will be omitted when marshaling and ignored when unmarshaling. Also, fields that start with a lowercase letter will be ignored. However, embedding a struct whose type has a lowercase letter but contains uppercase fields is OK.

#### Sets

By default, slices will be marshaled as DynamoDB lists. To marshal a field to sets instead, use the `dynamo:",set"` option. Empty sets will be automatically omitted.

You can use maps as sets too. The following types are supported:

- `[]T`
- `map[T]struct{}`
- `map[T]bool`

where `T` represents any type that marshals into a DynamoDB string, number, or binary value. 

Note that the order of objects within a set is undefined.

#### Omitting empty values (omitempty)

Using the **omitempty** option (as in `dynamo:",omitempty"`) will omit the field if it has a zero (ex. an empty string, 0, nil pointer) value. Structs are supported. 

It also supports the `isZeroer` interface below:

```go
type isZeroer interface {
	IsZero() bool
}
```

If `IsZero()` returns true, the field will be omitted. This gives us built-in support for `time.Time`. 

You can also use the `dynamo:",omitemptyelem"` option to omit empty values inside of slices.

#### Automatic omission

Some values will be automatically omitted.

- Empty strings
- Empty sets
- Empty structs
- Nil pointers and interfaces
- Types that implement `encoding.TextMarshaler` and whose `MarshalText` method returns 0-length or nil slice.
- Zero-length binary (byte slices)

To override this behavior, use the `dynamo:",allowempty"` flag. Not all empty types can be stored by DynamoDB. For example, empty sets will still be omitted.

To override auto-omit behavior for children of a map, for example `map[string]string`, use the `dynamo:",allowemptyelem"` option.

#### Using the NULL type

DynamoDB has a special NULL type to represent null values. In general, this library avoids marshaling things as NULL and prefers to omit those values instead. If you want empty/nil values to marshal to NULL, use the `dynamo:",null"` option.

#### Unix time

By default, `time.Time` will marshal to a string because it implements `encoding.TextMarshaler`.

If you want `time.Time` to marshal as a Unix time value (number of seconds since the Unix epoch), you can use the `dynamo:",unixtime"` option. This is useful for TTL fields, which must be Unix time.

### Creating tables

You can use struct tags to specify hash keys, range keys, and indexes when creating a table.

For example:

```go
type UserAction struct {
	UserID string    `dynamo:"ID,hash" index:"Seq-ID-index,range"`
	Time   time.Time `dynamo:",range"`
	Seq    int64     `localIndex:"ID-Seq-index,range" index:"Seq-ID-index,hash"`
	UUID   string    `index:"UUID-index,hash"`
}
```

This creates a table with the primary hash key ID and range key Time. It creates two global secondary indices called UUID-index and Seq-ID-index, and a local secondary index called ID-Seq-index.


### Compatibility with the official AWS library

dynamo has been in development before the official AWS libraries were stable. We use a different encoder and decoder than the [dynamodbattribute](https://godoc.org/github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute) package. dynamo uses the `dynamo` struct tag instead of the `dynamodbav` struct tag, and we also prefer to automatically omit invalid values such as empty strings, whereas the dynamodbattribute package substitutes null values for them. Items that satisfy the [`dynamodbattribute.(Un)marshaler`](https://godoc.org/github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute#Marshaler) interfaces are compatibile with both libraries.

In order to use dynamodbattribute's encoding facilities, you must wrap objects passed to dynamo with [`dynamo.AWSEncoding`](https://godoc.org/github.com/guregu/dynamo#AWSEncoding). Here is a quick example:

```go
// Notice the use of the dynamodbav struct tag
type book struct {
	ID    int    `dynamodbav:"id"`
	Title string `dynamodbav:"title"`
}
// Putting an item
err := db.Table("Books").Put(dynamo.AWSEncoding(book{
	ID:    42,
	Title: "Principia Discordia",
})).Run()
// When getting an item you MUST pass a pointer to AWSEncoding!
var someBook book
err := db.Table("Books").Get("ID", 555).One(dynamo.AWSEncoding(&someBook))
```

### Integration tests

By default, tests are run in offline mode. Create a table called `TestDB`, with a number partition key called `UserID` and a string sort key called `Time`. It also needs a Global Secondary Index called `Msg-Time-index` with a string partition key called `Msg` and a string sort key called `Time`.

Change the table name with the environment variable `DYNAMO_TEST_TABLE`. You must specify `DYNAMO_TEST_REGION`, setting it to the AWS region where your test table is.


 ```bash
DYNAMO_TEST_REGION=us-west-2 go test github.com/guregu/dynamo/... -cover
 ```

If you want to use [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html) to run local tests, specify `DYNAMO_TEST_ENDPOINT`.

 ```bash
DYNAMO_TEST_REGION=us-west-2 DYNAMO_TEST_ENDPOINT=http://localhost:8000 go test github.com/guregu/dynamo/... -cover
 ```

Example of using [aws-cli](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.CLI.html) to create a table for testing.

```bash
aws dynamodb create-table \
    --table-name TestDB \
    --attribute-definitions \
        AttributeName=UserID,AttributeType=N \
        AttributeName=Time,AttributeType=S \
        AttributeName=Msg,AttributeType=S \
    --key-schema \
        AttributeName=UserID,KeyType=HASH \
        AttributeName=Time,KeyType=RANGE \
    --global-secondary-indexes \
        IndexName=Msg-Time-index,KeySchema=[{'AttributeName=Msg,KeyType=HASH'},{'AttributeName=Time,KeyType=RANGE'}],Projection={'ProjectionType=ALL'} \
    --billing-mode PAY_PER_REQUEST \
    --region us-west-2 \
    --endpoint-url http://localhost:8000 # using DynamoDB local
```

### License

BSD 2-Clause
