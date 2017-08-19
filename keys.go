package dynamo

// KeyType is used to specify the type of hash and range keys for tables and indexes.
type KeyType string

// Key types for table and index hash/range keys.
const (
	BinaryType KeyType = "B"
	StringType         = "S"
	NumberType         = "N"
	NoneType           = ""
)

// Keyed provides hash key and range key values.
type Keyed interface {
	HashKey() interface{}
	RangeKey() interface{}
}

// Keys provides an easy way to specify the hash and range keys.
//	table.Batch("ID", "Month").
//		Get([]dynamo.Keys{{1, "2015-10"}, {42, "2015-12"}, {42, "1992-02"}}...).
//		All(&results)
type Keys [2]interface{}

// HashKey returns the hash key's value.
func (k Keys) HashKey() interface{} { return k[0] }

// RangeKey returns the range key's value.
func (k Keys) RangeKey() interface{} { return k[1] }
