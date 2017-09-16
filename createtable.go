package dynamo

import (
	"encoding"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// StreamView determines what information is written to a table's stream.
type StreamView string

var (
	// Only the key attributes of the modified item are written to the stream.
	KeysOnlyView StreamView = dynamodb.StreamViewTypeKeysOnly
	// The entire item, as it appears after it was modified, is written to the stream.
	NewImageView StreamView = dynamodb.StreamViewTypeNewImage
	// The entire item, as it appeared before it was modified, is written to the stream.
	OldImageView StreamView = dynamodb.StreamViewTypeOldImage
	// Both the new and the old item images of the item are written to the stream.
	NewAndOldImagesView StreamView = dynamodb.StreamViewTypeNewAndOldImages
)

// IndexProjection determines which attributes are mirrored into indices.
type IndexProjection string

var (
	// Only the key attributes of the modified item are written to the stream.
	KeysOnlyProjection IndexProjection = dynamodb.ProjectionTypeKeysOnly
	// All of the table attributes are projected into the index.
	AllProjection IndexProjection = dynamodb.ProjectionTypeAll
	// Only the specified table attributes are projected into the index.
	IncludeProjection IndexProjection = dynamodb.ProjectionTypeInclude
)

// CreateTable is a request to create a new table.
// See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html
type CreateTable struct {
	db            *DB
	tableName     string
	attribs       []*dynamodb.AttributeDefinition
	schema        []*dynamodb.KeySchemaElement
	globalIndices map[string]dynamodb.GlobalSecondaryIndex
	localIndices  map[string]dynamodb.LocalSecondaryIndex
	readUnits     int64
	writeUnits    int64
	streamView    StreamView
	err           error
}

// CreateTable begins a new operation to create a table with the given name.
// The second parameter must be a struct with appropriate hash and range key struct tags
// for the primary key and all indices.
//
// An example of a from struct follows:
// 	type UserAction struct {
// 		UserID string    `dynamo:"ID,hash" index:"Seq-ID-index,range"`
// 		Time   time.Time `dynamo:",range"`
// 		Seq    int64     `localIndex:"ID-Seq-index,range" index:"Seq-ID-index,hash"`
// 		UUID   string    `index:"UUID-index,hash"`
// 	}
// This creates a table with the primary hash key ID and range key Time.
// It creates two global secondary indices called UUID-index and Seq-ID-index,
// and a local secondary index called ID-Seq-index.
func (db *DB) CreateTable(name string, from interface{}) *CreateTable {
	ct := &CreateTable{
		db:            db,
		tableName:     name,
		schema:        []*dynamodb.KeySchemaElement{},
		globalIndices: make(map[string]dynamodb.GlobalSecondaryIndex),
		localIndices:  make(map[string]dynamodb.LocalSecondaryIndex),
		readUnits:     1,
		writeUnits:    1,
	}
	rv := reflect.ValueOf(from)
	ct.setError(ct.from(rv))
	return ct
}

// Provision specifies the provisioned read and write capacity for this table.
// If Provision isn't called, the table will be created with 1 unit each.
func (ct *CreateTable) Provision(readUnits, writeUnits int64) *CreateTable {
	ct.readUnits, ct.writeUnits = readUnits, writeUnits
	return ct
}

// ProvisionIndex specifies the provisioned read and write capacity for the given
// global secondary index. Local secondary indices share their capacity with the table.
func (ct *CreateTable) ProvisionIndex(index string, readUnits, writeUnits int64) *CreateTable {
	idx := ct.globalIndices[index]
	idx.ProvisionedThroughput = &dynamodb.ProvisionedThroughput{
		ReadCapacityUnits:  &readUnits,
		WriteCapacityUnits: &writeUnits,
	}
	ct.globalIndices[index] = idx
	return ct
}

// Stream enables DynamoDB Streams for this table which the specified type of view.
// Streams are disabled by default.
func (ct *CreateTable) Stream(view StreamView) *CreateTable {
	ct.streamView = view
	return ct
}

// Project specifies the projection type for the given table.
// When using IncludeProjection, you must specify the additional attributes to include via includeAttribs.
func (ct *CreateTable) Project(index string, projection IndexProjection, includeAttribs ...string) *CreateTable {
	projectionStr := string(projection)
	proj := &dynamodb.Projection{
		ProjectionType: &projectionStr,
	}
	if projection == IncludeProjection {
		for _, attr := range includeAttribs {
			proj.NonKeyAttributes = append(proj.NonKeyAttributes, &attr)
		}
	}
	if idx, global := ct.globalIndices[index]; global {
		idx.Projection = proj
		ct.globalIndices[index] = idx
	} else if localIdx, ok := ct.localIndices[index]; ok {
		localIdx.Projection = proj
		ct.localIndices[index] = localIdx
	} else {
		ct.setError(fmt.Errorf("dynamo: no such index: %s", index))
	}
	return ct
}

// Index specifies an index to add to this table.
func (ct *CreateTable) Index(index Index) *CreateTable {
	ct.add(index.HashKey, string(index.HashKeyType))
	ks := []*dynamodb.KeySchemaElement{
		&dynamodb.KeySchemaElement{
			AttributeName: &index.HashKey,
			KeyType:       aws.String(string(index.HashKeyType)),
		},
	}
	if index.RangeKey != "" {
		ct.add(index.RangeKey, string(index.RangeKeyType))
		ks = append(ks, &dynamodb.KeySchemaElement{
			AttributeName: &index.RangeKey,
			KeyType:       aws.String(string(index.RangeKeyType)),
		})
	}

	var proj *dynamodb.Projection
	if index.ProjectionType != "" {
		proj = &dynamodb.Projection{
			ProjectionType: aws.String((string)(index.ProjectionType)),
		}
		if index.ProjectionType == IncludeProjection {
			proj.NonKeyAttributes = aws.StringSlice(index.ProjectionAttribs)
		}
	}

	if index.Local {
		idx := ct.localIndices[index.Name]
		idx.KeySchema = ks
		if proj != nil {
			idx.Projection = proj
		}
		ct.localIndices[index.Name] = idx
		return ct
	}

	idx := ct.globalIndices[index.Name]
	idx.KeySchema = ks
	if index.Throughput.Read != 0 || index.Throughput.Write != 0 {
		idx.ProvisionedThroughput = &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  &index.Throughput.Read,
			WriteCapacityUnits: &index.Throughput.Write,
		}
	}
	if proj != nil {
		idx.Projection = proj
	}
	ct.globalIndices[index.Name] = idx
	return ct
}

// Run creates this table or returns and error.
func (ct *CreateTable) Run() error {
	ctx, cancel := defaultContext()
	defer cancel()
	return ct.RunWithContext(ctx)
}

func (ct *CreateTable) RunWithContext(ctx aws.Context) error {
	if ct.err != nil {
		return ct.err
	}

	input := ct.input()
	return retry(ctx, func() error {
		_, err := ct.db.client.CreateTableWithContext(ctx, input)
		return err
	})
}

func (ct *CreateTable) from(rv reflect.Value) error {
	switch rv.Kind() {
	case reflect.Struct: // ok
	case reflect.Ptr:
		return ct.from(rv.Elem())
	default:
		return fmt.Errorf("dynamo: CreateTable example must be a struct")
	}

	for i := 0; i < rv.Type().NumField(); i++ {
		field := rv.Type().Field(i)
		fv := rv.Field(i)

		name, _, _ := fieldInfo(field)
		if name == "-" {
			// skip
			continue
		}

		// inspect anonymous structs
		if fv.Type().Kind() == reflect.Struct && field.Anonymous {
			if err := ct.from(fv); err != nil {
				return err
			}
		}

		// primary keys
		if keyType := keyTypeFromTag(field.Tag.Get("dynamo")); keyType != "" {
			ct.add(name, typeOf(fv))
			ct.schema = append(ct.schema, &dynamodb.KeySchemaElement{
				AttributeName: &name,
				KeyType:       &keyType,
			})
		}

		// global secondary index
		if gsi, ok := tagLookup(string(field.Tag), "index"); ok {
			for _, index := range gsi {
				ct.add(name, typeOf(fv))
				keyType := keyTypeFromTag(index)
				indexName := index[:len(index)-len(keyType)-1]
				idx := ct.globalIndices[indexName]
				idx.KeySchema = append(idx.KeySchema, &dynamodb.KeySchemaElement{
					AttributeName: &name,
					KeyType:       &keyType,
				})
				ct.globalIndices[indexName] = idx
			}
		}

		// local secondary index
		if lsi, ok := tagLookup(string(field.Tag), "localIndex"); ok {
			for _, localIndex := range lsi {
				ct.add(name, typeOf(fv))
				keyType := keyTypeFromTag(localIndex)
				indexName := localIndex[:len(localIndex)-len(keyType)-1]
				idx := ct.localIndices[indexName]
				idx.KeySchema = append(idx.KeySchema, &dynamodb.KeySchemaElement{
					AttributeName: &name,
					KeyType:       &keyType,
				})
				ct.localIndices[indexName] = idx
			}
		}
	}

	return nil
}

func (ct *CreateTable) input() *dynamodb.CreateTableInput {
	sortKeySchemas(ct.schema)
	input := &dynamodb.CreateTableInput{
		TableName:            &ct.tableName,
		AttributeDefinitions: ct.attribs,
		KeySchema:            ct.schema,
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  &ct.readUnits,
			WriteCapacityUnits: &ct.writeUnits,
		},
	}
	if ct.streamView != "" {
		enabled := true
		view := string(ct.streamView)
		input.StreamSpecification = &dynamodb.StreamSpecification{
			StreamEnabled:  &enabled,
			StreamViewType: &view,
		}
	}
	for name, idx := range ct.localIndices {
		name, idx := name, idx
		idx.IndexName = &name
		if idx.Projection == nil {
			all := string(AllProjection)
			idx.Projection = &dynamodb.Projection{
				ProjectionType: &all,
			}
		}
		// add the primary hash key
		if len(idx.KeySchema) == 1 {
			idx.KeySchema = []*dynamodb.KeySchemaElement{
				ct.schema[0],
				idx.KeySchema[0],
			}
		}
		sortKeySchemas(idx.KeySchema)
		input.LocalSecondaryIndexes = append(input.LocalSecondaryIndexes, &idx)
	}
	for name, idx := range ct.globalIndices {
		name, idx := name, idx
		idx.IndexName = &name
		if idx.Projection == nil {
			all := string(AllProjection)
			idx.Projection = &dynamodb.Projection{
				ProjectionType: &all,
			}
		}
		if idx.ProvisionedThroughput == nil {
			units := int64(1)
			idx.ProvisionedThroughput = &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  &units,
				WriteCapacityUnits: &units,
			}
		}
		sortKeySchemas(idx.KeySchema)
		input.GlobalSecondaryIndexes = append(input.GlobalSecondaryIndexes, &idx)
	}
	return input
}

func (ct *CreateTable) add(name string, typ string) {
	if typ == "" {
		ct.setError(fmt.Errorf("dynamo: invalid type for key: %s", name))
		return
	}

	for _, attr := range ct.attribs {
		if *attr.AttributeName == name {
			return
		}
	}

	ct.attribs = append(ct.attribs, &dynamodb.AttributeDefinition{
		AttributeName: &name,
		AttributeType: &typ,
	})
}

func (ct *CreateTable) setError(err error) {
	if ct.err == nil {
		ct.err = err
	}
}

func typeOf(rv reflect.Value) string {
	if rv.CanInterface() {
		switch x := rv.Interface().(type) {
		case Marshaler:
			if av, err := x.MarshalDynamo(); err == nil {
				if iface, err := av2iface(av); err == nil {
					return typeOf(reflect.ValueOf(iface))
				}
			}
		case encoding.TextMarshaler:
			return "S"
		}
	}

	typ := rv.Type()
check:
	switch typ.Kind() {
	case reflect.Ptr:
		typ = typ.Elem()
		goto check
	case reflect.String:
		return "S"
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16,
		reflect.Int8, reflect.Float64, reflect.Float32:
		return "N"
	case reflect.Slice, reflect.Array:
		if typ.Elem().Kind() == reflect.Uint8 {
			return "B"
		}
	}

	return ""
}

func keyTypeFromTag(tag string) string {
	for _, v := range strings.Split(tag, ",") {
		switch v {
		case "hash", "partition":
			return dynamodb.KeyTypeHash
		case "range", "sort":
			return dynamodb.KeyTypeRange
		}
	}
	return ""
}

func sortKeySchemas(schemas []*dynamodb.KeySchemaElement) {
	if *schemas[0].KeyType == dynamodb.KeyTypeRange {
		schemas[0], schemas[1] = schemas[1], schemas[0]
	}
}

// ripped from the stdlib
// Copyright 2009 The Go Authors. All rights reserved.
func tagLookup(tag, key string) (value []string, ok bool) {
	for tag != "" {
		// Skip leading space.
		i := 0
		for i < len(tag) && tag[i] == ' ' {
			i++
		}
		tag = tag[i:]
		if tag == "" {
			break
		}

		// Scan to colon. A space, a quote or a control character is a syntax error.
		// Strictly speaking, control chars include the range [0x7f, 0x9f], not just
		// [0x00, 0x1f], but in practice, we ignore the multi-byte control characters
		// as it is simpler to inspect the tag's bytes than the tag's runes.
		i = 0
		for i < len(tag) && tag[i] > ' ' && tag[i] != ':' && tag[i] != '"' && tag[i] != 0x7f {
			i++
		}
		if i == 0 || i+1 >= len(tag) || tag[i] != ':' || tag[i+1] != '"' {
			break
		}
		name := string(tag[:i])
		tag = tag[i+1:]

		// Scan quoted string to find value.
		i = 1
		for i < len(tag) && tag[i] != '"' {
			if tag[i] == '\\' {
				i++
			}
			i++
		}
		if i >= len(tag) {
			break
		}
		qvalue := string(tag[:i+1])
		tag = tag[i+1:]

		if key == name {
			v, err := strconv.Unquote(qvalue)
			if err != nil {
				break
			}
			value = append(value, v)
		}
	}
	return value, len(value) > 0
}
