package dynamo

import (
	"encoding"
	"fmt"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// StreamView determines what information is written to a table's stream.
type StreamView string

var (
	// KeysOnly: Only the key attributes of the modified item are written to the stream.
	KeysOnlyView StreamView = dynamodb.StreamViewTypeKeysOnly
	// NewImage: The entire item, as it appears after it was modified, is written to the stream.
	NewImageView StreamView = dynamodb.StreamViewTypeNewImage
	// OldImage: The entire item, as it appeared before it was modified, is written to the stream.
	OldImageView StreamView = dynamodb.StreamViewTypeOldImage
	// NewAndOldImages: Both the new and the old item images of the item are written to the stream.
	NewAndOldImagesView StreamView = dynamodb.StreamViewTypeNewAndOldImages
)

type IndexProjection string

var (
	// KeysOnly: Only the key attributes of the modified item are written to the stream.
	KeysOnlyProjection IndexProjection = dynamodb.ProjectionTypeKeysOnly
	// All of the table attributes are projected into the index.
	AllProjection IndexProjection = dynamodb.ProjectionTypeAll
	// IncludeProjection: Only the specified table attributes are projected into the index.
	IncludeProjection IndexProjection = dynamodb.ProjectionTypeInclude
)

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

func (ct *CreateTable) Provision(readUnits, writeUnits int64) *CreateTable {
	ct.readUnits, ct.writeUnits = readUnits, writeUnits
	return ct
}

func (ct *CreateTable) ProvisionIndex(index string, readUnits, writeUnits int64) *CreateTable {
	idx := ct.globalIndices[index]
	idx.ProvisionedThroughput = &dynamodb.ProvisionedThroughput{
		ReadCapacityUnits:  &readUnits,
		WriteCapacityUnits: &writeUnits,
	}
	ct.globalIndices[index] = idx
	return ct
}

func (ct *CreateTable) Stream(view StreamView) *CreateTable {
	ct.streamView = view
	return ct
}

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

func (ct *CreateTable) Run() error {
	if ct.err != nil {
		return ct.err
	}

	input := ct.input()
	fmt.Printf("RUN %#v\n", input)
	// return retry(func() error {
	// 	_, err := ct.db.client.CreateTable(input)
	// 	return err
	// })
	return nil
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
		if index := field.Tag.Get("index"); index != "" {
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

		// local secondary index
		if localIndex := field.Tag.Get("localIndex"); localIndex != "" {
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

	switch rv.Kind() {
	case reflect.Ptr:
		return typeOf(rv.Elem())
	case reflect.String:
		return "S"
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16,
		reflect.Int8, reflect.Float64, reflect.Float32:
		return "N"
	case reflect.Slice, reflect.Array:
		if rv.Type().Elem().Kind() == reflect.Int8 {
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
