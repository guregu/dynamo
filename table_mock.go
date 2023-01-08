package dynamo

import (
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type tableschema struct {
	keys          keyschema
	globalIndices map[string]keyschema
	localIndices  map[string]keyschema
}

type keyschema struct {
	hashKey  string
	rangeKey string
}

type testdata struct {
	original interface{}
	rvFields map[string]reflect.Value
	avFields map[string]*dynamodb.AttributeValue
}

func (db *DB) MockTable(schema interface{}, testdata []interface{}) (t Table, err error) {
	createTableObj := db.CreateTable("", schema)
	if createTableObj.err != nil {
		err = createTableObj.err
		return
	}

	t.db = db

	// primary keys
	t.schema.keys = toKeypair(createTableObj.schema)

	// global secondary index
	t.schema.globalIndices = make(map[string]keyschema, len(createTableObj.globalIndices))
	for key := range createTableObj.globalIndices {
		t.schema.globalIndices[key] = toKeypair(createTableObj.globalIndices[key].KeySchema)
	}

	// local secondary index
	t.schema.localIndices = make(map[string]keyschema, len(createTableObj.localIndices))
	for key := range createTableObj.localIndices {
		keys := toKeypair(createTableObj.localIndices[key].KeySchema)
		keys.hashKey = t.schema.keys.hashKey
		t.schema.localIndices[key] = keys
	}

	t.testData, err = toTestdata(testdata)
	return
}

func toKeypair(keySchemas []*dynamodb.KeySchemaElement) (p keyschema) {
	for i := range keySchemas {
		switch *keySchemas[i].KeyType {
		case dynamodb.KeyTypeHash:
			p.hashKey = *keySchemas[i].AttributeName
		case dynamodb.KeyTypeRange:
			p.rangeKey = *keySchemas[i].AttributeName
		}
	}
	return
}

func toTestdata(data []interface{}) ([]testdata, error) {
	testData := make([]testdata, len(data))

	for i := range data {
		rv := reflect.ValueOf(data[i])
		if rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}

		if rv.Kind() != reflect.Struct {
			return testData, fmt.Errorf("dynamo: mock table: test data is not struct: %s", rv.Kind().String())
		}

		testData[i].original = data[i]
		testData[i].rvFields = fieldsInStruct(rv)
		testData[i].avFields = make(map[string]*dynamodb.AttributeValue, len(testData[i].rvFields))

		for key, value := range testData[i].rvFields {
			av, err := marshal(value.Interface(), flagNone)
			if err != nil {
				return nil, err
			}
			if av == nil {
				av = &dynamodb.AttributeValue{NULL: aws.Bool(true)}
			}
			testData[i].avFields[key] = av
		}
	}

	return testData, nil
}
