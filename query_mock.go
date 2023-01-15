package dynamo

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (q *Query) isMock() bool {
	return q.table.db.isMock
}

func (q *Query) mockOne(out interface{}) error {
	results, _, err := q.list()
	if err != nil {
		return err
	}

	if len(results) == 0 {
		return ErrNotFound
	}

	return unmarshalItem(results[0].avFields, out)
}

func (q *Query) mockCount() (int64, error) {
	results, _, err := q.list()
	if err != nil {
		return 0, err
	}

	return int64(len(results)), nil
}

func (itr *queryIter) mockNext(out interface{}) bool {
	// stop if exceed limit
	if itr.query.limit > 0 && itr.n == itr.query.limit {
		return false
	}

	// can we use results we already have?
	if itr.output != nil {
		// stop if no result
		if int(itr.n) == len(itr.output.Items) {
			return false
		}

		item := itr.output.Items[int(itr.n)]
		itr.last = item
		itr.err = itr.unmarshal(item, out)
		itr.n++
		return itr.err == nil
	}

	// new query
	results, keys, err := itr.query.list()
	if err != nil {
		itr.err = err
		return false
	}
	if len(results) == 0 {
		return false
	}
	itr.output = &dynamodb.QueryOutput{Items: []map[string]*dynamodb.AttributeValue{}}
	limit := len(results)
	if itr.query.searchLimit > 0 {
		limit = int(itr.query.searchLimit)
	}
	var (
		pass  bool
		count int
	)
	for i := range results {
		if itr.query.startKey != nil && !pass {
			if compareAttrValueMap(itr.query.startKey, results[i].avFields) {
				pass = true
			}
			continue
		}
		itr.output.Items = append(itr.output.Items, results[i].avFields)
		count++
		if count == limit {
			itr.output.LastEvaluatedKey = results[i].avFields
			break
		}
	}

	itr.keys = map[string]struct{}{
		keys.hashKey:  {},
		keys.rangeKey: {},
	}
	item := itr.output.Items[int(itr.n)]
	itr.last = item
	itr.err = itr.unmarshal(item, out)
	itr.n++
	return itr.err == nil
}

func (q *Query) list() ([]testdata, keyschema, error) {
	keys, err := q.keyschema()
	if err != nil {
		return nil, keys, err
	}

	results, err := q.search(keys)
	if err != nil {
		return nil, keys, err
	}

	return results, keys, nil
}

func (q *Query) keyschema() (k keyschema, err error) {
	var ok bool
	if q.index == "" {
		k = q.table.schema.keys
	} else if k, ok = q.table.schema.globalIndices[q.index]; ok {
		// ok
	} else if k, ok = q.table.schema.localIndices[q.index]; ok {
		// ok
	} else {
		// ng
		err = errors.New("dynamo: mock one: index not found")
		return
	}

	// validate q.hashkey and q.rangekey
	if q.hashKey != k.hashKey {
		err = fmt.Errorf("dynamo: mock one: invalid hashkey name: expected %s: actural %s", k.hashKey, q.hashKey)
		return
	}
	if q.rangeKey != "" && q.rangeKey != k.rangeKey {
		err = fmt.Errorf("dynamo: mock one: invalid rangekey name: expected %s: actural %s", k.rangeKey, q.rangeKey)
		return
	}

	return
}

func (q *Query) search(keys keyschema) ([]testdata, error) {
	result := []testdata{}
	for i, data := range q.table.testData {
		// search by hashkey
		ok, err := compareAttrValue(data.avFields[keys.hashKey], q.hashValue, Equal, data.rvFields[keys.hashKey].Kind())
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}

		// skip searching by rangekey when no rangekey
		if q.rangeKey == "" {
			result = append(result, data)
			continue
		}

		// search by rangekey
		if q.rangeOp == Between {
			ok, err = betweenAttrValue(data.avFields[keys.rangeKey], q.rangeValues[0], q.rangeValues[1], data.rvFields[keys.rangeKey].Kind())
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
		} else {
			ok, err = compareAttrValue(data.avFields[keys.rangeKey], q.rangeValues[0], q.rangeOp, data.rvFields[keys.rangeKey].Kind())
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
		}

		result = append(result, q.table.testData[i])
	}

	// sort
	if q.order != nil {
		result = q.sort(result, keys)
	}

	return result, nil
}

func compareAttrValue(x, y *dynamodb.AttributeValue, operator Operator, rt reflect.Kind) (bool, error) {
	switch operator {
	case Equal:
		if x.String() == y.String() {
			return true, nil
		}
	case NotEqual:
		if x.String() != y.String() {
			return true, nil
		}
	case BeginsWith:
		if strings.HasPrefix(*x.S, *y.S) {
			return true, nil
		}
	case Less, LessOrEqual, Greater, GreaterOrEqual:
		switch rt {
		case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
			x, err := strconv.ParseInt(*x.N, 10, 64)
			if err != nil {
				return false, err
			}
			y, err := strconv.ParseInt(*y.N, 10, 64)
			if err != nil {
				return false, err
			}

			switch operator {
			case Less:
				if x < y {
					return true, nil
				}
			case LessOrEqual:
				if x <= y {
					return true, nil
				}
			case Greater:
				if x > y {
					return true, nil
				}
			case GreaterOrEqual:
				if x >= y {
					return true, nil
				}
			}
		case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
			x, err := strconv.ParseUint(*x.N, 10, 64)
			if err != nil {
				return false, err
			}
			y, err := strconv.ParseUint(*y.N, 10, 64)
			if err != nil {
				return false, err
			}

			switch operator {
			case Less:
				if x < y {
					return true, nil
				}
			case LessOrEqual:
				if x <= y {
					return true, nil
				}
			case Greater:
				if x > y {
					return true, nil
				}
			case GreaterOrEqual:
				if x >= y {
					return true, nil
				}
			}
		case reflect.Float64, reflect.Float32:
			x, err := strconv.ParseFloat(*x.N, 64)
			if err != nil {
				return false, err
			}
			y, err := strconv.ParseFloat(*y.N, 64)
			if err != nil {
				return false, err
			}

			switch operator {
			case Less:
				if x < y {
					return true, nil
				}
			case LessOrEqual:
				if x <= y {
					return true, nil
				}
			case Greater:
				if x > y {
					return true, nil
				}
			case GreaterOrEqual:
				if x >= y {
					return true, nil
				}
			}
		default:
			if x.S != nil && y.S != nil {
				x := *x.S
				y := *y.S
				switch operator {
				case Less:
					if x < y {
						return true, nil
					}
				case LessOrEqual:
					if x <= y {
						return true, nil
					}
				case Greater:
					if x > y {
						return true, nil
					}
				case GreaterOrEqual:
					if x >= y {
						return true, nil
					}
				}
			}
		}
	}

	return false, nil
}

func betweenAttrValue(x, y1, y2 *dynamodb.AttributeValue, rt reflect.Kind) (bool, error) {
	switch rt {
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		x, err := strconv.ParseInt(*x.N, 10, 64)
		if err != nil {
			return false, err
		}
		y1, err := strconv.ParseInt(*y1.N, 10, 64)
		if err != nil {
			return false, err
		}
		y2, err := strconv.ParseInt(*y2.N, 10, 64)
		if err != nil {
			return false, err
		}
		if y1 <= x && x <= y2 {
			return true, nil
		}
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		x, err := strconv.ParseUint(*x.N, 10, 64)
		if err != nil {
			return false, err
		}
		y1, err := strconv.ParseUint(*y1.N, 10, 64)
		if err != nil {
			return false, err
		}
		y2, err := strconv.ParseUint(*y2.N, 10, 64)
		if err != nil {
			return false, err
		}
		if y1 <= x && x <= y2 {
			return true, nil
		}
	case reflect.Float64, reflect.Float32:
		x, err := strconv.ParseFloat(*x.N, 64)
		if err != nil {
			return false, err
		}
		y1, err := strconv.ParseFloat(*y1.N, 64)
		if err != nil {
			return false, err
		}
		y2, err := strconv.ParseFloat(*y2.N, 64)
		if err != nil {
			return false, err
		}
		if y1 <= x && x <= y2 {
			return true, nil
		}
	default:
		if x.S != nil && y1.S != nil && y2.S != nil {
			x := *x.S
			y1 := *y1.S
			y2 := *y2.S
			if y1 <= x && x <= y2 {
				return true, nil
			}
		}
	}

	return false, nil
}

func (q *Query) sort(data []testdata, keys keyschema) []testdata {
	sort.Slice(data, func(i, j int) bool {
		irv := data[i].rvFields[keys.rangeKey]
		jrv := data[j].rvFields[keys.rangeKey]

		switch irv.Kind() {
		case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
			if *q.order == Descending {
				return irv.Int() > jrv.Int()
			} else {
				return irv.Int() < jrv.Int()
			}
		case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
			if *q.order == Descending {
				return irv.Uint() > jrv.Uint()
			} else {
				return irv.Uint() < jrv.Uint()
			}
		case reflect.Float64, reflect.Float32:
			if *q.order == Descending {
				return irv.Float() > jrv.Float()
			} else {
				return irv.Float() < jrv.Float()
			}
		default:
			if *q.order == Descending {
				return data[i].avFields[keys.rangeKey].String() > data[j].avFields[keys.rangeKey].String()
			} else {
				return data[i].avFields[keys.rangeKey].String() < data[j].avFields[keys.rangeKey].String()
			}
		}
	})

	return data
}

func compareAttrValueMap(src, tar map[string]*dynamodb.AttributeValue) bool {
	for key := range src {
		if src[key].String() != tar[key].String() {
			return false
		}
	}
	return true
}
