package dynamo

import (
	"reflect"
	"testing"
)

func TestMarshal(t *testing.T) {
	for _, tc := range encodingTests {
		item, err := marshal(tc.in, "")
		if err != nil {
			t.Errorf("%s: unexpected error: %v", tc.name, err)
		}

		if !reflect.DeepEqual(item, tc.out) {
			t.Errorf("%s: bad result: %#v ≠ %#v", tc.name, item, tc.out)
		}
	}
}

func TestMarshalItem(t *testing.T) {
	for _, tc := range itemEncodingTests {
		item, err := marshalItem(tc.in)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", tc.name, err)
		}

		if !reflect.DeepEqual(item, tc.out) {
			t.Errorf("%s: bad result: %#v ≠ %#v", tc.name, item, tc.out)
		}
	}
}
