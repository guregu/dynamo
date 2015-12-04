package dynamo

import (
	"fmt"
	"testing"
)

func TestSubExpr(t *testing.T) {
	s := subber{}

	subbed, err := s.subExpr("$ > ? AND begins_with (Title, ?)", "Count", "1", "foo")
	if err != nil {
		t.Error(err)
	}

	const format = "%s > :v0 AND begins_with (Title, :v1)"
	// you should be able to sub the same name twice and get the same result
	expect := fmt.Sprintf(format, s.subName("Count"))
	if subbed != expect {
		t.Errorf("bad subbed expr: %v ≠ %v", subbed, expect)
	}
}

func BenchmarkSubExpr(b *testing.B) {
	const expr = "'User' = ? AND $ > ?"
	for i := 0; i < b.N; i++ {
		s := subber{}
		s.subExpr(expr, 613, "Time", "2015-12-04")
	}
}
