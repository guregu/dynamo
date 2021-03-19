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

func TestWrapExpr(t *testing.T) {
	test := []struct {
		in  string
		out string
	}{
		{"A", "(A)"},
		{"(A)", "(A)"},
		{"(A) OR (B)", "((A) OR (B))"},
		{"((A) OR (B))", "((A) OR (B))"},
		{"(A) OR attribute_exists('FOO')", "((A) OR attribute_exists('FOO'))"},
		{"('expires_at' >= ?) OR ('expires_at' = ?)", "(('expires_at' >= ?) OR ('expires_at' = ?))"},
		{")", ")"},
		{"()(", "()("},
		{"", ""},
	}
	for _, tc := range test {
		got := wrapExpr(tc.in)
		if tc.out != got {
			t.Errorf("wrapExpr mismatch. want: %s got: %s", tc.out, got)
		}
	}
}

func BenchmarkSubExpr(b *testing.B) {
	const expr = "'User' = ? AND $ > ?"
	for i := 0; i < b.N; i++ {
		s := subber{}
		s.subExpr(expr, 613, "Time", "2015-12-04")
	}
}
