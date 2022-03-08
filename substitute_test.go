package dynamo

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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

func TestSubMerge(t *testing.T) {
	s := subber{}
	lit := ExpressionLiteral{
		Expression: "contains(#a, :v) AND (#abc.#abcdef = :v0)",
		AttributeNames: map[string]*string{
			"#a":      aws.String("name"),
			"#abc":    aws.String("custom"),
			"#abcdef": aws.String("model"),
		},
		AttributeValues: map[string]*dynamodb.AttributeValue{
			":v":  {S: aws.String("abc")},
			":v0": {N: aws.String("555")},
		},
	}
	rewrite, err := s.subExpr("?", lit)
	if err != nil {
		t.Fatal(err)
	}
	want := "contains(#x_a, :x_v) AND (#x_abc.#x_abcdef = :x_v0)"
	if rewrite != want {
		t.Error("bad rewrite. want:", want, "got:", rewrite)
	}

	for k, v := range lit.AttributeNames {
		foreign := foreignPlaceholder.Replace(k)
		got, ok := s.nameExpr[foreign]
		if !ok {
			t.Error("missing merged name:", k, foreign)
		}
		if !reflect.DeepEqual(v, got) {
			t.Error("merged name mismatch. want:", v, "got:", got)
		}
	}

	for k, v := range lit.AttributeValues {
		foreign := foreignPlaceholder.Replace(k)
		got, ok := s.valueExpr[foreign]
		if !ok {
			t.Error("missing merged value:", k, foreign)
		}
		if !reflect.DeepEqual(v, got) {
			t.Error("merged value mismatch. want:", v, "got:", got)
		}
	}

	t.Run("wrap", func(t *testing.T) {
		s := subber{}
		lit := lit.Wrap()
		rewrite, err := s.subExpr("$", lit)
		if err != nil {
			t.Fatal(err)
		}
		want := "(contains(#x_a, :x_v) AND (#x_abc.#x_abcdef = :x_v0))"
		if rewrite != want {
			t.Error("bad rewrite. want:", want, "got:", rewrite)
		}
	})
}

func BenchmarkSubExpr(b *testing.B) {
	const expr = "'User' = ? AND $ > ?"
	for i := 0; i < b.N; i++ {
		s := subber{}
		s.subExpr(expr, 613, "Time", "2015-12-04")
	}
}
