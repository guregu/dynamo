package exprs

import (
	"testing"
)

func BenchmarkParseCached(b *testing.B) {
	const expr = "'User' = ? AND $ > ?"
	for i := 0; i < b.N; i++ {
		Parse(expr)
	}
}

func BenchmarkParseUncached(b *testing.B) {
	const expr = "'User' = ? AND $ > ?"
	for i := 0; i < b.N; i++ {
		Parse(expr)
		exprCache.m[expr] = nil
	}
}
