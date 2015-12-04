package exprs

import (
	"testing"
)

func BenchmarkParse(b *testing.B) {
	const expr = "'User' = ? AND $ > ?"
	for i := 0; i < b.N; i++ {
		Parse(expr)
	}
}
