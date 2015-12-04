package exprs

import (
	"testing"
)

func TestParse(t *testing.T) {
	const ok = "'Count' > ? AND $ = ?"
	_, err := Parse(ok)
	if err != nil {
		t.Error(err)
	}

	const bad = "'Unclosed"
	_, err = Parse(bad)
	if err == nil {
		t.Error("expected error, got nil")
	}
}
