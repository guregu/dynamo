// Package exprs is the internal package for parsing DynamoDB "expressions", including
// condition expressions and filter expressions.
package exprs

import (
	"fmt"
	"sync"
)

// Expr is a "parsed" expression.
type Expr struct {
	Items []Item
	err   error
}

// TODO: actually parse something here.

// Parse returns a lexed but not very parsed expression.
func Parse(input string) (*Expr, error) {
	exprCache.RLock()
	expr := exprCache.m[input]
	exprCache.RUnlock()
	if expr != nil {
		return expr, expr.err
	}

	expr = &Expr{}
	l := lex(input)
loop:
	for {
		item := l.nextItem()
		switch item.Type {
		case ItemError:
			expr.err = fmt.Errorf("dynamo: expression lex error: %s at position %d", item.Val, item.Pos)
			break loop
		case ItemEOF:
			break loop
		}
		expr.Items = append(expr.Items, item)
	}
	exprCache.Lock()
	exprCache.m[input] = expr
	exprCache.Unlock()
	return expr, expr.err
}

// TODO: provide a way to disable this cache from outside
// for people with insane query requirements.

// exprCache holds an in-memory cache of already lexed expressions.
var exprCache = struct {
	m map[string]*Expr // input → expr
	sync.RWMutex
}{m: make(map[string]*Expr)}
