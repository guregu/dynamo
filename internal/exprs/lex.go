package exprs

import (
	"fmt"
	"unicode/utf8"
)

// Cribbed from "Lexical Scanning in Go" - Rob Pike
// https://www.youtube.com/watch?v=HxaD_trXwRE
// and the standard library (text/template/parse)

type ItemType int

const (
	ItemError ItemType = iota
	ItemEOF
	ItemText
	ItemQuotedName
	ItemNamePlaceholder
	ItemValuePlaceholder
)

type Item struct {
	Type ItemType
	Pos  int
	Val  string
}

func (i Item) String() string {
	switch i.Type {
	case ItemEOF:
		return "EOF"
	case ItemNamePlaceholder:
		return "$"
	case ItemValuePlaceholder:
		return "?"
	}
	return i.Val
}

const eof = -1

type stateFn func(*lexer) stateFn

type lexer struct {
	input string
	state stateFn
	start int
	pos   int
	width int
	items chan Item
}

func (l *lexer) next() rune {
	if (l.pos) >= len(l.input) {
		l.width = 0
		return eof
	}
	r, w := utf8.DecodeRuneInString(l.input[l.pos:])
	l.width = w
	l.pos += l.width
	return r
}

func (l *lexer) backup() {
	l.pos -= l.width
}

func (l *lexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

func (l *lexer) emit(t ItemType) {
	l.items <- Item{
		Type: t,
		Pos:  l.pos,
		Val:  l.input[l.start:l.pos],
	}
	l.start = l.pos
}

func (l *lexer) ignore() {
	l.start = l.pos
}

// accepts

func (l *lexer) errorf(format string, args ...interface{}) stateFn {
	l.items <- Item{ItemError, l.start, fmt.Sprintf(format, args...)}
	return nil
}

// nextItem returns the next item from the input.
// Called by the parser, not in the lexing goroutine.
func (l *lexer) nextItem() Item {
	item := <-l.items
	// l.lastPos = item.pos
	return item
}

// drain drains the output so the lexing goroutine will exit.
// Called by the parser, not in the lexing goroutine.
func (l *lexer) drain() {
	for range l.items {
	}
}

func lex(input string) *lexer {
	l := &lexer{
		input: input,
		items: make(chan Item),
	}
	go l.run()
	return l
}

func (l *lexer) run() {
	for l.state = lexText; l.state != nil; {
		l.state = l.state(l)
	}
	close(l.items)
}

func lexText(l *lexer) stateFn {
loop:
	for {
		var nextFn stateFn
		r := l.next()
		switch r {
		case '\'':
			nextFn = lexQuotedName
		case '$':
			nextFn = lexName
		case '?':
			nextFn = lexValue
		case eof:
			break loop
		default:
			continue
		}

		// do the needful
		l.backup()
		if l.pos > l.start {
			l.emit(ItemText)
		}
		return nextFn
	}
	// Correctly reached EOF.
	if l.pos > l.start {
		l.emit(ItemText)
	}
	l.emit(ItemEOF)
	return nil
}

func lexQuotedName(l *lexer) stateFn {
	l.next() // first "
loop:
	for {
		switch l.next() {
		case '\'':
			break loop
		case eof:
			return l.errorf("unterminated string")
		}
	}
	l.emit(ItemQuotedName)
	return lexText
}

// when we're on a $
func lexName(l *lexer) stateFn {
	l.next()
	l.emit(ItemNamePlaceholder)
	return lexText
}

// when we're on a ?
func lexValue(l *lexer) stateFn {
	l.next()
	l.emit(ItemValuePlaceholder)
	return lexText
}
