package dynamo

import (
	"bytes"
	"encoding"
	"encoding/base32"
	"fmt"
	"strconv"
	"strings"

	"github.com/guregu/dynamo/v2/internal/exprs"
)

// subber is a "mixin" for operators for keep track of subtituted keys and values
type subber struct {
	nameExpr  map[string]string
	valueExpr Item
}

func (s *subber) subName(name string) string {
	if s.nameExpr == nil {
		s.nameExpr = make(map[string]string)
	}

	sub := "#s" + encodeName(name)
	s.nameExpr[sub] = name
	return sub
}

func (s *subber) subValue(value interface{}, flags encodeFlags) (string, error) {
	if s.valueExpr == nil {
		s.valueExpr = make(Item)
	}

	if lit, ok := value.(ExpressionLiteral); ok {
		return s.merge(lit), nil
	}

	sub := fmt.Sprintf(":v%d", len(s.valueExpr))
	av, err := marshal(value, flags)
	if err != nil {
		return "", err
	}
	if av == nil {
		return "", fmt.Errorf("invalid substitute value for '%s': %v", sub, av)
	}
	s.valueExpr[sub] = av
	return sub, nil
}

// subExpr takes a dynamo-flavored expression and fills in its placeholders
// with the given args.
func (s *subber) subExpr(expr string, args ...interface{}) (string, error) {
	return s.subExprFlags(flagNone, expr, args...)
}

// subExprN is like subExpr, but allows empty and null args
func (s *subber) subExprN(expr string, args ...interface{}) (string, error) {
	return s.subExprFlags(flagAllowEmpty|flagNull, expr, args...)
}

func (s *subber) subExprFlags(flags encodeFlags, expr string, args ...interface{}) (string, error) {
	// TODO: real parsing?
	lexed, err := exprs.Parse(expr)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	var idx int
	for _, item := range lexed.Items {
		var err error
		switch item.Type {
		case exprs.ItemText:
			_, err = buf.WriteString(item.Val)
		case exprs.ItemQuotedName:
			sub := s.subName(item.Val[1 : len(item.Val)-1]) // trim ""
			_, err = buf.WriteString(sub)
		case exprs.ItemNamePlaceholder:
			if idx >= len(args) {
				err = fmt.Errorf("dynamo: missing argument for %s placeholder (at position %d of %q)", item.Val, item.Pos, expr)
				break
			}
			switch x := args[idx].(type) {
			case ExpressionLiteral:
				_, err = buf.WriteString(s.merge(x))
			case encoding.TextMarshaler:
				var txt []byte
				txt, err = x.MarshalText()
				if err == nil {
					sub := s.subName(string(txt))
					_, err = buf.WriteString(sub)
				}
			case string:
				sub := s.subName(x)
				_, err = buf.WriteString(sub)
			case int:
				_, err = buf.WriteString(strconv.Itoa(x))
			case int64:
				_, err = buf.WriteString(strconv.FormatInt(x, 10))
			default:
				err = fmt.Errorf("dynamo: type of argument for $ must be string, int, int64, encoding.TextMarshaler or dynamo.ExpressionLiteral (got type %T at position %d of %q)", x, item.Pos, expr)
			}
			idx++
		case exprs.ItemValuePlaceholder:
			if idx >= len(args) {
				err = fmt.Errorf("dynamo: missing argument for %s placeholder (at position %d of %q)", item.Val, item.Pos, expr)
				break
			}
			var sub string
			if sub, err = s.subValue(args[idx], flags); err == nil {
				_, err = buf.WriteString(sub)
			}
			idx++
		case exprs.ItemMagicLiteral:
			if idx >= len(args) {
				err = fmt.Errorf("dynamo: missing argument for %s placeholder (at position %d of %q)", item.Val, item.Pos, expr)
				break
			}
			_, err = buf.WriteString(args[idx].(string))
			idx++
		}
		if err != nil {
			return "", err
		}
	}

	return buf.String(), nil
}

// ExpressionLiteral is a raw DynamoDB expression.
// Its fields are equivalent to FilterExpression (and similar), ExpressionAttributeNames, and ExpressionAttributeValues in the DynamoDB API.
// This can be passed to any function that takes an expression, as either $ or ?.
// Your placeholders will be automatically prefixed to avoid clobbering regular placeholder substitution.
//
// dynamo provides many convenience functions around expressions to avoid having to use this.
// However, this can be useful when you need to handle complex dynamic expressions.
type ExpressionLiteral struct {
	// Expression is a raw DynamoDB expression.
	Expression string
	// AttributeNames is a map of placeholders (such as #foo) to attribute names.
	AttributeNames map[string]*string
	// AttributeValues is a map of placeholders (such as :bar) to attribute values.
	AttributeValues Item
}

// we don't want people to accidentally refer to our placeholders, so just slap an x_ in front of theirs
var foreignPlaceholder = strings.NewReplacer("#", "#x_", ":", ":x_")

// merge in a foreign expression literal
// returns a rewritten expression with prefixed placeholders
func (s *subber) merge(lit ExpressionLiteral) string {
	prefix := func(key string) string {
		return string(key[0]) + "x_" + key[1:]
	}

	if len(lit.AttributeNames) > 0 && s.nameExpr == nil {
		s.nameExpr = make(map[string]string)
	}
	for k, v := range lit.AttributeNames {
		safe := prefix(k)
		s.nameExpr[safe] = *v
	}

	if len(lit.AttributeValues) > 0 && s.valueExpr == nil {
		s.valueExpr = make(Item)
	}
	for k, v := range lit.AttributeValues {
		safe := prefix(k)
		s.valueExpr[safe] = v
	}

	expr := foreignPlaceholder.Replace(lit.Expression)
	return expr
}

var nameEncoder = base32.StdEncoding.WithPadding(base32.NoPadding)

// encodeName consistently encodes a name.
// The consistency is important.
func encodeName(name string) string {
	return nameEncoder.EncodeToString([]byte(name))
}

// escape takes a name and evaluates and substitutes it if needed.
func (s *subber) escape(name string) (string, error) {
	// reserved word
	if upper := strings.ToUpper(name); reserved[upper] {
		return s.subName(name), nil
	}
	// needs to be parsed
	if strings.ContainsAny(name, ".[]()'") {
		return s.subExpr(name, nil)
	}
	// boring
	return name, nil
}

// wrapExpr wraps expr in parens if needed
func wrapExpr(expr string) string {
	if len(expr) == 0 {
		return expr
	}

	wrap := "(" + expr + ")"

	if !strings.ContainsAny(expr, "()") {
		return wrap
	}

	stack := make([]rune, 0, len(wrap))
	pop := func() rune {
		if len(stack) == 0 {
			return -1
		}
		popped := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		return popped
	}
	for _, r := range wrap {
		if r == ')' {
			var n int
			for r != '(' {
				r = pop()
				if r == -1 {
					// unbalanced expr
					return expr
				}
				n++
			}
			if n <= 1 {
				// redundant parenthesis detected
				return expr
			}
			continue
		}
		stack = append(stack, r)
	}
	return wrap
}
