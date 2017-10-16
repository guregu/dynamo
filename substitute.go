package dynamo

import (
	"bytes"
	"encoding"
	"encoding/base32"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/guregu/dynamo/internal/exprs"
)

// subber is a "mixin" for operators for keep track of subtituted keys and values
type subber struct {
	nameExpr  map[string]*string
	valueExpr map[string]*dynamodb.AttributeValue
}

func (s *subber) subName(name string) string {
	if s.nameExpr == nil {
		s.nameExpr = make(map[string]*string)
	}

	sub := "#s" + encodeName(name)
	s.nameExpr[sub] = aws.String(name)
	return sub
}

func (s *subber) subValue(value interface{}, special string) (string, error) {
	if s.valueExpr == nil {
		s.valueExpr = make(map[string]*dynamodb.AttributeValue)
	}

	sub := fmt.Sprintf(":v%d", len(s.valueExpr))
	av, err := marshal(value, special)
	if err != nil {
		return "", err
	}
	s.valueExpr[sub] = av
	return sub, nil
}

// subExpr takes a dynamo-flavored expression and fills in its placeholders
// with the given args.
func (s *subber) subExpr(expr string, args ...interface{}) (string, error) {
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
			switch x := args[idx].(type) {
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
				err = fmt.Errorf("dynamo: type of argument for $ must be string, int, or int64 (got %T)", x)
			}
			idx++
		case exprs.ItemValuePlaceholder:
			var sub string
			if sub, err = s.subValue(args[idx], ""); err == nil {
				_, err = buf.WriteString(sub)
			}
			idx++
		case exprs.ItemMagicLiteral:
			_, err = buf.WriteString(args[idx].(string))
			idx++
		}
		if err != nil {
			return "", err
		}
	}

	return buf.String(), nil
}

// encodeName consistently encodes a name.
// The consistency is important.
func encodeName(name string) string {
	name = base32.StdEncoding.EncodeToString([]byte(name))
	return strings.TrimRight(name, "=")
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
