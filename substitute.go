package dynamo

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// subber is a "mixin" for operators for keep track of subtituted keys and values
type subber struct {
	nameExpr  map[string]*string
	valueExpr map[string]*dynamodb.AttributeValue
}

func (s *subber) subName(name string) string {
	if needsSub(name) {
		if s.nameExpr == nil {
			s.nameExpr = make(map[string]*string)
		}

		sub := "#s" + encodeName(name)
		s.nameExpr[sub] = aws.String(name)
		return sub
	}
	return name
}

func (s *subber) subValue(value interface{}) (string, error) {
	if s.valueExpr == nil {
		s.valueExpr = make(map[string]*dynamodb.AttributeValue)
	}

	sub := fmt.Sprintf(":v%d", len(s.valueExpr))
	av, err := marshal(value, "")
	if err != nil {
		return "", err
	}
	s.valueExpr[sub] = av
	return sub, nil
}

func (s *subber) subExpr(expr string, args []interface{}) (string, error) {
	buf := make([]rune, 0, len(expr)+len(args)*8)

	idx := 0
	for _, chr := range expr {
		switch chr {
		case '$': // key placeholder
			sub := s.subName(args[idx].(string))
			buf = append(buf, []rune(sub)...)
			idx++
		case '?': // value placeholder
			sub, err := s.subValue(args[idx])
			if err != nil {
				return "", err
			}
			buf = append(buf, []rune(sub)...)
			idx++
		default:
			buf = append(buf, chr)
		}
	}

	return string(buf), nil
}

// TODO: validate against ASCII and starting with a number etc
func needsSub(name string) bool {
	name = strings.ToUpper(name)
	switch {
	case reserved[name]:
		return true
	case strings.ContainsRune(name, '.'):
		return true
	}
	return false
}

func encodeName(name string) string {
	name = base64.StdEncoding.EncodeToString([]byte(name))
	return strings.TrimRight(name, "=")
}
