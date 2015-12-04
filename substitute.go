package dynamo

import (
	"bytes"
	"encoding/base64"
	"fmt"
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
			sub := s.subName(args[idx].(string))
			_, err = buf.WriteString(sub)
			idx++
		case exprs.ItemValuePlaceholder:
			var sub string
			if sub, err = s.subValue(args[idx]); err == nil {
				_, err = buf.WriteString(sub)
			}
			idx++
		}
		if err != nil {
			return "", err
		}
	}

	return buf.String(), nil
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
