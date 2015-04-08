package dynamo

import (
	"fmt"
	"strings"

	"github.com/awslabs/aws-sdk-go/aws"
)

// subber is a "mixin" for operators for keep track of subtituted names
type subber struct {
	nameExpr map[string]*string
}

func (s *subber) nameMap() *map[string]*string {
	if len(s.nameExpr) == 0 {
		return nil
	}
	return &s.nameExpr
}

func (s *subber) substitute(name string) string {
	if needsSub(name) {
		if s.nameExpr == nil {
			s.nameExpr = make(map[string]*string)
		}

		sub := fmt.Sprintf("#s%d", len(s.nameExpr))
		s.nameExpr[sub] = aws.String(name)
		fmt.Println("subbing", name, sub)
		return sub
	}
	return name
}

func (s *subber) unsub(sub string) string {
	if n, ok := s.nameExpr[sub]; ok {
		return *n
	}
	return sub
}

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
