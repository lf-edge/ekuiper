// Copyright 2021-2022 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package xsql

import (
	"bufio"
	"bytes"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"io"
	"strconv"
	"strings"
)

type Scanner struct {
	r *bufio.Reader
}

func NewScanner(r io.Reader) *Scanner {
	return &Scanner{r: bufio.NewReader(r)}
}

func (s *Scanner) Scan() (tok ast.Token, lit string) {
	ch := s.read()
	if isWhiteSpace(ch) {
		//s.unread()
		return s.ScanWhiteSpace()
	} else if isLetter(ch) {
		s.unread()
		return s.ScanIdent()
	} else if isQuotation(ch) {
		s.unread()
		return s.ScanString()
	} else if isDigit(ch) {
		s.unread()
		return s.ScanNumber(false, false)
	} else if isBackquote(ch) {
		return s.ScanBackquoteIdent()
	}

	switch ch {
	case eof:
		return ast.EOF, ast.Tokens[ast.EOF]
	case '=':
		return ast.EQ, ast.Tokens[ast.EQ]
	case '!':
		_, _ = s.ScanWhiteSpace()
		if r := s.read(); r == '=' {
			return ast.NEQ, ast.Tokens[ast.NEQ]
		} else {
			s.unread()
		}
		return ast.EQ, ast.Tokens[ast.EQ]
	case '<':
		_, _ = s.ScanWhiteSpace()
		if r := s.read(); r == '=' {
			return ast.LTE, ast.Tokens[ast.LTE]
		} else {
			s.unread()
		}
		return ast.LT, ast.Tokens[ast.LT]
	case '>':
		_, _ = s.ScanWhiteSpace()
		if r := s.read(); r == '=' {
			return ast.GTE, ast.Tokens[ast.GTE]
		} else {
			s.unread()
		}
		return ast.GT, ast.Tokens[ast.GT]
	case '+':
		return ast.ADD, ast.Tokens[ast.ADD]
	case '-':
		_, _ = s.ScanWhiteSpace()
		if r := s.read(); r == '-' {
			s.skipUntilNewline()
			return ast.COMMENT, ""
		} else if r == '>' {
			return ast.ARROW, ast.Tokens[ast.ARROW]
		} else if r == '.' {
			_, _ = s.ScanWhiteSpace()
			if r1 := s.read(); isDigit(r1) {
				s.unread()
				return s.ScanNumber(true, true)
			} else {
				s.unread()
			}
			s.unread()
		} else {
			s.unread()
		}
		return ast.SUB, ast.Tokens[ast.SUB]
	case '/':
		_, _ = s.ScanWhiteSpace()
		if r := s.read(); r == '*' {
			if err := s.skipUntilEndComment(); err != nil {
				return ast.ILLEGAL, ""
			}
			return ast.COMMENT, ""
		} else {
			s.unread()
		}
		return ast.DIV, ast.Tokens[ast.DIV]
	case '.':
		if r := s.read(); isDigit(r) {
			s.unread()
			return s.ScanNumber(true, false)
		}
		s.unread()
		return ast.DOT, ast.Tokens[ast.DOT]
	case '%':
		return ast.MOD, ast.Tokens[ast.MOD]
	case '&':
		return ast.BITWISE_AND, ast.Tokens[ast.BITWISE_AND]
	case '|':
		return ast.BITWISE_OR, ast.Tokens[ast.BITWISE_OR]
	case '^':
		return ast.BITWISE_XOR, ast.Tokens[ast.BITWISE_XOR]
	case '*':
		return ast.ASTERISK, ast.Tokens[ast.ASTERISK]
	case ',':
		return ast.COMMA, ast.Tokens[ast.COMMA]
	case '(':
		return ast.LPAREN, ast.Tokens[ast.LPAREN]
	case ')':
		return ast.RPAREN, ast.Tokens[ast.RPAREN]
	case '[':
		return ast.LBRACKET, ast.Tokens[ast.LBRACKET]
	case ']':
		return ast.RBRACKET, ast.Tokens[ast.RBRACKET]
	case ':':
		return ast.COLON, ast.Tokens[ast.COLON]
	case '#':
		return ast.HASH, ast.Tokens[ast.HASH]
	case ';':
		return ast.SEMICOLON, ast.Tokens[ast.SEMICOLON]
	}
	return ast.ILLEGAL, ""
}

func (s *Scanner) ScanIdent() (tok ast.Token, lit string) {
	var buf bytes.Buffer
	buf.WriteRune(s.read())
	for {
		if ch := s.read(); ch == eof {
			break
		} else if !isLetter(ch) && !isDigit(ch) && ch != '_' {
			s.unread()
			break
		} else {
			buf.WriteRune(ch)
		}
	}

	switch lit = strings.ToUpper(buf.String()); lit {
	case "SELECT":
		return ast.SELECT, lit
	case "AS":
		return ast.AS, lit
	case "FROM":
		return ast.FROM, lit
	case "WHERE":
		return ast.WHERE, lit
	case "AND":
		return ast.AND, lit
	case "OR":
		return ast.OR, lit
	case "GROUP":
		return ast.GROUP, lit
	case "HAVING":
		return ast.HAVING, lit
	case "ORDER":
		return ast.ORDER, lit
	case "BY":
		return ast.BY, lit
	case "DESC":
		return ast.DESC, lit
	case "ASC":
		return ast.ASC, lit
	case "FILTER":
		return ast.FILTER, lit
	case "INNER":
		return ast.INNER, lit
	case "LEFT":
		return ast.LEFT, lit
	case "RIGHT":
		return ast.RIGHT, lit
	case "FULL":
		return ast.FULL, lit
	case "CROSS":
		return ast.CROSS, lit
	case "JOIN":
		return ast.JOIN, lit
	case "ON":
		return ast.ON, lit
	case "CASE":
		return ast.CASE, lit
	case "WHEN":
		return ast.WHEN, lit
	case "THEN":
		return ast.THEN, lit
	case "ELSE":
		return ast.ELSE, lit
	case "END":
		return ast.END, lit
	case "IN":
		return ast.IN, lit
	case "NOT":
		return ast.NOT, lit
	case "BETWEEN":
		return ast.BETWEEN, lit
	case "LIKE":
		return ast.LIKE, lit
	case "OVER":
		return ast.OVER, lit
	case "PARTITION":
		return ast.PARTITION, lit
	case "CREATE":
		return ast.CREATE, lit
	case "DROP":
		return ast.DROP, lit
	case "EXPLAIN":
		return ast.EXPLAIN, lit
	case "DESCRIBE":
		return ast.DESCRIBE, lit
	case "SHOW":
		return ast.SHOW, lit
	case "STREAM":
		return ast.STREAM, lit
	case "STREAMS":
		return ast.STREAMS, lit
	case "TABLE":
		return ast.TABLE, lit
	case "TABLES":
		return ast.TABLES, lit
	case "WITH":
		return ast.WITH, lit
	case "BIGINT":
		return ast.XBIGINT, lit
	case "FLOAT":
		return ast.XFLOAT, lit
	case "DATETIME":
		return ast.XDATETIME, lit
	case "STRING":
		return ast.XSTRING, lit
	case "BYTEA":
		return ast.XBYTEA, lit
	case "BOOLEAN":
		return ast.XBOOLEAN, lit
	case "ARRAY":
		return ast.XARRAY, lit
	case "STRUCT":
		return ast.XSTRUCT, lit
	case "DATASOURCE":
		return ast.DATASOURCE, lit
	case "KEY":
		return ast.KEY, lit
	case "FORMAT":
		return ast.FORMAT, lit
	case "CONF_KEY":
		return ast.CONF_KEY, lit
	case "TYPE":
		return ast.TYPE, lit
	case "TRUE":
		return ast.TRUE, lit
	case "FALSE":
		return ast.FALSE, lit
	case "STRICT_VALIDATION":
		return ast.STRICT_VALIDATION, lit
	case "TIMESTAMP":
		return ast.TIMESTAMP, lit
	case "TIMESTAMP_FORMAT":
		return ast.TIMESTAMP_FORMAT, lit
	case "RETAIN_SIZE":
		return ast.RETAIN_SIZE, lit
	case "SHARED":
		return ast.SHARED, lit
	case "SCHEMAID":
		return ast.SCHEMAID, lit
	case "KIND":
		return ast.KIND, lit
	case "DD":
		return ast.DD, lit
	case "HH":
		return ast.HH, lit
	case "MI":
		return ast.MI, lit
	case "SS":
		return ast.SS, lit
	case "MS":
		return ast.MS, lit
	}

	return ast.IDENT, buf.String()
}

func (s *Scanner) ScanString() (tok ast.Token, lit string) {
	var buf bytes.Buffer
	ch := s.read()
	buf.WriteRune(ch)
	escape := false
	for {
		ch = s.read()
		if ch == '"' && !escape {
			buf.WriteRune(ch)
			break
		} else if ch == eof {
			return ast.BADSTRING, buf.String()
		} else if ch == '\\' && !escape {
			escape = true
			buf.WriteRune(ch)
		} else {
			escape = false
			buf.WriteRune(ch)
		}
	}
	r, err := strconv.Unquote(buf.String())
	if err != nil {
		return ast.ILLEGAL, "invalid string: " + buf.String()
	}
	return ast.STRING, r
}

func (s *Scanner) ScanDigit() (tok ast.Token, lit string) {
	var buf bytes.Buffer
	ch := s.read()
	buf.WriteRune(ch)
	for {
		if ch := s.read(); isDigit(ch) {
			buf.WriteRune(ch)
		} else {
			s.unread()
			break
		}
	}
	return ast.INTEGER, buf.String()
}

func (s *Scanner) ScanNumber(startWithDot bool, isNeg bool) (tok ast.Token, lit string) {
	var buf bytes.Buffer

	if isNeg {
		buf.WriteRune('-')
	}

	if startWithDot {
		buf.WriteRune('.')
	}

	ch := s.read()
	buf.WriteRune(ch)

	isNum := false
	for {
		if ch := s.read(); isDigit(ch) {
			buf.WriteRune(ch)
		} else if ch == '.' {
			isNum = true
			buf.WriteRune(ch)
		} else {
			s.unread()
			break
		}
	}
	if isNum || startWithDot {
		return ast.NUMBER, buf.String()
	} else {
		return ast.INTEGER, buf.String()
	}
}

func (s *Scanner) ScanBackquoteIdent() (tok ast.Token, lit string) {
	var buf bytes.Buffer
	for {
		ch := s.read()
		if isBackquote(ch) || ch == eof {
			break
		} else {
			buf.WriteRune(ch)
		}
	}
	return ast.IDENT, buf.String()
}

func (s *Scanner) skipUntilNewline() {
	for {
		if ch := s.read(); ch == '\n' || ch == eof {
			return
		}
	}
}

func (s *Scanner) skipUntilEndComment() error {
	for {
		if ch1 := s.read(); ch1 == '*' {
			// We might be at the end.
		star:
			ch2 := s.read()
			if ch2 == '/' {
				return nil
			} else if ch2 == '*' {
				// We are back in the state machine since we see a star.
				goto star
			} else if ch2 == eof {
				return io.EOF
			}
		} else if ch1 == eof {
			return io.EOF
		}
	}
}

func (s *Scanner) ScanWhiteSpace() (tok ast.Token, lit string) {
	var buf bytes.Buffer
	for {
		if ch := s.read(); ch == eof {
			break
		} else if !isWhiteSpace(ch) {
			s.unread()
			break
		} else {
			buf.WriteRune(ch)
		}
	}
	return ast.WS, buf.String()
}

func (s *Scanner) read() rune {
	ch, _, err := s.r.ReadRune()
	if err != nil {
		return eof
	}
	return ch
}

func (s *Scanner) unread() {
	_ = s.r.UnreadRune()
}

var eof = rune(0)

func isWhiteSpace(r rune) bool {
	return (r == ' ') || (r == '\t') || (r == '\r') || (r == '\n')
}

func isLetter(ch rune) bool { return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') }

func isDigit(ch rune) bool { return ch >= '0' && ch <= '9' }

func isQuotation(ch rune) bool { return ch == '"' }

func isBackquote(ch rune) bool { return ch == '`' }
