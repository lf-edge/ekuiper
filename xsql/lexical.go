package xsql

import (
	"bufio"
	"bytes"
	"io"
	"strconv"
	"strings"
)

type Token int

const (
	// Special tokens
	ILLEGAL Token = iota
	EOF
	WS
	COMMENT

	AS
	// Literals
	IDENT // main

	INTEGER   // 12345
	NUMBER    //12345.67
	STRING    // "abc"
	BADSTRING // "abc

	operatorBeg
	// ADD and the following are InfluxQL Operators
	ADD         // +
	SUB         // -
	MUL         // *
	DIV         // /
	MOD         // %
	BITWISE_AND // &
	BITWISE_OR  // |
	BITWISE_XOR // ^

	AND // AND
	OR  // OR

	EQ  // =
	NEQ // !=
	LT  // <
	LTE // <=
	GT  // >
	GTE // >=

	SUBSET //[
	ARROW  //->

	operatorEnd

	// Misc characters
	ASTERISK  // *
	COMMA     // ,
	LPAREN    // (
	RPAREN    // )
	LBRACKET  //[
	RBRACKET  //]
	HASH      // #
	DOT       // .
	COLON     //:
	SEMICOLON //;
	COLSEP    //\007

	// Keywords
	SELECT
	FROM
	JOIN
	INNER
	LEFT
	RIGHT
	FULL
	CROSS
	ON
	WHERE
	GROUP
	ORDER
	HAVING
	BY
	ASC
	DESC

	TRUE
	FALSE

	CREATE
	DROP
	EXPLAIN
	DESCRIBE
	SHOW
	STREAM
	STREAMS
	WITH

	XBIGINT
	XFLOAT
	XSTRING
	XDATETIME
	XBOOLEAN
	XARRAY
	XSTRUCT

	DATASOURCE
	KEY
	FORMAT
	CONF_KEY
	TYPE
	STRICT_VALIDATION
	TIMESTAMP
	TIMESTAMP_FORMAT

	DD
	HH
	MI
	SS
	MS
)

var tokens = []string{
	ILLEGAL: "ILLEGAL",
	EOF:     "EOF",
	AS:      "AS",
	WS:      "WS",
	IDENT:   "IDENT",
	INTEGER: "INTEGER",
	NUMBER:  "NUMBER",
	STRING:  "STRING",

	ADD:         "+",
	SUB:         "-",
	MUL:         "*",
	DIV:         "/",
	MOD:         "%",
	BITWISE_AND: "&",
	BITWISE_OR:  "|",
	BITWISE_XOR: "^",

	EQ:  "=",
	NEQ: "!=",
	LT:  "<",
	LTE: "<=",
	GT:  ">",
	GTE: ">=",

	SUBSET: "[]",
	ARROW:  "->",

	ASTERISK: "*",
	COMMA:    ",",

	LPAREN:    "(",
	RPAREN:    ")",
	LBRACKET:  "[",
	RBRACKET:  "]",
	HASH:      "#",
	DOT:       ".",
	SEMICOLON: ";",
	COLON:     ":",
	COLSEP:    "\007",

	SELECT: "SELECT",
	FROM:   "FROM",
	JOIN:   "JOIN",
	LEFT:   "LEFT",
	INNER:  "INNER",
	ON:     "ON",
	WHERE:  "WHERE",
	GROUP:  "GROUP",
	ORDER:  "ORDER",
	HAVING: "HAVING",
	BY:     "BY",
	ASC:    "ASC",
	DESC:   "DESC",

	CREATE:   "CREATE",
	DROP:     "RROP",
	EXPLAIN:  "EXPLAIN",
	DESCRIBE: "DESCRIBE",
	SHOW:     "SHOW",
	STREAM:   "STREAM",
	STREAMS:  "STREAMS",
	WITH:     "WITH",

	XBIGINT:   "BIGINT",
	XFLOAT:    "FLOAT",
	XSTRING:   "STRING",
	XDATETIME: "DATETIME",
	XBOOLEAN:  "BOOLEAN",
	XARRAY:    "ARRAY",
	XSTRUCT:   "STRUCT",

	DATASOURCE:        "DATASOURCE",
	KEY:               "KEY",
	FORMAT:            "FORMAT",
	CONF_KEY:          "CONF_KEY",
	TYPE:              "TYPE",
	STRICT_VALIDATION: "STRICT_VALIDATION",
	TIMESTAMP:         "TIMESTAMP",
	TIMESTAMP_FORMAT:  "TIMESTAMP_FORMAT",

	AND:   "AND",
	OR:    "OR",
	TRUE:  "TRUE",
	FALSE: "FALSE",

	DD: "DD",
	HH: "HH",
	MI: "MI",
	SS: "SS",
	MS: "MS",
}

func (tok Token) String() string {
	if tok >= 0 && tok < Token(len(tokens)) {
		return tokens[tok]
	}
	return ""
}

type Scanner struct {
	r *bufio.Reader
}

func NewScanner(r io.Reader) *Scanner {
	return &Scanner{r: bufio.NewReader(r)}
}

func (s *Scanner) Scan() (tok Token, lit string) {
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
		return EOF, tokens[EOF]
	case '=':
		return EQ, tokens[EQ]
	case '!':
		_, _ = s.ScanWhiteSpace()
		if r := s.read(); r == '=' {
			return NEQ, tokens[NEQ]
		} else {
			s.unread()
		}
		return EQ, tokens[EQ]
	case '<':
		_, _ = s.ScanWhiteSpace()
		if r := s.read(); r == '=' {
			return LTE, tokens[LTE]
		} else {
			s.unread()
		}
		return LT, tokens[LT]
	case '>':
		_, _ = s.ScanWhiteSpace()
		if r := s.read(); r == '=' {
			return GTE, tokens[GTE]
		} else {
			s.unread()
		}
		return GT, tokens[GT]
	case '+':
		return ADD, tokens[ADD]
	case '-':
		_, _ = s.ScanWhiteSpace()
		if r := s.read(); r == '-' {
			s.skipUntilNewline()
			return COMMENT, ""
		} else if r == '>' {
			return ARROW, tokens[ARROW]
		} else if isDigit(r) {
			s.unread()
			return s.ScanNumber(false, true)
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
		return SUB, tokens[SUB]
	case '/':
		_, _ = s.ScanWhiteSpace()
		if r := s.read(); r == '*' {
			if err := s.skipUntilEndComment(); err != nil {
				return ILLEGAL, ""
			}
			return COMMENT, ""
		} else {
			s.unread()
		}
		return DIV, tokens[DIV]
	case '.':
		if r := s.read(); isDigit(r) {
			s.unread()
			return s.ScanNumber(true, false)
		}
		s.unread()
		return DOT, tokens[DOT]
	case '%':
		return MOD, tokens[MOD]
	case '&':
		return BITWISE_AND, tokens[BITWISE_AND]
	case '|':
		return BITWISE_OR, tokens[BITWISE_OR]
	case '^':
		return BITWISE_XOR, tokens[BITWISE_XOR]
	case '*':
		return ASTERISK, tokens[ASTERISK]
	case ',':
		return COMMA, tokens[COMMA]
	case '(':
		return LPAREN, tokens[LPAREN]
	case ')':
		return RPAREN, tokens[RPAREN]
	case '[':
		return LBRACKET, tokens[LBRACKET]
	case ']':
		return RBRACKET, tokens[RBRACKET]
	case ':':
		return COLON, tokens[COLON]
	case '#':
		return HASH, tokens[HASH]
	case ';':
		return SEMICOLON, tokens[SEMICOLON]
	}
	return ILLEGAL, ""
}

func (s *Scanner) ScanIdent() (tok Token, lit string) {
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
		return SELECT, lit
	case "AS":
		return AS, lit
	case "FROM":
		return FROM, lit
	case "WHERE":
		return WHERE, lit
	case "AND":
		return AND, lit
	case "OR":
		return OR, lit
	case "GROUP":
		return GROUP, lit
	case "HAVING":
		return HAVING, lit
	case "ORDER":
		return ORDER, lit
	case "BY":
		return BY, lit
	case "DESC":
		return DESC, lit
	case "ASC":
		return ASC, lit
	case "INNER":
		return INNER, lit
	case "LEFT":
		return LEFT, lit
	case "RIGHT":
		return RIGHT, lit
	case "FULL":
		return FULL, lit
	case "CROSS":
		return CROSS, lit
	case "JOIN":
		return JOIN, lit
	case "ON":
		return ON, lit
	case "CREATE":
		return CREATE, lit
	case "DROP":
		return DROP, lit
	case "EXPLAIN":
		return EXPLAIN, lit
	case "DESCRIBE":
		return DESCRIBE, lit
	case "SHOW":
		return SHOW, lit
	case "STREAM":
		return STREAM, lit
	case "STREAMS":
		return STREAMS, lit
	case "WITH":
		return WITH, lit
	case "BIGINT":
		return XBIGINT, lit
	case "FLOAT":
		return XFLOAT, lit
	case "DATETIME":
		return XDATETIME, lit
	case "STRING":
		return XSTRING, lit
	case "BOOLEAN":
		return XBOOLEAN, lit
	case "ARRAY":
		return XARRAY, lit
	case "STRUCT":
		return XSTRUCT, lit
	case "DATASOURCE":
		return DATASOURCE, lit
	case "KEY":
		return KEY, lit
	case "FORMAT":
		return FORMAT, lit
	case "CONF_KEY":
		return CONF_KEY, lit
	case "TYPE":
		return TYPE, lit
	case "TRUE":
		return TRUE, lit
	case "FALSE":
		return FALSE, lit
	case "STRICT_VALIDATION":
		return STRICT_VALIDATION, lit
	case "TIMESTAMP":
		return TIMESTAMP, lit
	case "TIMESTAMP_FORMAT":
		return TIMESTAMP_FORMAT, lit
	case "DD":
		return DD, lit
	case "HH":
		return HH, lit
	case "MI":
		return MI, lit
	case "SS":
		return SS, lit
	case "MS":
		return MS, lit
	}

	return IDENT, buf.String()
}

func (s *Scanner) ScanString() (tok Token, lit string) {
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
			return BADSTRING, buf.String()
		} else if ch == '\\' && !escape {
			escape = true
			buf.WriteRune(ch)
		} else {
			escape = false
			buf.WriteRune(ch)
		}
	}
	r, _ := strconv.Unquote(buf.String())
	return STRING, r
}

func (s *Scanner) ScanDigit() (tok Token, lit string) {
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
	return INTEGER, buf.String()
}

func (s *Scanner) ScanNumber(startWithDot bool, isNeg bool) (tok Token, lit string) {
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
		return NUMBER, buf.String()
	} else {
		return INTEGER, buf.String()
	}
}

func (s *Scanner) ScanBackquoteIdent() (tok Token, lit string) {
	var buf bytes.Buffer
	for {
		ch := s.read()
		if isBackquote(ch) || ch == eof {
			break
		} else {
			buf.WriteRune(ch)
		}
	}
	return IDENT, buf.String()
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

func (s *Scanner) ScanWhiteSpace() (tok Token, lit string) {
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
	return WS, buf.String()
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

func (tok Token) isOperator() bool {
	return (tok > operatorBeg && tok < operatorEnd) || tok == ASTERISK || tok == LBRACKET
}

func (tok Token) isTimeLiteral() bool { return tok >= DD && tok <= MS }

func (tok Token) allowedSourceToken() bool {
	return tok == IDENT || tok == DIV || tok == HASH || tok == ADD
}

//Allowed special field name token
func (tok Token) allowedSFNToken() bool { return tok == DOT }

func (tok Token) Precedence() int {
	switch tok {
	case OR:
		return 1
	case AND:
		return 2
	case EQ, NEQ, LT, LTE, GT, GTE:
		return 3
	case ADD, SUB, BITWISE_OR, BITWISE_XOR:
		return 4
	case MUL, DIV, MOD, BITWISE_AND, SUBSET, ARROW:
		return 5
	}
	return 0
}

type DataType int

const (
	UNKNOWN DataType = iota
	BIGINT
	FLOAT
	STRINGS
	DATETIME
	BOOLEAN
	ARRAY
	STRUCT
)

var dataTypes = []string{
	BIGINT:   "bigint",
	FLOAT:    "float",
	STRINGS:  "string",
	DATETIME: "datetime",
	BOOLEAN:  "boolean",
	ARRAY:    "array",
	STRUCT:   "struct",
}

func (d DataType) isSimpleType() bool {
	return d >= BIGINT && d <= BOOLEAN
}

func (d DataType) String() string {
	if d >= 0 && d < DataType(len(dataTypes)) {
		return dataTypes[d]
	}
	return ""
}

func getDataType(tok Token) DataType {
	switch tok {
	case XBIGINT:
		return BIGINT
	case XFLOAT:
		return FLOAT
	case XSTRING:
		return STRINGS
	case XDATETIME:
		return DATETIME
	case XBOOLEAN:
		return BOOLEAN
	case XARRAY:
		return ARRAY
	case XSTRUCT:
		return STRUCT
	}
	return UNKNOWN
}
