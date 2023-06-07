// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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

package ast

import "strings"

type Token int

const (
	// Special Tokens
	ILLEGAL Token = iota
	EOF
	WS
	COMMENT

	AS
	// Literals
	IDENT // main

	INTEGER     // 12345
	NUMBER      // 12345.67
	STRING      // "abc"
	SINGLEQUOTE // 'abc'
	BADSTRING   // "abc

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
	IN     // IN
	NOT    // NOT
	NOTIN  // NOT
	BETWEEN
	NOTBETWEEN
	LIKE
	NOTLIKE

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
	FILTER
	CASE
	WHEN
	THEN
	ELSE
	END
	OVER
	PARTITION

	TRUE
	FALSE

	DD
	HH
	MI
	SS
	MS
)

var Tokens = []string{
	ILLEGAL:     "ILLEGAL",
	EOF:         "EOF",
	AS:          "AS",
	WS:          "WS",
	IDENT:       "IDENT",
	INTEGER:     "INTEGER",
	NUMBER:      "NUMBER",
	STRING:      "STRING",
	SINGLEQUOTE: "SINGLEQUOTE",

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
	IN:     "IN",

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

	SELECT:    "SELECT",
	FROM:      "FROM",
	JOIN:      "JOIN",
	LEFT:      "LEFT",
	INNER:     "INNER",
	ON:        "ON",
	WHERE:     "WHERE",
	GROUP:     "GROUP",
	ORDER:     "ORDER",
	HAVING:    "HAVING",
	BY:        "BY",
	ASC:       "ASC",
	DESC:      "DESC",
	FILTER:    "FILTER",
	CASE:      "CASE",
	WHEN:      "WHEN",
	THEN:      "THEN",
	ELSE:      "ELSE",
	END:       "END",
	OVER:      "OVER",
	PARTITION: "PARTITION",

	AND:        "AND",
	OR:         "OR",
	TRUE:       "TRUE",
	FALSE:      "FALSE",
	NOTIN:      "NOT IN",
	BETWEEN:    "BETWEEN",
	NOTBETWEEN: "NOT BETWEEN",
	LIKE:       "LIKE",
	NOTLIKE:    "NOT LIKE",

	DD: "DD",
	HH: "HH",
	MI: "MI",
	SS: "SS",
	MS: "MS",
}

const (
	SELECT_LIT = "SELECT"
	CREATE     = "CREATE"
	DROP       = "DROP"
	EXPLAIN    = "EXPLAIN"
	DESCRIBE   = "DESCRIBE"
	SHOW       = "SHOW"
	STREAM     = "STREAM"
	TABLE      = "TABLE"
	STREAMS    = "STREAMS"
	TABLES     = "TABLES"
	WITH       = "WITH"

	DATASOURCE        = "DATASOURCE"
	KEY               = "KEY"
	FORMAT            = "FORMAT"
	CONF_KEY          = "CONF_KEY"
	TYPE              = "TYPE"
	STRICT_VALIDATION = "STRICT_VALIDATION"
	TIMESTAMP         = "TIMESTAMP"
	TIMESTAMP_FORMAT  = "TIMESTAMP_FORMAT"
	RETAIN_SIZE       = "RETAIN_SIZE"
	SHARED            = "SHARED"
	SCHEMAID          = "SCHEMAID"
	KIND              = "KIND"
	DELIMITER         = "DELIMITER"

	XBIGINT   = "BIGINT"
	XFLOAT    = "FLOAT"
	XSTRING   = "STRING"
	XBYTEA    = "BYTEA"
	XDATETIME = "DATETIME"
	XBOOLEAN  = "BOOLEAN"
	XARRAY    = "ARRAY"
	XSTRUCT   = "STRUCT"
)

var StreamTokens = map[string]struct{}{
	DATASOURCE:        {},
	KEY:               {},
	FORMAT:            {},
	CONF_KEY:          {},
	TYPE:              {},
	STRICT_VALIDATION: {},
	TIMESTAMP:         {},
	TIMESTAMP_FORMAT:  {},
	RETAIN_SIZE:       {},
	SHARED:            {},
	SCHEMAID:          {},
	KIND:              {},
	DELIMITER:         {},
}

var StreamDataTypes = map[string]DataType{
	XBIGINT:   BIGINT,
	XFLOAT:    FLOAT,
	XSTRING:   STRINGS,
	XBYTEA:    BYTEA,
	XDATETIME: DATETIME,
	XBOOLEAN:  BOOLEAN,
	XARRAY:    ARRAY,
	XSTRUCT:   STRUCT,
}

func IsStreamOptionKeyword(_ Token, lit string) bool {
	// token is always IDENT
	_, ok := StreamTokens[lit]
	return ok
}

var COLUMN_SEPARATOR = Tokens[COLSEP]

func (tok Token) String() string {
	if tok >= 0 && tok < Token(len(Tokens)) {
		return Tokens[tok]
	}
	return ""
}

func (tok Token) IsOperator() bool {
	return (tok > operatorBeg && tok < operatorEnd) || tok == ASTERISK || tok == LBRACKET || tok == DOT
}

func (tok Token) IsTimeLiteral() bool { return tok >= DD && tok <= MS }

func (tok Token) AllowedSourceToken() bool {
	return tok == IDENT || tok == DIV || tok == HASH || tok == ADD
}

// Allowed special field name token
func (tok Token) AllowedSFNToken() bool { return tok == DOT }

func (tok Token) Precedence() int {
	switch tok {
	case OR:
		return 1
	case AND:
		return 2
	case EQ, NEQ, LT, LTE, GT, GTE, IN, NOTIN, BETWEEN, NOTBETWEEN, LIKE, NOTLIKE:
		return 3
	case ADD, SUB, BITWISE_OR, BITWISE_XOR:
		return 4
	case MUL, DIV, MOD, BITWISE_AND, SUBSET, ARROW, DOT:
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
	BYTEA
	DATETIME
	BOOLEAN
	ARRAY
	STRUCT
)

var dataTypes = []string{
	BIGINT:   "bigint",
	FLOAT:    "float",
	STRINGS:  "string",
	BYTEA:    "bytea",
	DATETIME: "datetime",
	BOOLEAN:  "boolean",
	ARRAY:    "array",
	STRUCT:   "struct",
}

func (d DataType) IsSimpleType() bool {
	return d >= BIGINT && d <= BOOLEAN
}

func (d DataType) String() string {
	if d >= 0 && d < DataType(len(dataTypes)) {
		return dataTypes[d]
	}
	return ""
}

func GetDataType(lit string) DataType {
	lit = strings.ToUpper(lit)
	if dt, ok := StreamDataTypes[lit]; ok {
		return dt
	}
	return UNKNOWN
}
