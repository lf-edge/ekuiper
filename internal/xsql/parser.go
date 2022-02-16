// Copyright 2022 EMQ Technologies Co., Ltd.
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
	"fmt"
	"github.com/golang-collections/collections/stack"
	"github.com/lf-edge/ekuiper/internal/binder/function"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/message"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"
)

type Parser struct {
	s *Scanner

	i   int // buffer index
	n   int // buffer char count
	buf [3]struct {
		tok ast.Token
		lit string
	}
	inFunc string // currently parsing function name
	f      int    // anonymous field index number
	clause string
}

func (p *Parser) parseCondition() (ast.Expr, error) {
	if tok, _ := p.scanIgnoreWhitespace(); tok != ast.WHERE {
		p.unscan()
		return nil, nil
	}
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}
	return expr, nil
}

func (p *Parser) scan() (tok ast.Token, lit string) {
	if p.n > 0 {
		p.n--
		return p.curr()
	}

	tok, lit = p.s.Scan()

	if tok != ast.WS && tok != ast.COMMENT {
		p.i = (p.i + 1) % len(p.buf)
		buf := &p.buf[p.i]
		buf.tok, buf.lit = tok, lit
	}

	return
}

func (p *Parser) curr() (ast.Token, string) {
	i := (p.i - p.n + len(p.buf)) % len(p.buf)
	buf := &p.buf[i]
	return buf.tok, buf.lit
}

func (p *Parser) scanIgnoreWhitespace() (tok ast.Token, lit string) {
	tok, lit = p.scan()

	for {
		if tok == ast.WS || tok == ast.COMMENT {
			tok, lit = p.scan()
		} else {
			break
		}
	}
	return tok, lit
}

func (p *Parser) unscan() { p.n++ }

func NewParser(r io.Reader) *Parser {
	return &Parser{s: NewScanner(r)}
}

func (p *Parser) ParseQueries() ([]ast.SelectStatement, error) {
	var stmts []ast.SelectStatement

	if stmt, err := p.Parse(); err != nil {
		return nil, err
	} else {
		stmts = append(stmts, *stmt)
	}

	for {
		if tok, _ := p.scanIgnoreWhitespace(); tok == ast.SEMICOLON {
			if stmt, err := p.Parse(); err != nil {
				return nil, err
			} else {
				if stmt != nil {
					stmts = append(stmts, *stmt)
				}
			}
		} else if tok == ast.EOF {
			break
		}
	}

	return stmts, nil
}

func (p *Parser) Parse() (*ast.SelectStatement, error) {
	selects := &ast.SelectStatement{}

	if tok, lit := p.scanIgnoreWhitespace(); tok == ast.EOF {
		return nil, nil
	} else if tok != ast.SELECT {
		return nil, fmt.Errorf("Found %q, Expected SELECT.\n", lit)
	}
	p.clause = "select"
	if fields, err := p.parseFields(); err != nil {
		return nil, err
	} else {
		selects.Fields = fields
	}
	p.clause = "from"
	if src, err := p.parseSource(); err != nil {
		return nil, err
	} else {
		selects.Sources = src
	}
	p.clause = "join"
	if joins, err := p.parseJoins(); err != nil {
		return nil, err
	} else {
		selects.Joins = joins
	}
	p.clause = "where"
	if exp, err := p.parseCondition(); err != nil {
		return nil, err
	} else {
		if exp != nil {
			selects.Condition = exp
		}
	}
	p.clause = "groupby"
	if dims, err := p.parseDimensions(); err != nil {
		return nil, err
	} else {
		selects.Dimensions = dims
	}
	p.clause = "having"
	if having, err := p.parseHaving(); err != nil {
		return nil, err
	} else {
		selects.Having = having
	}
	p.clause = "orderby"
	if sorts, err := p.parseSorts(); err != nil {
		return nil, err
	} else {
		selects.SortFields = sorts
	}
	p.clause = ""
	if tok, lit := p.scanIgnoreWhitespace(); tok == ast.SEMICOLON {
		p.unscan()
		return selects, nil
	} else if tok != ast.EOF {
		return nil, fmt.Errorf("found %q, expected EOF.", lit)
	}

	if err := Validate(selects); err != nil {
		return nil, err
	}

	return selects, nil
}

func (p *Parser) parseSource() (ast.Sources, error) {
	var sources ast.Sources
	if tok, lit := p.scanIgnoreWhitespace(); tok != ast.FROM {
		return nil, fmt.Errorf("found %q, expected FROM.", lit)
	}

	if src, alias, err := p.parseSourceLiteral(); err != nil {
		return nil, err
	} else {
		sources = append(sources, &ast.Table{Name: src, Alias: alias})
	}

	return sources, nil
}

//TODO Current func has problems when the source includes white space.
func (p *Parser) parseSourceLiteral() (string, string, error) {
	var sourceSeg []string
	var alias string
	for {
		//HASH, DIV & ADD token is specially support for MQTT topic name patterns.
		if tok, lit := p.scanIgnoreWhitespace(); tok.AllowedSourceToken() {
			sourceSeg = append(sourceSeg, lit)
			if tok1, lit1 := p.scanIgnoreWhitespace(); tok1 == ast.AS {
				if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == ast.IDENT {
					alias = lit2
				} else {
					return "", "", fmt.Errorf("found %q, expected JOIN key word.", lit)
				}
			} else if tok1.AllowedSourceToken() {
				sourceSeg = append(sourceSeg, lit1)
			} else {
				p.unscan()
				break
			}
		} else {
			p.unscan()
			break
		}
	}
	return strings.Join(sourceSeg, ""), alias, nil
}

func (p *Parser) parseFieldNameSections() ([]string, error) {
	var fieldNameSects []string
	for {
		if tok, lit := p.scanIgnoreWhitespace(); tok == ast.IDENT || tok == ast.ASTERISK {
			fieldNameSects = append(fieldNameSects, lit)
			if tok1, _ := p.scanIgnoreWhitespace(); !tok1.AllowedSFNToken() {
				p.unscan()
				break
			}
		} else {
			p.unscan()
			break
		}
	}
	if len(fieldNameSects) == 0 {
		return nil, fmt.Errorf("Cannot find any field name.\n")
	} else if len(fieldNameSects) > 2 {
		return nil, fmt.Errorf("Too many field names. Please use -> to reference keys in struct.\n")
	}
	return fieldNameSects, nil
}

func (p *Parser) parseJoins() (ast.Joins, error) {
	var joins ast.Joins
	for {
		if tok, lit := p.scanIgnoreWhitespace(); tok == ast.INNER || tok == ast.LEFT || tok == ast.RIGHT || tok == ast.FULL || tok == ast.CROSS {
			if tok1, _ := p.scanIgnoreWhitespace(); tok1 == ast.JOIN {
				var jt = ast.INNER_JOIN
				switch tok {
				case ast.INNER:
					jt = ast.INNER_JOIN
				case ast.LEFT:
					jt = ast.LEFT_JOIN
				case ast.RIGHT:
					jt = ast.RIGHT_JOIN
				case ast.FULL:
					jt = ast.FULL_JOIN
				case ast.CROSS:
					jt = ast.CROSS_JOIN
				}

				if j, err := p.ParseJoin(jt); err != nil {
					return nil, err
				} else {
					joins = append(joins, *j)
				}
			} else {
				return nil, fmt.Errorf("found %q, expected JOIN key word.", lit)
			}
		} else {
			p.unscan()
			if len(joins) > 0 {
				return joins, nil
			}
			return nil, nil
		}
	}
}

func (p *Parser) ParseJoin(joinType ast.JoinType) (*ast.Join, error) {
	var j = &ast.Join{JoinType: joinType}
	if src, alias, err := p.parseSourceLiteral(); err != nil {
		return nil, err
	} else {
		j.Name = src
		j.Alias = alias
		if tok1, _ := p.scanIgnoreWhitespace(); tok1 == ast.ON {
			if ast.CROSS_JOIN == joinType {
				return nil, fmt.Errorf("On expression is not required for cross join type.\n")
			}
			if exp, err := p.ParseExpr(); err != nil {
				return nil, err
			} else {
				j.Expr = exp
			}
		} else {
			p.unscan()
		}
	}
	return j, nil
}

func (p *Parser) parseDimensions() (ast.Dimensions, error) {
	var ds ast.Dimensions
	if t, _ := p.scanIgnoreWhitespace(); t == ast.GROUP {
		if t1, l1 := p.scanIgnoreWhitespace(); t1 == ast.BY {
			for {
				if exp, err := p.ParseExpr(); err != nil {
					return nil, err
				} else {
					d := ast.Dimension{Expr: exp}
					ds = append(ds, d)
				}
				if tok, _ := p.scanIgnoreWhitespace(); tok == ast.COMMA {
					continue
				} else {
					p.unscan()
					break
				}
			}
		} else {
			return nil, fmt.Errorf("found %q, expected BY statement.", l1)
		}
	} else {
		p.unscan()
	}
	return ds, nil
}

func (p *Parser) parseHaving() (ast.Expr, error) {
	if tok, _ := p.scanIgnoreWhitespace(); tok != ast.HAVING {
		p.unscan()
		return nil, nil
	}
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}
	return expr, nil
}

func (p *Parser) parseSorts() (ast.SortFields, error) {
	var ss ast.SortFields
	if t, _ := p.scanIgnoreWhitespace(); t == ast.ORDER {
		if t1, l1 := p.scanIgnoreWhitespace(); t1 == ast.BY {
			for {
				if t1, l1 = p.scanIgnoreWhitespace(); t1 == ast.IDENT {
					s := ast.SortField{Ascending: true}

					p.unscan()
					if name, err := p.parseFieldNameSections(); err == nil {
						if len(name) == 2 {
							s.StreamName = ast.StreamName(name[0])
							s.Name = name[1]
						} else {
							s.Name = name[0]
						}
						s.Uname = strings.Join(name, ast.COLUMN_SEPARATOR)
					} else {
						return nil, err
					}

					if t2, _ := p.scanIgnoreWhitespace(); t2 == ast.DESC {
						s.Ascending = false
						ss = append(ss, s)
					} else if t2 == ast.ASC {
						ss = append(ss, s)
					} else {
						ss = append(ss, s)
						p.unscan()
						continue
					}
				} else if t1 == ast.COMMA {
					continue
				} else {
					p.unscan()
					break
				}
			}
		} else {
			return nil, fmt.Errorf("found %q, expected BY keyword.", l1)
		}
	} else {
		p.unscan()
	}

	return ss, nil
}

func (p *Parser) parseFields() (ast.Fields, error) {
	var fields ast.Fields

	tok, _ := p.scanIgnoreWhitespace()
	if tok == ast.ASTERISK {
		fields = append(fields, ast.Field{AName: "", Expr: &ast.Wildcard{Token: tok}})
		return fields, nil
	}
	p.unscan()

	for {
		field, err := p.parseField()

		if err != nil {
			return nil, err
		} else {
			fields = append(fields, *field)
		}

		tok, _ = p.scanIgnoreWhitespace()
		if tok != ast.COMMA {
			p.unscan()
			break
		}
	}
	return fields, nil
}

func (p *Parser) parseField() (*ast.Field, error) {
	field := &ast.Field{}
	if exp, err := p.ParseExpr(); err != nil {
		return nil, err
	} else {
		field.Name = nameExpr(exp)
		field.Expr = exp
	}

	if alias, err := p.parseAlias(); err != nil {
		return nil, err
	} else {
		if alias != "" {
			field.AName = alias
		}
	}
	if field.Name == "" && field.AName == "" {
		field.Name = DEFAULT_FIELD_NAME_PREFIX + strconv.Itoa(p.f)
		p.f += 1
	}

	return field, nil
}

func nameExpr(exp ast.Expr) string {
	switch e := exp.(type) {
	case *ast.FieldRef:
		return e.Name
	case *ast.Call:
		return e.Name
	default:
		return ""
	}
}

func (p *Parser) parseAlias() (string, error) {
	tok, lit := p.scanIgnoreWhitespace()
	if tok == ast.AS {
		if tok, lit = p.scanIgnoreWhitespace(); tok != ast.IDENT {
			return "", fmt.Errorf("found %q, expected as alias.", lit)
		} else {
			return lit, nil
		}
	}
	p.unscan()
	return "", nil
}

func (p *Parser) ParseExpr() (ast.Expr, error) {
	var err error
	root := &ast.BinaryExpr{}

	root.RHS, err = p.parseUnaryExpr(false)
	if err != nil {
		return nil, err
	}

	for {
		op, _ := p.scanIgnoreWhitespace()
		if !op.IsOperator() {
			p.unscan()
			return root.RHS, nil
		} else if op == ast.ASTERISK { //Change the asterisk to Mul token.
			op = ast.MUL
		} else if op == ast.LBRACKET { //LBRACKET is a special token, need to unscan
			op = ast.SUBSET
			p.unscan()
		}

		var rhs ast.Expr
		if rhs, err = p.parseUnaryExpr(op == ast.ARROW); err != nil {
			return nil, err
		}

		for node := root; ; {
			r, ok := node.RHS.(*ast.BinaryExpr)
			if !ok || r.OP.Precedence() >= op.Precedence() {
				node.RHS = &ast.BinaryExpr{LHS: node.RHS, RHS: rhs, OP: op}
				break
			}
			node = r
		}
	}
}

func (p *Parser) parseUnaryExpr(isSubField bool) (ast.Expr, error) {
	if tok1, _ := p.scanIgnoreWhitespace(); tok1 == ast.LPAREN {
		expr, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}
		// Expect an RPAREN at the end.
		if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 != ast.RPAREN {
			return nil, fmt.Errorf("found %q, expected right paren.", lit2)
		}

		return &ast.ParenExpr{Expr: expr}, nil
	} else if tok1 == ast.LBRACKET {
		return p.parseBracketExpr()
	}

	p.unscan()

	tok, lit := p.scanIgnoreWhiteSpaceWithNegativeNum()
	if tok == ast.CASE {
		return p.parseCaseExpr()
	} else if tok == ast.IDENT {
		if tok1, _ := p.scanIgnoreWhitespace(); tok1 == ast.LPAREN {
			return p.parseCall(lit)
		}
		p.unscan() //Back the Lparen token
		p.unscan() //Back the ident token
		if n, err := p.parseFieldNameSections(); err != nil {
			return nil, err
		} else {
			if p.inmeta() {
				if len(n) == 2 {
					return &ast.MetaRef{StreamName: ast.StreamName(n[0]), Name: n[1]}, nil
				}
				if isSubField {
					return &ast.JsonFieldRef{Name: n[0]}, nil
				}
				return &ast.MetaRef{StreamName: ast.DefaultStream, Name: n[0]}, nil
			} else {
				if len(n) == 2 {
					return &ast.FieldRef{StreamName: ast.StreamName(n[0]), Name: n[1]}, nil
				}
				if isSubField {
					return &ast.JsonFieldRef{Name: n[0]}, nil
				}
				return &ast.FieldRef{StreamName: ast.DefaultStream, Name: n[0]}, nil
			}
		}
	} else if tok == ast.STRING {
		return &ast.StringLiteral{Val: lit}, nil
	} else if tok == ast.INTEGER {
		val, _ := strconv.Atoi(lit)
		return &ast.IntegerLiteral{Val: val}, nil
	} else if tok == ast.NUMBER {
		if v, err := strconv.ParseFloat(lit, 64); err != nil {
			return nil, fmt.Errorf("found %q, invalid number value.", lit)
		} else {
			return &ast.NumberLiteral{Val: v}, nil
		}
	} else if tok == ast.TRUE || tok == ast.FALSE {
		if v, err := strconv.ParseBool(lit); err != nil {
			return nil, fmt.Errorf("found %q, invalid boolean value.", lit)
		} else {
			return &ast.BooleanLiteral{Val: v}, nil
		}
	} else if tok.IsTimeLiteral() {
		return &ast.TimeLiteral{Val: tok}, nil
	} else if tok == ast.ASTERISK {
		return p.parseAsterisk()
	}

	return nil, fmt.Errorf("found %q, expected expression.", lit)
}

func (p *Parser) parseBracketExpr() (ast.Expr, error) {
	tok2, lit2 := p.scanIgnoreWhiteSpaceWithNegativeNum()
	if tok2 == ast.RBRACKET {
		//field[]
		return &ast.ColonExpr{Start: &ast.IntegerLiteral{Val: 0}, End: &ast.IntegerLiteral{Val: math.MinInt32}}, nil
	} else if tok2 == ast.INTEGER {
		start, err := strconv.Atoi(lit2)
		if err != nil {
			return nil, fmt.Errorf("The start index %s is not an int value in bracket expression.", lit2)
		}
		if tok3, _ := p.scanIgnoreWhitespace(); tok3 == ast.RBRACKET {
			//Such as field[2]
			return &ast.IndexExpr{Index: &ast.IntegerLiteral{Val: start}}, nil
		} else if tok3 == ast.COLON {
			//Such as field[2:] or field[2:4]
			return p.parseColonExpr(&ast.IntegerLiteral{Val: start})
		}
	} else if tok2 == ast.COLON {
		//Such as field[:3] or [:]
		return p.parseColonExpr(&ast.IntegerLiteral{Val: 0})
	} else {
		p.unscan()
		start, err := p.ParseExpr()
		if err != nil {
			return nil, fmt.Errorf("The start index %s is invalid in bracket expression.", lit2)
		}
		if tok3, _ := p.scanIgnoreWhitespace(); tok3 == ast.RBRACKET {
			//Such as field[2]
			return &ast.IndexExpr{Index: start}, nil
		} else if tok3 == ast.COLON {
			//Such as field[2:] or field[2:4]
			return p.parseColonExpr(start)
		}
	}
	return nil, fmt.Errorf("Unexpected token %q. when parsing bracket expressions.", lit2)
}

func (p *Parser) parseColonExpr(start ast.Expr) (ast.Expr, error) {
	tok, lit := p.scanIgnoreWhiteSpaceWithNegativeNum()
	if tok == ast.INTEGER {
		end, err := strconv.Atoi(lit)
		if err != nil {
			return nil, fmt.Errorf("The end index %s is not an int value in bracket expression.", lit)
		}

		if tok1, lit1 := p.scanIgnoreWhitespace(); tok1 == ast.RBRACKET {
			return &ast.ColonExpr{Start: start, End: &ast.IntegerLiteral{Val: end}}, nil
		} else {
			return nil, fmt.Errorf("Found %q, expected right bracket.", lit1)
		}
	} else if tok == ast.RBRACKET {
		return &ast.ColonExpr{Start: start, End: &ast.IntegerLiteral{Val: math.MinInt32}}, nil
	}
	return nil, fmt.Errorf("Found %q, expected right bracket.", lit)
}

func (p *Parser) scanIgnoreWhiteSpaceWithNegativeNum() (ast.Token, string) {
	tok, lit := p.scanIgnoreWhitespace()
	if tok == ast.SUB {
		_, _ = p.s.ScanWhiteSpace()
		r := p.s.read()
		if isDigit(r) {
			p.s.unread()
			tok, lit = p.s.ScanNumber(false, true)
		}
	}
	return tok, lit
}

func (p *Parser) parseAs(f *ast.Field) (*ast.Field, error) {
	tok, lit := p.scanIgnoreWhitespace()
	if tok != ast.IDENT {
		return nil, fmt.Errorf("found %q, expected as alias.", lit)
	}
	f.AName = lit
	return f, nil
}

var WindowFuncs = map[string]struct{}{
	"tumblingwindow": {},
	"hoppingwindow":  {},
	"sessionwindow":  {},
	"slidingwindow":  {},
	"countwindow":    {},
}

func convFuncName(n string) (string, bool) {
	lname := strings.ToLower(n)
	if _, ok := WindowFuncs[lname]; ok {
		return lname, ok
	} else {
		return function.ConvName(n)
	}
}

func (p *Parser) parseCall(n string) (ast.Expr, error) {
	// Check if n function exists and convert it to lowercase for built-in func
	name, ok := convFuncName(n)
	if !ok {
		return nil, fmt.Errorf("function %s not found", n)
	}
	p.inFunc = name
	defer func() { p.inFunc = "" }()
	ft := function.GetFuncType(name)
	if ft == function.FuncTypeCols && p.clause != "select" {
		return nil, fmt.Errorf("function %s can only be used inside the select clause", n)
	}
	var args []ast.Expr
	for {
		if tok, _ := p.scanIgnoreWhitespace(); tok == ast.RPAREN {
			if valErr := validateFuncs(name, nil); valErr != nil {
				return nil, valErr
			}
			return &ast.Call{Name: name, Args: args}, nil
		} else {
			p.unscan()
		}

		if exp, err := p.ParseExpr(); err != nil {
			return nil, err
		} else {
			if ft == function.FuncTypeCols {
				field := &ast.ColFuncField{Expr: exp, Name: nameExpr(exp)}
				args = append(args, field)
			} else {
				args = append(args, exp)
			}
		}

		if tok, _ := p.scanIgnoreWhitespace(); tok != ast.COMMA {
			p.unscan()
			break
		}
	}

	if tok, lit := p.scanIgnoreWhitespace(); tok != ast.RPAREN {
		return nil, fmt.Errorf("found function call %q, expected ), but with %q.", name, lit)
	}
	if wt, err := validateWindows(name, args); wt == ast.NOT_WINDOW {
		if valErr := validateFuncs(name, args); valErr != nil {
			return nil, valErr
		}
		// Add context for some aggregate func
		if name == "deduplicate" {
			args = append([]ast.Expr{&ast.Wildcard{Token: ast.ASTERISK}}, args...)
		}
		return &ast.Call{Name: name, Args: args}, nil
	} else {
		if err != nil {
			return nil, err
		}
		win, err := p.ConvertToWindows(wt, args)
		if err != nil {
			return nil, err
		}
		// parse filter clause
		f, err := p.parseFilter()
		if err != nil {
			return nil, err
		} else if f != nil {
			win.Filter = f
		}
		return win, nil
	}
}

func (p *Parser) parseCaseExpr() (*ast.CaseExpr, error) {
	c := &ast.CaseExpr{}
	tok, _ := p.scanIgnoreWhitespace()
	p.unscan()
	if tok != ast.WHEN { // no condition value for case, additional validation needed
		if exp, err := p.ParseExpr(); err != nil {
			return nil, err
		} else {
			c.Value = exp
		}
	}

loop:
	for {
		tok, _ := p.scanIgnoreWhitespace()
		switch tok {
		case ast.WHEN:
			if exp, err := p.ParseExpr(); err != nil {
				return nil, err
			} else {
				if c.WhenClauses == nil {
					c.WhenClauses = make([]*ast.WhenClause, 0)
				}
				if c.Value == nil && !ast.IsBooleanArg(exp) {
					return nil, fmt.Errorf("invalid CASE expression, WHEN expression must be a bool condition")
				}
				w := &ast.WhenClause{
					Expr: exp,
				}
				tokThen, _ := p.scanIgnoreWhitespace()
				if tokThen != ast.THEN {
					return nil, fmt.Errorf("invalid CASE expression, THEN expected after WHEN")
				} else {
					if expThen, err := p.ParseExpr(); err != nil {
						return nil, err
					} else {
						w.Result = expThen
						c.WhenClauses = append(c.WhenClauses, w)
					}
				}
			}
		case ast.ELSE:
			if c.WhenClauses != nil {
				if exp, err := p.ParseExpr(); err != nil {
					return nil, err
				} else {
					c.ElseClause = exp
				}
			} else {
				return nil, fmt.Errorf("invalid CASE expression, WHEN expected before ELSE")
			}
		case ast.END:
			if c.WhenClauses != nil {
				break loop
			} else {
				return nil, fmt.Errorf("invalid CASE expression, WHEN expected before END")
			}
		default:
			return nil, fmt.Errorf("invalid CASE expression, END expected")
		}
	}
	return c, nil
}

func validateWindows(fname string, args []ast.Expr) (ast.WindowType, error) {
	switch fname {
	case "tumblingwindow":
		if err := validateWindow(fname, 2, args); err != nil {
			return ast.TUMBLING_WINDOW, err
		}
		return ast.TUMBLING_WINDOW, nil
	case "hoppingwindow":
		if err := validateWindow(fname, 3, args); err != nil {
			return ast.HOPPING_WINDOW, err
		}
		return ast.HOPPING_WINDOW, nil
	case "sessionwindow":
		if err := validateWindow(fname, 3, args); err != nil {
			return ast.SESSION_WINDOW, err
		}
		return ast.SESSION_WINDOW, nil
	case "slidingwindow":
		if err := validateWindow(fname, 2, args); err != nil {
			return ast.SLIDING_WINDOW, err
		}
		return ast.SLIDING_WINDOW, nil
	case "countwindow":
		if len(args) == 1 {
			if para1, ok := args[0].(*ast.IntegerLiteral); ok && para1.Val > 0 {
				return ast.COUNT_WINDOW, nil
			} else {
				return ast.COUNT_WINDOW, fmt.Errorf("Invalid parameter value %s.", args[0])
			}
		} else if len(args) == 2 {
			if para1, ok1 := args[0].(*ast.IntegerLiteral); ok1 {
				if para2, ok2 := args[1].(*ast.IntegerLiteral); ok2 {
					if para1.Val < para2.Val {
						return ast.COUNT_WINDOW, fmt.Errorf("The second parameter value %d should be less than the first parameter %d.", para2.Val, para1.Val)
					} else {
						return ast.COUNT_WINDOW, nil
					}
				}
			}
			return ast.COUNT_WINDOW, fmt.Errorf("Invalid parameter value %s, %s.", args[0], args[1])
		} else {
			return ast.COUNT_WINDOW, fmt.Errorf("Invalid parameter count.")
		}

	}
	return ast.NOT_WINDOW, nil
}

func validateWindow(funcName string, expectLen int, args []ast.Expr) error {
	if len(args) != expectLen {
		return fmt.Errorf("The arguments for %s should be %d.\n", funcName, expectLen)
	}
	if _, ok := args[0].(*ast.TimeLiteral); !ok {
		return fmt.Errorf("The 1st argument for %s is expecting timer literal expression. One value of [dd|hh|mi|ss|ms].\n", funcName)
	}

	for i := 1; i < len(args); i++ {
		if _, ok := args[i].(*ast.IntegerLiteral); !ok {
			return fmt.Errorf("The %d argument for %s is expecting interger literal expression. \n", i, funcName)
		}
	}
	return nil

}

func (p *Parser) ConvertToWindows(wtype ast.WindowType, args []ast.Expr) (*ast.Window, error) {
	win := &ast.Window{WindowType: wtype}
	if wtype == ast.COUNT_WINDOW {
		win.Length = &ast.IntegerLiteral{Val: args[0].(*ast.IntegerLiteral).Val}
		if len(args) == 2 {
			win.Interval = &ast.IntegerLiteral{Val: args[1].(*ast.IntegerLiteral).Val}
		}
		return win, nil
	}
	var unit = 1
	v := args[0].(*ast.TimeLiteral).Val
	switch v {
	case ast.DD:
		unit = 24 * 3600 * 1000
	case ast.HH:
		unit = 3600 * 1000
	case ast.MI:
		unit = 60 * 1000
	case ast.SS:
		unit = 1000
	case ast.MS:
		unit = 1
	default:
		return nil, fmt.Errorf("Invalid timeliteral %s", v)
	}
	win.Length = &ast.IntegerLiteral{Val: args[1].(*ast.IntegerLiteral).Val * unit}
	if len(args) > 2 {
		win.Interval = &ast.IntegerLiteral{Val: args[2].(*ast.IntegerLiteral).Val * unit}
	} else {
		win.Interval = &ast.IntegerLiteral{Val: 0}
	}
	return win, nil
}

func (p *Parser) ParseCreateStmt() (ast.Statement, error) {
	if tok, _ := p.scanIgnoreWhitespace(); tok == ast.CREATE {
		tok1, lit1 := p.scanIgnoreWhitespace()
		stmt := &ast.StreamStmt{}
		switch tok1 {
		case ast.STREAM:
			stmt.StreamType = ast.TypeStream
		case ast.TABLE:
			stmt.StreamType = ast.TypeTable
		default:
			return nil, fmt.Errorf("found %q, expected keyword stream or table.", lit1)
		}
		if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == ast.IDENT {
			stmt.Name = ast.StreamName(lit2)
			if fields, err := p.parseStreamFields(); err != nil {
				return nil, err
			} else {
				stmt.StreamFields = fields
			}
			if opts, err := p.parseStreamOptions(); err != nil {
				return nil, err
			} else {
				stmt.Options = opts
			}
			if tok3, lit3 := p.scanIgnoreWhitespace(); tok3 == ast.SEMICOLON {
				p.unscan()
			} else if tok3 == ast.EOF {
				//Finish parsing create stream statement. Jump to validate
			} else {
				return nil, fmt.Errorf("found %q, expected semicolon or EOF.", lit3)
			}
		} else {
			return nil, fmt.Errorf("found %q, expected stream name.", lit2)
		}
		if valErr := validateStream(stmt); valErr != nil {
			return nil, valErr
		}
		return stmt, nil
	} else {
		p.unscan()
		return nil, nil
	}

}

// TODO more accurate validation for table
func validateStream(stmt *ast.StreamStmt) error {
	f := stmt.Options.FORMAT
	if f == "" {
		f = message.FormatJson
	}
	switch strings.ToLower(f) {
	case message.FormatJson:
		//do nothing
	case message.FormatBinary:
		if stmt.StreamType == ast.TypeTable {
			return fmt.Errorf("'binary' format is not supported for table")
		}
		switch len(stmt.StreamFields) {
		case 0:
			// do nothing for schemaless
		case 1:
			f := stmt.StreamFields[0]
			if bt, ok := f.FieldType.(*ast.BasicType); ok {
				if bt.Type == ast.BYTEA {
					break
				}
			}
			return fmt.Errorf("'binary' format stream can have only 'bytea' type field")
		default:
			return fmt.Errorf("'binary' format stream can have only one field")
		}
	default:
		return fmt.Errorf("option 'format=%s' is invalid", f)
	}
	return nil
}

func (p *Parser) parseShowStmt() (ast.Statement, error) {
	if tok, _ := p.scanIgnoreWhitespace(); tok == ast.SHOW {
		tok1, lit1 := p.scanIgnoreWhitespace()
		switch tok1 {
		case ast.STREAMS:
			ss := &ast.ShowStreamsStatement{}
			if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == ast.EOF || tok2 == ast.SEMICOLON {
				return ss, nil
			} else {
				return nil, fmt.Errorf("found %q, expected semecolon or EOF.", lit2)
			}
		case ast.TABLES:
			ss := &ast.ShowTablesStatement{}
			if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == ast.EOF || tok2 == ast.SEMICOLON {
				return ss, nil
			} else {
				return nil, fmt.Errorf("found %q, expected semecolon or EOF.", lit2)
			}
		default:
			return nil, fmt.Errorf("found %q, expected keyword streams or tables.", lit1)
		}
	} else {
		p.unscan()
		return nil, nil
	}
}

func (p *Parser) parseDescribeStmt() (ast.Statement, error) {
	if tok, _ := p.scanIgnoreWhitespace(); tok == ast.DESCRIBE {
		tok1, lit1 := p.scanIgnoreWhitespace()
		switch tok1 {
		case ast.STREAM:
			dss := &ast.DescribeStreamStatement{}
			if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == ast.IDENT {
				dss.Name = lit2
				return dss, nil
			} else {
				return nil, fmt.Errorf("found %q, expected stream name.", lit2)
			}
		case ast.TABLE:
			dss := &ast.DescribeTableStatement{}
			if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == ast.IDENT {
				dss.Name = lit2
				return dss, nil
			} else {
				return nil, fmt.Errorf("found %q, expected table name.", lit2)
			}
		default:
			return nil, fmt.Errorf("found %q, expected keyword stream or table.", lit1)
		}
	} else {
		p.unscan()
		return nil, nil
	}
}

func (p *Parser) parseExplainStmt() (ast.Statement, error) {
	if tok, _ := p.scanIgnoreWhitespace(); tok == ast.EXPLAIN {
		tok1, lit1 := p.scanIgnoreWhitespace()
		switch tok1 {
		case ast.STREAM:
			ess := &ast.ExplainStreamStatement{}
			if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == ast.IDENT {
				ess.Name = lit2
				return ess, nil
			} else {
				return nil, fmt.Errorf("found %q, expected stream name.", lit2)
			}
		case ast.TABLE:
			ess := &ast.ExplainTableStatement{}
			if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == ast.IDENT {
				ess.Name = lit2
				return ess, nil
			} else {
				return nil, fmt.Errorf("found %q, expected table name.", lit2)
			}
		default:
			return nil, fmt.Errorf("found %q, expected keyword stream or table.", lit1)
		}
	} else {
		p.unscan()
		return nil, nil
	}
}

func (p *Parser) parseDropStmt() (ast.Statement, error) {
	if tok, _ := p.scanIgnoreWhitespace(); tok == ast.DROP {
		tok1, lit1 := p.scanIgnoreWhitespace()
		switch tok1 {
		case ast.STREAM:
			ess := &ast.DropStreamStatement{}
			if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == ast.IDENT {
				ess.Name = lit2
				return ess, nil
			} else {
				return nil, fmt.Errorf("found %q, expected stream name.", lit2)
			}
		case ast.TABLE:
			ess := &ast.DropTableStatement{}
			if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == ast.IDENT {
				ess.Name = lit2
				return ess, nil
			} else {
				return nil, fmt.Errorf("found %q, expected table name.", lit2)
			}
		default:
			return nil, fmt.Errorf("found %q, expected keyword stream or table.", lit1)
		}
	} else {
		p.unscan()
		return nil, nil
	}
}

func (p *Parser) parseStreamFields() (ast.StreamFields, error) {
	lStack := &stack.Stack{}
	var fields ast.StreamFields
	if tok, lit := p.scanIgnoreWhitespace(); tok == ast.LPAREN {
		lStack.Push(lit)
		for {
			//For the schemaless streams
			//create stream demo () WITH (FORMAT="JSON", DATASOURCE="demo" TYPE="edgex")
			if tok1, _ := p.scanIgnoreWhitespace(); tok1 == ast.RPAREN {
				lStack.Pop()
				if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 != ast.WITH {
					return nil, fmt.Errorf("found %q, expected is with.", lit2)
				}
				return fields, nil
			} else {
				p.unscan()
			}
			if f, err := p.parseStreamField(); err != nil {
				return nil, err
			} else {
				fields = append(fields, *f)
			}

			if tok1, _ := p.scanIgnoreWhitespace(); tok1 == ast.RPAREN {
				lStack.Pop()
				if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == ast.WITH {
					//Check the stack for LPAREN; If the stack for LPAREN is not zero, then it's not correct.
					if lStack.Len() > 0 {
						return nil, fmt.Errorf("Parenthesis is not matched.")
					}
					break
				} else if tok2 == ast.COMMA {
					if lStack.Len() > 0 {
						return nil, fmt.Errorf("Parenthesis is in create record type not matched.")
					}
					p.unscan()
					break
				} else if tok2 == ast.RPAREN { //The nested type definition of ARRAY and Struct, such as "field ARRAY(STRUCT(f BIGINT))"
					if lStack.Len() > 0 {
						return nil, fmt.Errorf("Parenthesis is not matched.")
					}
					p.unscan()
					break
				} else {
					if lStack.Len() == 0 {
						return nil, fmt.Errorf("found %q, expected is with.", lit2)
					}
					p.unscan()
				}
			} else {
				p.unscan()
			}
		}

	} else {
		return nil, fmt.Errorf("found %q, expected lparen after stream name.", lit)
	}
	return fields, nil
}

func (p *Parser) parseStreamField() (*ast.StreamField, error) {
	field := &ast.StreamField{}
	if tok, lit := p.scanIgnoreWhitespace(); tok == ast.IDENT {
		field.Name = lit
		tok1, lit1 := p.scanIgnoreWhitespace()
		if t := ast.GetDataType(tok1); t != ast.UNKNOWN && t.IsSimpleType() {
			field.FieldType = &ast.BasicType{Type: t}
		} else if t == ast.ARRAY {
			if f, e := p.parseStreamArrayType(); e != nil {
				return nil, e
			} else {
				field.FieldType = f
			}
		} else if t == ast.STRUCT {
			if f, e := p.parseStreamStructType(); e != nil {
				return nil, e
			} else {
				field.FieldType = f
			}
		} else if t == ast.UNKNOWN {
			return nil, fmt.Errorf("found %q, expect valid stream field types(BIGINT | FLOAT | STRINGS | DATETIME | BOOLEAN | BYTEA | ARRAY | STRUCT).", lit1)
		}

		if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == ast.COMMA {
			//Just consume the comma.
		} else if tok2 == ast.RPAREN {
			p.unscan()
		} else {
			return nil, fmt.Errorf("found %q, expect comma or rparen.", lit2)
		}
	} else {
		return nil, fmt.Errorf("found %q, expect stream field name.", lit)
	}
	return field, nil
}

func (p *Parser) parseStreamArrayType() (ast.FieldType, error) {
	lStack := &stack.Stack{}
	if tok, _ := p.scanIgnoreWhitespace(); tok == ast.LPAREN {
		lStack.Push(ast.LPAREN)
		tok1, lit1 := p.scanIgnoreWhitespace()
		if t := ast.GetDataType(tok1); t != ast.UNKNOWN && t.IsSimpleType() {
			if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == ast.RPAREN {
				lStack.Pop()
				if lStack.Len() > 0 {
					return nil, fmt.Errorf("Parenthesis is in array type not matched.")
				}
				return &ast.ArrayType{Type: t}, nil
			} else {
				return nil, fmt.Errorf("found %q, expect rparen in array type definition.", lit2)
			}
		} else if tok1 == ast.XSTRUCT {
			if f, err := p.parseStreamStructType(); err != nil {
				return nil, err
			} else {
				if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == ast.RPAREN {
					lStack.Pop()
					if lStack.Len() > 0 {
						return nil, fmt.Errorf("Parenthesis is in struct of array type %q not matched.", tok1)
					}
					return &ast.ArrayType{Type: ast.STRUCT, FieldType: f}, nil
				} else {
					return nil, fmt.Errorf("found %q, expect rparen in struct of array type definition.", lit2)
				}
			}
		} else if tok1 == ast.COMMA {
			p.unscan()
		} else {
			return nil, fmt.Errorf("found %q, expect stream data types.", lit1)
		}
	} else {

	}
	return nil, nil
}

func (p *Parser) parseStreamStructType() (ast.FieldType, error) {
	rf := &ast.RecType{}
	if sfs, err := p.parseStreamFields(); err != nil {
		return nil, err
	} else {
		if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == ast.COMMA {
			rf.StreamFields = sfs
			p.unscan()
		} else if tok2 == ast.RPAREN {
			rf.StreamFields = sfs
			p.unscan()
		} else {
			return nil, fmt.Errorf("found %q, expect comma in create stream record statement.", lit2)
		}
	}
	return rf, nil
}

func (p *Parser) parseStreamOptions() (*ast.Options, error) {
	opts := &ast.Options{STRICT_VALIDATION: true}
	v := reflect.ValueOf(opts)
	lStack := &stack.Stack{}
	if tok, lit := p.scanIgnoreWhitespace(); tok == ast.LPAREN {
		lStack.Push(ast.LPAREN)
		for {
			if tok1, lit1 := p.scanIgnoreWhitespace(); tok1 == ast.DATASOURCE || tok1 == ast.FORMAT || tok1 == ast.KEY || tok1 == ast.CONF_KEY || tok1 == ast.STRICT_VALIDATION || tok1 == ast.TYPE || tok1 == ast.TIMESTAMP || tok1 == ast.TIMESTAMP_FORMAT || tok1 == ast.RETAIN_SIZE || tok1 == ast.SHARED {
				if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == ast.EQ {
					if tok3, lit3 := p.scanIgnoreWhitespace(); tok3 == ast.STRING {
						switch tok1 {
						case ast.STRICT_VALIDATION:
							if val := strings.ToUpper(lit3); (val != "TRUE") && (val != "FALSE") {
								return nil, fmt.Errorf("found %q, expect TRUE/FALSE value in %s option.", lit3, tok1)
							} else {
								opts.STRICT_VALIDATION = val == "TRUE"
							}
						case ast.RETAIN_SIZE:
							if val, err := strconv.Atoi(lit3); err != nil {
								return nil, fmt.Errorf("found %q, expect number value in %s option.", lit3, tok1)
							} else {
								opts.RETAIN_SIZE = val
							}
						case ast.SHARED:
							if val := strings.ToUpper(lit3); (val != "TRUE") && (val != "FALSE") {
								return nil, fmt.Errorf("found %q, expect TRUE/FALSE value in %s option.", lit3, tok1)
							} else {
								opts.SHARED = val == "TRUE"
							}
						default:
							f := v.Elem().FieldByName(lit1)
							if f.IsValid() {
								f.SetString(lit3)
							} else { // should not happen
								return nil, fmt.Errorf("invalid field %s.", lit1)
							}
						}
					} else {
						return nil, fmt.Errorf("found %q, expect string value in option.", lit3)
					}
				} else {
					return nil, fmt.Errorf("found %q, expect equals(=) in options.", lit2)
				}

			} else if tok1 == ast.COMMA {
				continue
			} else if tok1 == ast.RPAREN {
				if lStack.Pop(); lStack.Len() == 0 {
					break
				} else {
					return nil, fmt.Errorf("Parenthesis is not matched in options definition.")
				}
			} else {
				return nil, fmt.Errorf("found %q, unknown option keys(DATASOURCE|FORMAT|KEY|CONF_KEY|SHARED|STRICT_VALIDATION|TYPE|TIMESTAMP|TIMESTAMP_FORMAT|RETAIN_SIZE).", lit1)
			}
		}
	} else {
		return nil, fmt.Errorf("found %q, expect stream options.", lit)
	}
	return opts, nil
}

// Only support filter on window now
func (p *Parser) parseFilter() (ast.Expr, error) {
	if tok, _ := p.scanIgnoreWhitespace(); tok != ast.FILTER {
		p.unscan()
		return nil, nil
	}
	if tok, lit := p.scanIgnoreWhitespace(); tok != ast.LPAREN {
		return nil, fmt.Errorf("Found %q after FILTER, expect parentheses.", lit)
	}
	if tok, lit := p.scanIgnoreWhitespace(); tok != ast.WHERE {
		return nil, fmt.Errorf("Found %q after FILTER(, expect WHERE.", lit)
	}
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}
	if tok, lit := p.scanIgnoreWhitespace(); tok != ast.RPAREN {
		return nil, fmt.Errorf("Found %q after FILTER, expect right parentheses.", lit)
	}
	return expr, nil
}

func (p *Parser) parseAsterisk() (ast.Expr, error) {
	switch p.inFunc {
	case "mqtt", "meta":
		return &ast.MetaRef{StreamName: ast.DefaultStream, Name: "*"}, nil
	case "":
		return nil, fmt.Errorf("unsupported * expression, it must be used inside fields or function parameters.")
	default:
		return &ast.Wildcard{Token: ast.ASTERISK}, nil
	}
}

func (p *Parser) inmeta() bool {
	return p.inFunc == "meta" || p.inFunc == "mqtt"
}
