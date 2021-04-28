package xsql

import (
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/golang-collections/collections/stack"
	"io"
	"math"
	"strconv"
	"strings"
)

type Parser struct {
	s *Scanner

	i   int // buffer index
	n   int // buffer char count
	buf [3]struct {
		tok Token
		lit string
	}
	inmeta bool
}

func (p *Parser) parseCondition() (Expr, error) {
	if tok, _ := p.scanIgnoreWhitespace(); tok != WHERE {
		p.unscan()
		return nil, nil
	}
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}
	return expr, nil
}

func (p *Parser) scan() (tok Token, lit string) {
	if p.n > 0 {
		p.n--
		return p.curr()
	}

	tok, lit = p.s.Scan()

	if tok != WS && tok != COMMENT {
		p.i = (p.i + 1) % len(p.buf)
		buf := &p.buf[p.i]
		buf.tok, buf.lit = tok, lit
	}

	return
}

func (p *Parser) curr() (Token, string) {
	i := (p.i - p.n + len(p.buf)) % len(p.buf)
	buf := &p.buf[i]
	return buf.tok, buf.lit
}

func (p *Parser) scanIgnoreWhitespace() (tok Token, lit string) {
	tok, lit = p.scan()

	for {
		if tok == WS || tok == COMMENT {
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

func (p *Parser) ParseQueries() (SelectStatements, error) {
	var stmts SelectStatements

	if stmt, err := p.Parse(); err != nil {
		return nil, err
	} else {
		stmts = append(stmts, *stmt)
	}

	for {
		if tok, _ := p.scanIgnoreWhitespace(); tok == SEMICOLON {
			if stmt, err := p.Parse(); err != nil {
				return nil, err
			} else {
				if stmt != nil {
					stmts = append(stmts, *stmt)
				}
			}
		} else if tok == EOF {
			break
		}
	}

	return stmts, nil
}

func (p *Parser) Parse() (*SelectStatement, error) {
	selects := &SelectStatement{}

	if tok, lit := p.scanIgnoreWhitespace(); tok == EOF {
		return nil, nil
	} else if tok != SELECT {
		return nil, fmt.Errorf("Found %q, Expected SELECT.\n", lit)
	}

	if fields, err := p.parseFields(); err != nil {
		return nil, err
	} else {
		selects.Fields = fields
	}

	if src, err := p.parseSource(); err != nil {
		return nil, err
	} else {
		selects.Sources = src
	}

	if joins, err := p.parseJoins(); err != nil {
		return nil, err
	} else {
		selects.Joins = joins
	}

	if exp, err := p.parseCondition(); err != nil {
		return nil, err
	} else {
		if exp != nil {
			selects.Condition = exp
		}
	}

	if dims, err := p.parseDimensions(); err != nil {
		return nil, err
	} else {
		selects.Dimensions = dims
	}

	if having, err := p.parseHaving(); err != nil {
		return nil, err
	} else {
		selects.Having = having
	}

	if sorts, err := p.parseSorts(); err != nil {
		return nil, err
	} else {
		selects.SortFields = sorts
	}

	if tok, lit := p.scanIgnoreWhitespace(); tok == SEMICOLON {
		p.unscan()
		return selects, nil
	} else if tok != EOF {
		return nil, fmt.Errorf("found %q, expected EOF.", lit)
	}

	if err := Validate(selects); err != nil {
		return nil, err
	}

	return selects, nil
}

func (p *Parser) parseSource() (Sources, error) {
	var sources Sources
	if tok, lit := p.scanIgnoreWhitespace(); tok != FROM {
		return nil, fmt.Errorf("found %q, expected FROM.", lit)
	}

	if src, alias, err := p.parseSourceLiteral(); err != nil {
		return nil, err
	} else {
		sources = append(sources, &Table{Name: src, Alias: alias})
	}

	return sources, nil
}

//TODO Current func has problems when the source includes white space.
func (p *Parser) parseSourceLiteral() (string, string, error) {
	var sourceSeg []string
	var alias string
	for {
		//HASH, DIV & ADD token is specially support for MQTT topic name patterns.
		if tok, lit := p.scanIgnoreWhitespace(); tok.allowedSourceToken() {
			sourceSeg = append(sourceSeg, lit)
			if tok1, lit1 := p.scanIgnoreWhitespace(); tok1 == AS {
				if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == IDENT {
					alias = lit2
				} else {
					return "", "", fmt.Errorf("found %q, expected JOIN key word.", lit)
				}
			} else if tok1.allowedSourceToken() {
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
		if tok, lit := p.scanIgnoreWhitespace(); tok == IDENT || tok == ASTERISK {
			fieldNameSects = append(fieldNameSects, lit)
			if tok1, _ := p.scanIgnoreWhitespace(); !tok1.allowedSFNToken() {
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

func (p *Parser) parseJoins() (Joins, error) {
	var joins Joins
	for {
		if tok, lit := p.scanIgnoreWhitespace(); tok == INNER || tok == LEFT || tok == RIGHT || tok == FULL || tok == CROSS {
			if tok1, _ := p.scanIgnoreWhitespace(); tok1 == JOIN {
				var jt = INNER_JOIN
				switch tok {
				case INNER:
					jt = INNER_JOIN
				case LEFT:
					jt = LEFT_JOIN
				case RIGHT:
					jt = RIGHT_JOIN
				case FULL:
					jt = FULL_JOIN
				case CROSS:
					jt = CROSS_JOIN
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
	return joins, nil
}

func (p *Parser) ParseJoin(joinType JoinType) (*Join, error) {
	var j = &Join{JoinType: joinType}
	if src, alias, err := p.parseSourceLiteral(); err != nil {
		return nil, err
	} else {
		j.Name = src
		j.Alias = alias
		if tok1, _ := p.scanIgnoreWhitespace(); tok1 == ON {
			if CROSS_JOIN == joinType {
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

func (p *Parser) parseDimensions() (Dimensions, error) {
	var ds Dimensions
	if t, _ := p.scanIgnoreWhitespace(); t == GROUP {
		if t1, l1 := p.scanIgnoreWhitespace(); t1 == BY {
			for {
				if exp, err := p.ParseExpr(); err != nil {
					return nil, err
				} else {
					d := Dimension{Expr: exp}
					ds = append(ds, d)
				}
				if tok, _ := p.scanIgnoreWhitespace(); tok == COMMA {
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

func (p *Parser) parseHaving() (Expr, error) {
	if tok, _ := p.scanIgnoreWhitespace(); tok != HAVING {
		p.unscan()
		return nil, nil
	}
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}
	return expr, nil
}

func (p *Parser) parseSorts() (SortFields, error) {
	var ss SortFields
	if t, _ := p.scanIgnoreWhitespace(); t == ORDER {
		if t1, l1 := p.scanIgnoreWhitespace(); t1 == BY {
			for {
				if t1, l1 = p.scanIgnoreWhitespace(); t1 == IDENT {
					s := SortField{Ascending: true}

					p.unscan()
					if name, err := p.parseFieldNameSections(); err == nil {
						s.Name = strings.Join(name, tokens[COLSEP])
					} else {
						return nil, err
					}

					if t2, _ := p.scanIgnoreWhitespace(); t2 == DESC {
						s.Ascending = false
						ss = append(ss, s)
					} else if t2 == ASC {
						ss = append(ss, s)
					} else {
						ss = append(ss, s)
						p.unscan()
						continue
					}
				} else if t1 == COMMA {
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

func (p *Parser) parseFields() (Fields, error) {
	var fields Fields

	tok, _ := p.scanIgnoreWhitespace()
	if tok == ASTERISK {
		fields = append(fields, Field{AName: "", Expr: &Wildcard{Token: tok}})
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
		if tok != COMMA {
			p.unscan()
			break
		}
	}
	return fields, nil
}

func (p *Parser) parseField() (*Field, error) {
	field := &Field{}
	if exp, err := p.ParseExpr(); err != nil {
		return nil, err
	} else {
		if e, ok := exp.(*FieldRef); ok {
			field.Name = e.Name
		} else if e, ok := exp.(*Call); ok {
			field.Name = e.Name
		}
		field.Expr = exp
	}

	if alias, err := p.parseAlias(); err != nil {
		return nil, err
	} else {
		if alias != "" {
			field.AName = alias
		}
	}

	return field, nil
}

func (p *Parser) parseAlias() (string, error) {
	tok, lit := p.scanIgnoreWhitespace()
	if tok == AS {
		if tok, lit = p.scanIgnoreWhitespace(); tok != IDENT {
			return "", fmt.Errorf("found %q, expected as alias.", lit)
		} else {
			return lit, nil
		}
	}
	p.unscan()
	return "", nil
}

func (p *Parser) ParseExpr() (Expr, error) {
	var err error
	root := &BinaryExpr{}

	root.RHS, err = p.parseUnaryExpr()
	if err != nil {
		return nil, err
	}

	for {
		op, _ := p.scanIgnoreWhitespace()
		if !op.isOperator() {
			p.unscan()
			return root.RHS, nil
		} else if op == ASTERISK { //Change the asterisk to Mul token.
			op = MUL
		} else if op == LBRACKET { //LBRACKET is a special token, need to unscan
			op = SUBSET
			p.unscan()
		}

		var rhs Expr
		if rhs, err = p.parseUnaryExpr(); err != nil {
			return nil, err
		}

		for node := root; ; {
			r, ok := node.RHS.(*BinaryExpr)
			if !ok || r.OP.Precedence() >= op.Precedence() {
				node.RHS = &BinaryExpr{LHS: node.RHS, RHS: rhs, OP: op}
				break
			}
			node = r
		}
	}

	return nil, nil
}

func (p *Parser) parseUnaryExpr() (Expr, error) {
	if tok1, _ := p.scanIgnoreWhitespace(); tok1 == LPAREN {
		expr, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}
		// Expect an RPAREN at the end.
		if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 != RPAREN {
			return nil, fmt.Errorf("found %q, expected right paren.", lit2)
		}

		return &ParenExpr{Expr: expr}, nil
	} else if tok1 == LBRACKET {
		return p.parseBracketExpr()
	}

	p.unscan()

	tok, lit := p.scanIgnoreWhitespace()
	if tok == CASE {
		return p.parseCaseExpr()
	} else if tok == IDENT {
		if tok1, _ := p.scanIgnoreWhitespace(); tok1 == LPAREN {
			return p.parseCall(lit)
		}
		p.unscan() //Back the Lparen token
		p.unscan() //Back the ident token
		if n, err := p.parseFieldNameSections(); err != nil {
			return nil, err
		} else {
			if p.inmeta {
				if len(n) == 2 {
					return &MetaRef{StreamName: StreamName(n[0]), Name: n[1]}, nil
				}
				return &MetaRef{StreamName: "", Name: n[0]}, nil
			} else {
				if len(n) == 2 {
					return &FieldRef{StreamName: StreamName(n[0]), Name: n[1]}, nil
				}
				return &FieldRef{StreamName: "", Name: n[0]}, nil
			}
		}
	} else if tok == STRING {
		return &StringLiteral{Val: lit}, nil
	} else if tok == INTEGER {
		val, _ := strconv.Atoi(lit)
		return &IntegerLiteral{Val: val}, nil
	} else if tok == NUMBER {
		if v, err := strconv.ParseFloat(lit, 64); err != nil {
			return nil, fmt.Errorf("found %q, invalid number value.", lit)
		} else {
			return &NumberLiteral{Val: v}, nil
		}
	} else if tok == TRUE || tok == FALSE {
		if v, err := strconv.ParseBool(lit); err != nil {
			return nil, fmt.Errorf("found %q, invalid boolean value.", lit)
		} else {
			return &BooleanLiteral{Val: v}, nil
		}
	} else if tok.isTimeLiteral() {
		return &TimeLiteral{Val: tok}, nil
	}

	return nil, fmt.Errorf("found %q, expected expression.", lit)
}

func (p *Parser) parseBracketExpr() (Expr, error) {
	tok2, lit2 := p.scanIgnoreWhitespace()
	if tok2 == RBRACKET {
		//field[]
		return &ColonExpr{Start: 0, End: math.MinInt32}, nil
	} else if tok2 == INTEGER {
		start, err := strconv.Atoi(lit2)
		if err != nil {
			return nil, fmt.Errorf("The start index %s is not an int value in bracket expression.", lit2)
		}
		if tok3, _ := p.scanIgnoreWhitespace(); tok3 == RBRACKET {
			//Such as field[2]
			return &IndexExpr{Index: start}, nil
		} else if tok3 == COLON {
			//Such as field[2:] or field[2:4]
			return p.parseColonExpr(start)
		}
	} else if tok2 == COLON {
		//Such as field[:3] or [:]
		return p.parseColonExpr(0)
	}
	return nil, fmt.Errorf("Unexpected token %q. when parsing bracket expressions.", lit2)
}

func (p *Parser) parseColonExpr(start int) (Expr, error) {
	tok, lit := p.scanIgnoreWhitespace()
	if tok == INTEGER {
		end, err := strconv.Atoi(lit)
		if err != nil {
			return nil, fmt.Errorf("The end index %s is not an int value in bracket expression.", lit)
		}

		if tok1, lit1 := p.scanIgnoreWhitespace(); tok1 == RBRACKET {
			return &ColonExpr{Start: start, End: end}, nil
		} else {
			return nil, fmt.Errorf("Found %q, expected right bracket.", lit1)
		}
	} else if tok == RBRACKET {
		return &ColonExpr{Start: start, End: math.MinInt32}, nil
	}
	return nil, fmt.Errorf("Found %q, expected right bracket.", lit)
}

func (p *Parser) parseAs(f *Field) (*Field, error) {
	tok, lit := p.scanIgnoreWhitespace()
	if tok != IDENT {
		return nil, fmt.Errorf("found %q, expected as alias.", lit)
	}
	f.AName = lit
	return f, nil
}

func (p *Parser) parseCall(name string) (Expr, error) {
	if strings.ToLower(name) == "meta" || strings.ToLower(name) == "mqtt" {
		p.inmeta = true
		defer func() {
			p.inmeta = false
		}()
	}
	var args []Expr
	for {
		if tok, _ := p.scanIgnoreWhitespace(); tok == RPAREN {
			if valErr := validateFuncs(name, nil); valErr != nil {
				return nil, valErr
			}
			return &Call{Name: name, Args: args}, nil
		} else if tok == ASTERISK {
			if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 != RPAREN {
				return nil, fmt.Errorf("found %q, expected right paren.", lit2)
			} else {
				if p.inmeta {
					args = append(args, &MetaRef{StreamName: "", Name: "*"})
				} else {
					args = append(args, &Wildcard{Token: ASTERISK})
				}
				return &Call{Name: name, Args: args}, nil
			}
		} else {
			p.unscan()
		}

		if exp, err := p.ParseExpr(); err != nil {
			return nil, err
		} else {
			args = append(args, exp)
		}

		if tok, _ := p.scanIgnoreWhitespace(); tok != COMMA {
			p.unscan()
			break
		}
	}

	if tok, lit := p.scanIgnoreWhitespace(); tok != RPAREN {
		return nil, fmt.Errorf("found function call %q, expected ), but with %q.", name, lit)
	}
	if wt, error := validateWindows(name, args); wt == NOT_WINDOW {
		if valErr := validateFuncs(name, args); valErr != nil {
			return nil, valErr
		}
		// Add context for some aggregate func
		if name == "deduplicate" {
			args = append([]Expr{&Wildcard{Token: ASTERISK}}, args...)
		}
		return &Call{Name: name, Args: args}, nil
	} else {
		if error != nil {
			return nil, error
		}
		win, err := p.ConvertToWindows(wt, args)
		if err != nil {
			return nil, error
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

func (p *Parser) parseCaseExpr() (*CaseExpr, error) {
	c := &CaseExpr{}
	tok, _ := p.scanIgnoreWhitespace()
	p.unscan()
	if tok != WHEN { // no condition value for case, additional validation needed
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
		case WHEN:
			if exp, err := p.ParseExpr(); err != nil {
				return nil, err
			} else {
				if c.WhenClauses == nil {
					c.WhenClauses = make([]*WhenClause, 0)
				}
				if c.Value == nil && !isBooleanArg(exp) {
					return nil, fmt.Errorf("invalid CASE expression, WHEN expression must be a bool condition")
				}
				w := &WhenClause{
					Expr: exp,
				}
				tokThen, _ := p.scanIgnoreWhitespace()
				if tokThen != THEN {
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
		case ELSE:
			if c.WhenClauses != nil {
				if exp, err := p.ParseExpr(); err != nil {
					return nil, err
				} else {
					c.ElseClause = exp
				}
			} else {
				return nil, fmt.Errorf("invalid CASE expression, WHEN expected before ELSE")
			}
		case END:
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

func validateWindows(name string, args []Expr) (WindowType, error) {
	fname := strings.ToLower(name)
	switch fname {
	case "tumblingwindow":
		if err := validateWindow(fname, 2, args); err != nil {
			return TUMBLING_WINDOW, err
		}
		return TUMBLING_WINDOW, nil
	case "hoppingwindow":
		if err := validateWindow(fname, 3, args); err != nil {
			return HOPPING_WINDOW, err
		}
		return HOPPING_WINDOW, nil
	case "sessionwindow":
		if err := validateWindow(fname, 3, args); err != nil {
			return SESSION_WINDOW, err
		}
		return SESSION_WINDOW, nil
	case "slidingwindow":
		if err := validateWindow(fname, 2, args); err != nil {
			return SLIDING_WINDOW, err
		}
		return SLIDING_WINDOW, nil
	case "countwindow":
		if len(args) == 1 {
			if para1, ok := args[0].(*IntegerLiteral); ok && para1.Val > 0 {
				return COUNT_WINDOW, nil
			} else {
				return COUNT_WINDOW, fmt.Errorf("Invalid parameter value %s.", args[0])
			}
		} else if len(args) == 2 {
			if para1, ok1 := args[0].(*IntegerLiteral); ok1 {
				if para2, ok2 := args[1].(*IntegerLiteral); ok2 {
					if para1.Val < para2.Val {
						return COUNT_WINDOW, fmt.Errorf("The second parameter value %d should be less than the first parameter %d.", para2.Val, para1.Val)
					} else {
						return COUNT_WINDOW, nil
					}
				}
			}
			return COUNT_WINDOW, fmt.Errorf("Invalid parameter value %s, %s.", args[0], args[1])
		} else {
			return COUNT_WINDOW, fmt.Errorf("Invalid parameter count.")
		}

	}
	return NOT_WINDOW, nil
}

func validateWindow(funcName string, expectLen int, args []Expr) error {
	if len(args) != expectLen {
		return fmt.Errorf("The arguments for %s should be %d.\n", funcName, expectLen)
	}
	if _, ok := args[0].(*TimeLiteral); !ok {
		return fmt.Errorf("The 1st argument for %s is expecting timer literal expression. One value of [dd|hh|mi|ss|ms].\n", funcName)
	}

	for i := 1; i < len(args); i++ {
		if _, ok := args[i].(*IntegerLiteral); !ok {
			return fmt.Errorf("The %d argument for %s is expecting interger literal expression. \n", i, funcName)
		}
	}
	return nil

}

func (p *Parser) ConvertToWindows(wtype WindowType, args []Expr) (*Window, error) {
	win := &Window{WindowType: wtype}
	if wtype == COUNT_WINDOW {
		win.Length = &IntegerLiteral{Val: args[0].(*IntegerLiteral).Val}
		if len(args) == 2 {
			win.Interval = &IntegerLiteral{Val: args[1].(*IntegerLiteral).Val}
		}
		return win, nil
	}
	var unit = 1
	v := args[0].(*TimeLiteral).Val
	switch v {
	case DD:
		unit = 24 * 3600 * 1000
	case HH:
		unit = 3600 * 1000
	case MI:
		unit = 60 * 1000
	case SS:
		unit = 1000
	case MS:
		unit = 1
	default:
		return nil, fmt.Errorf("Invalid timeliteral %s", v)
	}
	win.Length = &IntegerLiteral{Val: args[1].(*IntegerLiteral).Val * unit}
	if len(args) > 2 {
		win.Interval = &IntegerLiteral{Val: args[2].(*IntegerLiteral).Val * unit}
	} else {
		win.Interval = &IntegerLiteral{Val: 0}
	}
	return win, nil
}

func (p *Parser) ParseCreateStmt() (Statement, error) {
	if tok, _ := p.scanIgnoreWhitespace(); tok == CREATE {
		tok1, lit1 := p.scanIgnoreWhitespace()
		stmt := &StreamStmt{}
		switch tok1 {
		case STREAM:
			stmt.StreamType = TypeStream
		case TABLE:
			stmt.StreamType = TypeTable
		default:
			return nil, fmt.Errorf("found %q, expected keyword stream or table.", lit1)
		}
		if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == IDENT {
			stmt.Name = StreamName(lit2)
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
			if tok3, lit3 := p.scanIgnoreWhitespace(); tok3 == SEMICOLON {
				p.unscan()
			} else if tok3 == EOF {
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
func validateStream(stmt *StreamStmt) error {
	f, ok := stmt.Options["FORMAT"]
	if !ok {
		f = common.FORMAT_JSON
	}
	switch strings.ToLower(f) {
	case common.FORMAT_JSON:
		//do nothing
	case common.FORMAT_BINARY:
		if stmt.StreamType == TypeTable {
			return fmt.Errorf("'binary' format is not supported for table")
		}
		switch len(stmt.StreamFields) {
		case 0:
			// do nothing for schemaless
		case 1:
			f := stmt.StreamFields[0]
			if bt, ok := f.FieldType.(*BasicType); ok {
				if bt.Type == BYTEA {
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

	if stmt.StreamType == TypeTable {
		if t, ok := stmt.Options["TYPE"]; ok {
			if strings.ToLower(t) != "file" {
				return fmt.Errorf("table only supports 'file' type")
			}
		}
	}
	return nil
}

func (p *Parser) parseShowStmt() (Statement, error) {
	if tok, _ := p.scanIgnoreWhitespace(); tok == SHOW {
		tok1, lit1 := p.scanIgnoreWhitespace()
		switch tok1 {
		case STREAMS:
			ss := &ShowStreamsStatement{}
			if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == EOF || tok2 == SEMICOLON {
				return ss, nil
			} else {
				return nil, fmt.Errorf("found %q, expected semecolon or EOF.", lit2)
			}
		case TABLES:
			ss := &ShowTablesStatement{}
			if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == EOF || tok2 == SEMICOLON {
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

func (p *Parser) parseDescribeStmt() (Statement, error) {
	if tok, _ := p.scanIgnoreWhitespace(); tok == DESCRIBE {
		tok1, lit1 := p.scanIgnoreWhitespace()
		switch tok1 {
		case STREAM:
			dss := &DescribeStreamStatement{}
			if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == IDENT {
				dss.Name = lit2
				return dss, nil
			} else {
				return nil, fmt.Errorf("found %q, expected stream name.", lit2)
			}
		case TABLE:
			dss := &DescribeTableStatement{}
			if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == IDENT {
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

func (p *Parser) parseExplainStmt() (Statement, error) {
	if tok, _ := p.scanIgnoreWhitespace(); tok == EXPLAIN {
		tok1, lit1 := p.scanIgnoreWhitespace()
		switch tok1 {
		case STREAM:
			ess := &ExplainStreamStatement{}
			if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == IDENT {
				ess.Name = lit2
				return ess, nil
			} else {
				return nil, fmt.Errorf("found %q, expected stream name.", lit2)
			}
		case TABLE:
			ess := &ExplainTableStatement{}
			if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == IDENT {
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

func (p *Parser) parseDropStmt() (Statement, error) {
	if tok, _ := p.scanIgnoreWhitespace(); tok == DROP {
		tok1, lit1 := p.scanIgnoreWhitespace()
		switch tok1 {
		case STREAM:
			ess := &DropStreamStatement{}
			if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == IDENT {
				ess.Name = lit2
				return ess, nil
			} else {
				return nil, fmt.Errorf("found %q, expected stream name.", lit2)
			}
		case TABLE:
			ess := &DropTableStatement{}
			if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == IDENT {
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

func (p *Parser) parseStreamFields() (StreamFields, error) {
	lStack := &stack.Stack{}
	var fields StreamFields
	if tok, lit := p.scanIgnoreWhitespace(); tok == LPAREN {
		lStack.Push(lit)
		for {
			//For the schemaless streams
			//create stream demo () WITH (FORMAT="JSON", DATASOURCE="demo" TYPE="edgex")
			if tok1, _ := p.scanIgnoreWhitespace(); tok1 == RPAREN {
				lStack.Pop()
				if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 != WITH {
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

			if tok1, _ := p.scanIgnoreWhitespace(); tok1 == RPAREN {
				lStack.Pop()
				if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == WITH {
					//Check the stack for LPAREN; If the stack for LPAREN is not zero, then it's not correct.
					if lStack.Len() > 0 {
						return nil, fmt.Errorf("Parenthesis is not matched.")
					}
					break
				} else if tok2 == COMMA {
					if lStack.Len() > 0 {
						return nil, fmt.Errorf("Parenthesis is in create record type not matched.")
					}
					p.unscan()
					break
				} else if tok2 == RPAREN { //The nested type definition of ARRAY and Struct, such as "field ARRAY(STRUCT(f BIGINT))"
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

func (p *Parser) parseStreamField() (*StreamField, error) {
	field := &StreamField{}
	if tok, lit := p.scanIgnoreWhitespace(); tok == IDENT {
		field.Name = lit
		tok1, lit1 := p.scanIgnoreWhitespace()
		if t := getDataType(tok1); t != UNKNOWN && t.isSimpleType() {
			field.FieldType = &BasicType{Type: t}
		} else if t == ARRAY {
			if f, e := p.parseStreamArrayType(); e != nil {
				return nil, e
			} else {
				field.FieldType = f
			}
		} else if t == STRUCT {
			if f, e := p.parseStreamStructType(); e != nil {
				return nil, e
			} else {
				field.FieldType = f
			}
		} else if t == UNKNOWN {
			return nil, fmt.Errorf("found %q, expect valid stream field types(BIGINT | FLOAT | STRINGS | DATETIME | BOOLEAN | BYTEA | ARRAY | STRUCT).", lit1)
		}

		if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == COMMA {
			//Just consume the comma.
		} else if tok2 == RPAREN {
			p.unscan()
		} else {
			return nil, fmt.Errorf("found %q, expect comma or rparen.", lit2)
		}
	} else {
		return nil, fmt.Errorf("found %q, expect stream field name.", lit)
	}
	return field, nil
}

func (p *Parser) parseStreamArrayType() (FieldType, error) {
	lStack := &stack.Stack{}
	if tok, _ := p.scanIgnoreWhitespace(); tok == LPAREN {
		lStack.Push(LPAREN)
		tok1, lit1 := p.scanIgnoreWhitespace()
		if t := getDataType(tok1); t != UNKNOWN && t.isSimpleType() {
			if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == RPAREN {
				lStack.Pop()
				if lStack.Len() > 0 {
					return nil, fmt.Errorf("Parenthesis is in array type not matched.")
				}
				return &ArrayType{Type: t}, nil
			} else {
				return nil, fmt.Errorf("found %q, expect rparen in array type definition.", lit2)
			}
		} else if tok1 == XSTRUCT {
			if f, err := p.parseStreamStructType(); err != nil {
				return nil, err
			} else {
				if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == RPAREN {
					lStack.Pop()
					if lStack.Len() > 0 {
						return nil, fmt.Errorf("Parenthesis is in struct of array type %q not matched.", tok1)
					}
					return &ArrayType{Type: STRUCT, FieldType: f}, nil
				} else {
					return nil, fmt.Errorf("found %q, expect rparen in struct of array type definition.", lit2)
				}
			}
		} else if tok1 == COMMA {
			p.unscan()
		} else {
			return nil, fmt.Errorf("found %q, expect stream data types.", lit1)
		}
	} else {

	}
	return nil, nil
}

func (p *Parser) parseStreamStructType() (FieldType, error) {
	rf := &RecType{}
	if sfs, err := p.parseStreamFields(); err != nil {
		return nil, err
	} else {
		if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == COMMA {
			rf.StreamFields = sfs
			p.unscan()
		} else if tok2 == RPAREN {
			rf.StreamFields = sfs
			p.unscan()
		} else {
			return nil, fmt.Errorf("found %q, expect comma in create stream record statement.", lit2)
		}
	}
	return rf, nil
}

func (p *Parser) parseStreamOptions() (map[string]string, error) {
	var opts = make(map[string]string)
	lStack := &stack.Stack{}
	if tok, lit := p.scanIgnoreWhitespace(); tok == LPAREN {
		lStack.Push(LPAREN)
		for {
			if tok1, lit1 := p.scanIgnoreWhitespace(); tok1 == DATASOURCE || tok1 == FORMAT || tok1 == KEY || tok1 == CONF_KEY || tok1 == STRICT_VALIDATION || tok1 == TYPE || tok1 == TIMESTAMP || tok1 == TIMESTAMP_FORMAT {
				if tok2, lit2 := p.scanIgnoreWhitespace(); tok2 == EQ {
					if tok3, lit3 := p.scanIgnoreWhitespace(); tok3 == STRING {
						if tok1 == STRICT_VALIDATION {
							if val := strings.ToUpper(lit3); (val != "TRUE") && (val != "FALSE") {
								return nil, fmt.Errorf("found %q, expect TRUE/FALSE value in %s option.", lit3, tok1)
							}
						}
						opts[lit1] = lit3
					} else {
						return nil, fmt.Errorf("found %q, expect string value in option.", lit3)
					}
				} else {
					return nil, fmt.Errorf("found %q, expect equals(=) in options.", lit2)
				}

			} else if tok1 == COMMA {
				continue
			} else if tok1 == RPAREN {
				if lStack.Pop(); lStack.Len() == 0 {
					break
				} else {
					return nil, fmt.Errorf("Parenthesis is not matched in options definition.")
				}
			} else {
				return nil, fmt.Errorf("found %q, unknown option keys(DATASOURCE|FORMAT|KEY|CONF_KEY|STRICT_VALIDATION|TYPE).", lit1)
			}
		}
	} else {
		return nil, fmt.Errorf("found %q, expect stream options.", lit)
	}
	return opts, nil
}

// Only support filter on window now
func (p *Parser) parseFilter() (Expr, error) {
	if tok, _ := p.scanIgnoreWhitespace(); tok != FILTER {
		p.unscan()
		return nil, nil
	}
	if tok, lit := p.scanIgnoreWhitespace(); tok != LPAREN {
		return nil, fmt.Errorf("Found %q after FILTER, expect parentheses.", lit)
	}
	if tok, lit := p.scanIgnoreWhitespace(); tok != WHERE {
		return nil, fmt.Errorf("Found %q after FILTER(, expect WHERE.", lit)
	}
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}
	if tok, lit := p.scanIgnoreWhitespace(); tok != RPAREN {
		return nil, fmt.Errorf("Found %q after FILTER, expect right parentheses.", lit)
	}
	return expr, nil
}
