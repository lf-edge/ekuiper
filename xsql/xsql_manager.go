package xsql

import "fmt"

var Language = &ParseTree{}

type ParseTree struct {
	Handlers map[Token]func(*Parser) (Statement, error)
	Tokens   map[Token]*ParseTree
	Keys     []string
}

func (t *ParseTree) Handle(tok Token, fn func(*Parser) (Statement, error)) {
	// Verify that there is no conflict for this token in this parse tree.
	if _, conflict := t.Tokens[tok]; conflict {
		panic(fmt.Sprintf("conflict for token %s", tok))
	}

	if _, conflict := t.Handlers[tok]; conflict {
		panic(fmt.Sprintf("conflict for token %s", tok))
	}

	if t.Handlers == nil {
		t.Handlers = make(map[Token]func(*Parser) (Statement, error))
	}
	t.Handlers[tok] = fn
	t.Keys = append(t.Keys, tok.String())
}

func (pt *ParseTree) Parse(p *Parser) (Statement, error) {
	tok, _ := p.scanIgnoreWhitespace()
	p.unscan()
	if f, ok := pt.Handlers[tok]; ok {
		return f(p)
	}
	return nil, nil
}

func init() {
	Language.Handle(SELECT, func(p *Parser) (Statement, error) {
		return p.Parse()
	})

	Language.Handle(CREATE, func(p *Parser) (statement Statement, e error) {
		return p.ParseCreateStmt()
	})

	Language.Handle(SHOW, func(p *Parser) (statement Statement, e error) {
		return p.parseShowStmt()
	})

	Language.Handle(EXPLAIN, func(p *Parser) (statement Statement, e error) {
		return p.parseExplainStmt()
	})

	Language.Handle(DESCRIBE, func(p *Parser) (statement Statement, e error) {
		return p.parseDescribeStmt()
	})

	Language.Handle(DROP, func(p *Parser) (statement Statement, e error) {
		return p.parseDropStmt()
	})
}
