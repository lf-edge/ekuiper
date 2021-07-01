package xsql

import (
	"fmt"
	"github.com/emqx/kuiper/pkg/ast"
)

var (
	Language          = &ParseTree{}
	FuncRegisters     []FunctionRegister
	parserFuncRuntime *funcRuntime
)

type ParseTree struct {
	Handlers map[ast.Token]func(*Parser) (ast.Statement, error)
	Tokens   map[ast.Token]*ParseTree
	Keys     []string
}

func (t *ParseTree) Handle(tok ast.Token, fn func(*Parser) (ast.Statement, error)) {
	// Verify that there is no conflict for this token in this parse tree.
	if _, conflict := t.Tokens[tok]; conflict {
		panic(fmt.Sprintf("conflict for token %s", tok))
	}

	if _, conflict := t.Handlers[tok]; conflict {
		panic(fmt.Sprintf("conflict for token %s", tok))
	}

	if t.Handlers == nil {
		t.Handlers = make(map[ast.Token]func(*Parser) (ast.Statement, error))
	}
	t.Handlers[tok] = fn
	t.Keys = append(t.Keys, tok.String())
}

func (pt *ParseTree) Parse(p *Parser) (ast.Statement, error) {
	tok, _ := p.scanIgnoreWhitespace()
	p.unscan()
	if f, ok := pt.Handlers[tok]; ok {
		return f(p)
	}
	return nil, nil
}

func init() {
	Language.Handle(ast.SELECT, func(p *Parser) (ast.Statement, error) {
		return p.Parse()
	})

	Language.Handle(ast.CREATE, func(p *Parser) (statement ast.Statement, e error) {
		return p.ParseCreateStmt()
	})

	Language.Handle(ast.SHOW, func(p *Parser) (statement ast.Statement, e error) {
		return p.parseShowStmt()
	})

	Language.Handle(ast.EXPLAIN, func(p *Parser) (statement ast.Statement, e error) {
		return p.parseExplainStmt()
	})

	Language.Handle(ast.DESCRIBE, func(p *Parser) (statement ast.Statement, e error) {
		return p.parseDescribeStmt()
	})

	Language.Handle(ast.DROP, func(p *Parser) (statement ast.Statement, e error) {
		return p.parseDropStmt()
	})

	InitFuncRegisters()
}

func InitFuncRegisters(registers ...FunctionRegister) {
	FuncRegisters = registers
	parserFuncRuntime = NewFuncRuntime(nil, registers)
	ast.InitFuncFinder(parserFuncRuntime)
}
