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
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"strings"
)

var (
	Language          = &ParseTree{}
	parserFuncRuntime *funcRuntime
)

type ParseTree struct {
	Handlers map[string]func(*Parser) (ast.Statement, error)
	Tokens   map[string]*ParseTree
	Keys     []string
}

func (pt *ParseTree) Handle(lit string, fn func(*Parser) (ast.Statement, error)) {
	// Verify that there is no conflict for this token in this parse tree.
	if _, conflict := pt.Tokens[lit]; conflict {
		panic(fmt.Sprintf("conflict for token %s", lit))
	}

	if _, conflict := pt.Handlers[lit]; conflict {
		panic(fmt.Sprintf("conflict for token %s", lit))
	}

	if pt.Handlers == nil {
		pt.Handlers = make(map[string]func(*Parser) (ast.Statement, error))
	}
	pt.Handlers[lit] = fn
	pt.Keys = append(pt.Keys, lit)
}

func (pt *ParseTree) Parse(p *Parser) (ast.Statement, error) {
	_, lit := p.scanIgnoreWhitespace()
	lit = strings.ToUpper(lit)
	p.unscan()
	if f, ok := pt.Handlers[lit]; ok {
		return f(p)
	}
	return nil, nil
}

func init() {
	Language.Handle(ast.SELECT_LIT, func(p *Parser) (ast.Statement, error) {
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
}
