// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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

import (
	"fmt"
	"strconv"
)

type Node interface {
	node()
}

type NameNode interface {
	Node
	GetName() string
}

type Expr interface {
	Node
	expr()
	String() string
}

type Literal interface {
	Expr
	literal()
}

type ParenExpr struct {
	Expr Expr
}

type ArrowExpr struct {
	Expr Expr
}

type BracketExpr struct {
	Expr Expr
}

type ColonExpr struct {
	Start Expr
	End   Expr
}

type IndexExpr struct {
	Index Expr
}

type BooleanLiteral struct {
	Val bool
}

type TimeLiteral struct {
	Val Token
}

type IntegerLiteral struct {
	Val int
}

type StringLiteral struct {
	Val string
}

type NumberLiteral struct {
	Val float64
}

type Wildcard struct {
	Token Token
}

func (pe *ParenExpr) expr() {}
func (pe *ParenExpr) node() {}
func (pe *ParenExpr) String() string {
	e := ""
	if pe.Expr != nil {
		e += pe.Expr.String()
	}
	return "parenExpr:{ " + e + " }"
}

func (ae *ArrowExpr) expr() {}
func (ae *ArrowExpr) node() {}
func (ae *ArrowExpr) String() string {
	e := ""
	if ae.Expr != nil {
		e += ae.Expr.String()
	}
	return "arrowExpr:{ " + e + " }"
}

func (be *BracketExpr) expr() {}
func (be *BracketExpr) node() {}
func (be *BracketExpr) String() string {
	e := ""
	if be.Expr != nil {
		e += be.Expr.String()
	}
	return "bracketExpr:{ " + e + " }"
}

func (be *ColonExpr) expr() {}
func (be *ColonExpr) node() {}
func (be *ColonExpr) String() string {
	s := ""
	e := ""
	if be.Start != nil {
		s += "start:{ " + be.Start.String() + " }"
	}
	if be.End != nil {
		if be.Start != nil {
			e += ", "
		}
		e += "end:{ " + be.End.String() + " }"
	}
	return "ColonExpr:{ " + s + e + " }"
}

func (be *IndexExpr) expr() {}
func (be *IndexExpr) node() {}
func (be *IndexExpr) String() string {
	i := ""
	if be.Index != nil {
		i += be.Index.String()
	}
	return "IndexExpr:{ " + i + " }"
}

func (w *Wildcard) expr() {}
func (w *Wildcard) node() {}
func (w *Wildcard) String() string {
	return "wildCard:" + Tokens[w.Token]
}

func (bl *BooleanLiteral) expr()    {}
func (bl *BooleanLiteral) literal() {}
func (bl *BooleanLiteral) node()    {}
func (bl *BooleanLiteral) String() string {
	return "booleanLiteral:" + strconv.FormatBool(bl.Val)
}

func (tl *TimeLiteral) expr()    {}
func (tl *TimeLiteral) literal() {}
func (tl *TimeLiteral) node()    {}
func (tl *TimeLiteral) String() string {
	return "timeLiteral:" + Tokens[tl.Val]
}

func (il *IntegerLiteral) expr()    {}
func (il *IntegerLiteral) literal() {}
func (il *IntegerLiteral) node()    {}
func (il *IntegerLiteral) String() string {
	return "integerLiteral:" + strconv.Itoa(il.Val)
}

func (nl *NumberLiteral) expr()    {}
func (nl *NumberLiteral) literal() {}
func (nl *NumberLiteral) node()    {}
func (nl *NumberLiteral) String() string {
	return "numberLiteral:" + fmt.Sprintf("%f", nl.Val)
}

func (sl *StringLiteral) expr()    {}
func (sl *StringLiteral) literal() {}
func (sl *StringLiteral) node()    {}
func (sl *StringLiteral) String() string {
	return "stringLiteral:" + sl.Val
}

type FuncType int

const (
	FuncTypeUnknown FuncType = iota - 1
	FuncTypeScalar
	FuncTypeAgg
	FuncTypeCols
	FuncTypeSrf
)

type Call struct {
	Name     string
	FuncId   int
	FuncType FuncType
	Args     []Expr
	// This is used for analytic functions.
	// In planner, all analytic functions are planned to calculate in analytic_op which produce a new field.
	// This cachedField cached the new field name and when evaluating, just return the field access evaluated value.
	CachedField string
	Cached      bool
	Partition   *PartitionExpr
	WhenExpr    Expr
}

func (c *Call) expr()    {}
func (c *Call) literal() {}
func (c *Call) node()    {}
func (c *Call) String() string {
	args := ""
	if c.Args != nil {
		args = ", args:[ "
		for i, arg := range c.Args {
			args += arg.String()
			if i != len(c.Args)-1 {
				args += ", "
			}
		}
		args += " ]"
	}
	when := ""
	if c.WhenExpr != nil {
		when += ", when:{ " + c.WhenExpr.String() + " }"
	}
	return "callExpr:{ name:" + c.Name + args + when + " }"
}

type PartitionExpr struct {
	Exprs []Expr
}

func (pe *PartitionExpr) expr() {}
func (pe *PartitionExpr) node() {}
func (pe *PartitionExpr) String() string {
	e := ""
	for i, expr := range pe.Exprs {
		e += expr.String()
		if i != len(pe.Exprs)-1 {
			e += ", "
		}
	}
	return "partitionExpr:[ " + e + " ]"
}

type BinaryExpr struct {
	OP  Token
	LHS Expr
	RHS Expr
}

func (be *BinaryExpr) expr() {}
func (be *BinaryExpr) node() {}
func (be *BinaryExpr) String() string {
	l := ""
	r := ""
	if be.LHS != nil {
		l += ", left:{ " + be.LHS.String() + " }"
	}
	if be.RHS != nil {
		if be.LHS != nil {
			r += ", "
		}
		r += "right:{ " + be.RHS.String() + " }"
	}
	return "binaryExpr:{ option:" + Tokens[be.OP] + l + r + " }"
}

type WhenClause struct {
	// The condition Expression
	Expr   Expr
	Result Expr
}

func (w *WhenClause) expr() {}
func (w *WhenClause) node() {}
func (w *WhenClause) String() string {
	e := ""
	if w.Expr != nil {
		e += w.Expr.String()
	}
	return "whenClause : { " + e + " }"
}

type CaseExpr struct {
	// The compare value Expression. It can be a value Expression or nil.
	// When it is nil, the WhenClause Expr must be a logical(comparison) Expression
	Value       Expr
	WhenClauses []*WhenClause
	ElseClause  Expr
}

func (c *CaseExpr) expr() {}
func (c *CaseExpr) node() {}
func (c *CaseExpr) String() string {
	v := ""
	if c.Value != nil {
		v += "value:{ " + c.Value.String() + " }"
	}
	w := ""
	if c.WhenClauses != nil && len(c.WhenClauses) != 0 {
		if c.Value != nil {
			w += ", "
		}
		w += "whenClauses:[ "
		for i, clause := range c.WhenClauses {
			if clause.Expr != nil {
				w += "{ " + clause.String() + " }"
				if i != len(c.WhenClauses)-1 {
					w += ", "
				}
			}
		}
		w += " ]"
	}
	return "caseExprValue:{ " + v + w + " }"
}

type ValueSetExpr struct {
	LiteralExprs []Expr // ("A", "B", "C") or (1, 2, 3)
	ArrayExpr    Expr
}

func (c *ValueSetExpr) expr() {}
func (c *ValueSetExpr) node() {}
func (c *ValueSetExpr) String() string {
	le := ""
	if c.LiteralExprs != nil && len(c.LiteralExprs) != 0 {
		le += "literalExprs:[ "
		for i, expr := range c.LiteralExprs {
			le += expr.String()
			if i != len(c.LiteralExprs)-1 {
				le += ", "
			}
		}
		le += " ]"
	}
	a := ""
	if c.ArrayExpr != nil {
		if c.LiteralExprs != nil && len(c.LiteralExprs) != 0 {
			a += ", "
		}
		a += "arrayExpr:{" + c.ArrayExpr.String() + "}"
	}
	return "valueSetExpr:{ " + le + a + " }"
}

type BetweenExpr struct {
	Lower  Expr
	Higher Expr
}

func (b *BetweenExpr) expr() {}
func (b *BetweenExpr) node() {}
func (b *BetweenExpr) String() string {
	low := ""
	high := ""
	if b.Lower != nil {
		low += "lower:{ " + b.Lower.String() + " }"
	}
	if b.Higher != nil {
		if b.Lower != nil {
			high += ", "
		}
		high += "higher:{ " + b.Higher.String() + " }"
	}
	return "betweenExpr:{ " + low + high + " }"
}

type StreamName string

func (sn *StreamName) node() {}

const (
	DefaultStream = StreamName("$$default")
	AliasStream   = StreamName("$$alias")
)

type MetaRef struct {
	StreamName StreamName
	Name       string
}

func (fr *MetaRef) expr() {}
func (fr *MetaRef) node() {}
func (fr *MetaRef) String() string {
	sn := ""
	n := ""
	if fr.StreamName != "" {
		sn += "streamName:" + string(fr.StreamName)
	}
	if fr.Name != "" {
		if fr.StreamName != "" {
			n += ", "
		}
		n += "fieldName:" + fr.Name
	}
	return "metaRef:{ " + sn + n + " }"
}

type JsonFieldRef struct {
	Name string
}

func (fr *JsonFieldRef) expr() {}
func (fr *JsonFieldRef) node() {}
func (fr *JsonFieldRef) String() string {
	return "jsonFieldName:" + fr.Name
}

type ColFuncField struct {
	Name string
	Expr Expr
}

func (fr *ColFuncField) expr() {}
func (fr *ColFuncField) node() {}
func (fr *ColFuncField) String() string {
	e := ""
	if fr.Expr != nil {
		e += ", expr:{ " + fr.Expr.String() + " }"
	}
	return "colFuncField:{ name: " + fr.Name + e + " }"
}
