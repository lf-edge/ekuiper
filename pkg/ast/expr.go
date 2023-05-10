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
	"regexp"
	"strings"
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

func (ae *ArrowExpr) expr() {}
func (ae *ArrowExpr) node() {}

func (be *BracketExpr) expr() {}
func (be *BracketExpr) node() {}

func (be *ColonExpr) expr() {}
func (be *ColonExpr) node() {}

func (be *IndexExpr) expr() {}
func (be *IndexExpr) node() {}

func (w *Wildcard) expr() {}
func (w *Wildcard) node() {}

func (bl *BooleanLiteral) expr()    {}
func (bl *BooleanLiteral) literal() {}
func (bl *BooleanLiteral) node()    {}

func (tl *TimeLiteral) expr()    {}
func (tl *TimeLiteral) literal() {}
func (tl *TimeLiteral) node()    {}

func (il *IntegerLiteral) expr()    {}
func (il *IntegerLiteral) literal() {}
func (il *IntegerLiteral) node()    {}

func (nl *NumberLiteral) expr()    {}
func (nl *NumberLiteral) literal() {}
func (nl *NumberLiteral) node()    {}

func (sl *StringLiteral) expr()    {}
func (sl *StringLiteral) literal() {}
func (sl *StringLiteral) node()    {}

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

type PartitionExpr struct {
	Exprs []Expr
}

func (pe *PartitionExpr) expr() {}
func (pe *PartitionExpr) node() {}

type BinaryExpr struct {
	OP  Token
	LHS Expr
	RHS Expr
}

func (be *BinaryExpr) expr() {}
func (be *BinaryExpr) node() {}

type WhenClause struct {
	// The condition Expression
	Expr   Expr
	Result Expr
}

func (w *WhenClause) expr() {}
func (w *WhenClause) node() {}

type CaseExpr struct {
	// The compare value Expression. It can be a value Expression or nil.
	// When it is nil, the WhenClause Expr must be a logical(comparison) Expression
	Value       Expr
	WhenClauses []*WhenClause
	ElseClause  Expr
}

func (c *CaseExpr) expr() {}
func (c *CaseExpr) node() {}

type ValueSetExpr struct {
	LiteralExprs []Expr // ("A", "B", "C") or (1, 2, 3)
	ArrayExpr    Expr
}

func (c *ValueSetExpr) expr() {}
func (c *ValueSetExpr) node() {}

type BetweenExpr struct {
	Lower  Expr
	Higher Expr
}

func (b *BetweenExpr) expr() {}
func (b *BetweenExpr) node() {}

type LikePattern struct {
	Expr    Expr
	Pattern *regexp.Regexp
}

func (l *LikePattern) expr() {}
func (l *LikePattern) node() {}

func (l *LikePattern) Compile(likestr string) (*regexp.Regexp, error) {
	likestr = strings.ReplaceAll(strings.ReplaceAll(likestr, `\%`, `!@#`), `\_`, `!@$`)
	regstr := strings.ReplaceAll(strings.ReplaceAll(likestr, "%", ".*"), "_", ".")
	regstr = strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(regstr, `!@$`, `\_`), `!@#`, `\%`), `\`, `\\`)
	re, err := regexp.Compile("^" + regstr + "$")
	if err != nil {
		return nil, err
	}
	return re, nil
}

type StreamName string

func (sn *StreamName) node() {}

const (
	DefaultStream = StreamName("$$default")
	AliasStream   = StreamName("$$alias")
)

// FieldRef could be
//  1. SQL Field
//     1.1 Explicit field "stream.col"
//     1.2 Implicit field "col"  -> only exist in schemaless stream. Otherwise, explicit stream name will be bound
//     1.3 Alias field "expr as c" -> refer to an Expression or column
type FieldRef struct {
	// optional, bind in analyzer, empty means alias, default means not set
	// MUST have after binding for SQL fields. For 1.2,1.3 and 1.4, use special constant as stream name
	StreamName StreamName
	// optional, set only once. For selections, empty name will be assigned a default name
	// MUST have after binding, assign a name for 1.4
	Name string
	// Only for alias
	*AliasRef
}

func (fr *FieldRef) expr() {}
func (fr *FieldRef) node() {}
func (fr *FieldRef) IsColumn() bool {
	return fr.StreamName != AliasStream && fr.StreamName != ""
}

func (fr *FieldRef) IsAlias() bool {
	return fr.StreamName == AliasStream
}

func (fr *FieldRef) RefSelection(a *AliasRef) {
	fr.AliasRef = a
}

// RefSources Must call after binding or will get empty
func (fr *FieldRef) RefSources() []StreamName {
	if fr.StreamName == AliasStream {
		return fr.refSources
	} else if fr.StreamName != "" {
		return []StreamName{fr.StreamName}
	} else {
		return nil
	}
}

// SetRefSource Only call this for alias field ref
func (fr *FieldRef) SetRefSource(names []StreamName) {
	fr.refSources = names
}

type AliasRef struct {
	// MUST have, It is used for evaluation
	Expression Expr
	// MUST have after binding, calculate once in initializer. Could be 0 when alias an Expression without col like "1+2"
	refSources []StreamName
	// optional, lazy set when calculating isAggregate
	IsAggregate *bool
}

func NewAliasRef(e Expr) (*AliasRef, error) {
	r := make(map[StreamName]bool)
	var walkErr error
	WalkFunc(e, func(n Node) bool {
		switch f := n.(type) {
		case *FieldRef:
			switch f.StreamName {
			case AliasStream:
				walkErr = fmt.Errorf("cannot use alias %s inside another alias %v", f.Name, e)
				return false
			default:
				r[f.StreamName] = true
			}
		}
		return true
	})
	if walkErr != nil {
		return nil, walkErr
	}
	rs := make([]StreamName, 0)
	for k := range r {
		rs = append(rs, k)
	}
	return &AliasRef{
		Expression: e,
		refSources: rs,
	}, nil
}

// for testing only
func MockAliasRef(e Expr, r []StreamName, a *bool) *AliasRef {
	return &AliasRef{e, r, a}
}

type MetaRef struct {
	StreamName StreamName
	Name       string
}

func (fr *MetaRef) expr() {}
func (fr *MetaRef) node() {}

type JsonFieldRef struct {
	Name string
}

func (fr *JsonFieldRef) expr() {}
func (fr *JsonFieldRef) node() {}

type ColFuncField struct {
	Name string
	Expr Expr
}

func (fr *ColFuncField) expr() {}
func (fr *ColFuncField) node() {}
