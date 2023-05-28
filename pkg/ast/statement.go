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

package ast

import "strconv"

type Statement interface {
	stmt()
	Node
}

type SelectStatement struct {
	Fields     Fields
	Sources    Sources
	Joins      Joins
	Condition  Expr
	Dimensions Dimensions
	Having     Expr
	SortFields SortFields

	Statement
}

type Fields []Field

func (f Fields) node() {}

func (f Fields) Len() int {
	return len(f)
}

func (f Fields) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

func (f Fields) Less(i int, j int) bool {
	m := f[i].AName
	if m == "" {
		m = f[i].Name
	}
	n := f[j].AName
	if n == "" {
		n = f[j].Name
	}
	return m < n
}

type Field struct {
	Name  string
	AName string
	Expr  Expr

	Node
}

func (f *Field) GetName() string {
	if f.AName != "" {
		return f.AName
	} else {
		return f.Name
	}
}

func (f *Field) IsSelectionField() bool {
	if f.AName != "" {
		return true
	}
	_, ok := f.Expr.(*FieldRef)
	if ok {
		return true
	}
	return false
}

func (f *Field) IsColumn() bool {
	if f.AName != "" {
		return false
	}
	_, ok := f.Expr.(*FieldRef)
	if ok {
		return true
	}
	return false
}

type Sources []Source

func (s Sources) node() {}

type Source interface {
	Node
	source()
}

type Table struct {
	Name  string
	Alias string
	Source
}

type JoinType int

const (
	LEFT_JOIN JoinType = iota
	INNER_JOIN
	RIGHT_JOIN
	FULL_JOIN
	CROSS_JOIN
)

func (j JoinType) String() string {
	switch j {
	case LEFT_JOIN:
		return "LEFT_JOIN"
	case INNER_JOIN:
		return "INNER_JOIN"
	case RIGHT_JOIN:
		return "RIGHT_JOIN"
	case FULL_JOIN:
		return "FULL_JOIN"
	case CROSS_JOIN:
		return "CROSS_JOIN"
	default:
		return ""
	}
}

type Join struct {
	Name     string
	Alias    string
	JoinType JoinType
	Expr     Expr

	Node
}

type Joins []Join

func (j Joins) node() {}

type Dimension struct {
	Expr Expr

	Node
}

type Dimensions []Dimension

func (d Dimensions) node() {}

func (d *Dimensions) GetWindow() *Window {
	for _, child := range *d {
		if w, ok := child.Expr.(*Window); ok {
			return w
		}
	}
	return nil
}

func (d *Dimensions) GetGroups() Dimensions {
	var nd Dimensions
	for _, child := range *d {
		if _, ok := child.Expr.(*Window); !ok {
			nd = append(nd, child)
		}
	}
	return nd
}

type WindowType int

const (
	NOT_WINDOW WindowType = iota
	TUMBLING_WINDOW
	HOPPING_WINDOW
	SLIDING_WINDOW
	SESSION_WINDOW
	COUNT_WINDOW
)

func (w WindowType) String() string {
	switch w {
	case NOT_WINDOW:
		return "NOT_WINDOW"
	case TUMBLING_WINDOW:
		return "TUMBLING_WINDOW"
	case HOPPING_WINDOW:
		return "HOPPING_WINDOW"
	case SLIDING_WINDOW:
		return "SLIDING_WINDOW"
	case SESSION_WINDOW:
		return "SESSION_WINDOW"
	case COUNT_WINDOW:
		return "COUNT_WINDOW"
	}
	return ""
}

type Window struct {
	WindowType WindowType
	Length     *IntegerLiteral
	Interval   *IntegerLiteral
	Filter     Expr
	Expr
}

type SortField struct {
	Name       string
	StreamName StreamName
	Uname      string // unique name of a field
	Ascending  bool
	FieldExpr  Expr

	Expr
}

func (sf *SortField) String() string {
	fe := ""
	if sf.FieldExpr != nil {
		fe += ", fieldExpr:{ " + sf.FieldExpr.String() + " }"
	}
	return "SortField:{ name:" + sf.Name + ", ascending:" + strconv.FormatBool(sf.Ascending) + fe + " }"
}

type SortFields []SortField

func (d SortFields) node() {}

const (
	RowkindInsert = "insert"
	RowkindUpdate = "update"
	RowkindUpsert = "upsert"
	RowkindDelete = "delete"
)
