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
)

type ResolverVisitor struct {
	Stmt         *ast.SelectStatement
	aliasToTable map[string]string
	tableToAlias map[string]string
}

func (r *ResolverVisitor) GetOptimizedStmt() *ast.SelectStatement {
	r.getAliasMapping()
	r.transFormTree()
	return r.Stmt
}

func (r *ResolverVisitor) getAliasMapping() {
	m := make(map[string]string)
	m2 := make(map[string]string)

	ast.WalkFunc(r.Stmt, func(n ast.Node) bool {
		switch f := n.(type) {
		case *ast.Table:
			m[f.Alias] = f.Name
			m2[f.Name] = f.Alias
		case *ast.Join:
			m[f.Alias] = f.Name
			m2[f.Name] = f.Alias
		}
		return true
	})
	r.aliasToTable = m
	r.tableToAlias = m2
}

func (r *ResolverVisitor) transformToRealIfExist(aliasName string) string {
	if v, ok := r.aliasToTable[aliasName]; ok {
		return v
	} else {
		return aliasName
	}
}

/*
select t.a from test as t
->
select test.a as `t.a` from test as test
*/
func (r *ResolverVisitor) transFormTree() {
	// t.a -> test.a as `t.a`
	ast.WalkFunc(r.Stmt, func(n ast.Node) bool {
		switch f := n.(type) {
		case *ast.Field:
			fr, ok := f.Expr.(*ast.FieldRef)
			if !ok {
				return true
			}
			if f.AName != "" {
				return true
			}
			if fr.StreamName == "" {
				return true
			}
			f.AName = fmt.Sprintf("%s.%s", fr.StreamName, f.Name)
		}
		return true
	})

	// transform stream alias to real stream name
	// test as t -> test as test
	ast.WalkFunc(r.Stmt, func(n ast.Node) bool {
		switch f := n.(type) {
		case *ast.FieldRef:
			f.StreamName = ast.StreamName(r.transformToRealIfExist(string(f.StreamName)))
		}
		return true
	})
}
