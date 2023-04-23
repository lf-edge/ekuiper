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

package xsql

import (
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

// Validate validate select statement without context.
// This is the pre-validation. In planner, there will be a more comprehensive validation after binding
func Validate(stmt *ast.SelectStatement) error {
	if HasAggFuncs(stmt.Condition) {
		return fmt.Errorf("Not allowed to call aggregate functions in WHERE clause.")
	}

	for _, d := range stmt.Dimensions {
		if HasAggFuncs(d.Expr) {
			return fmt.Errorf("Not allowed to call aggregate functions in GROUP BY clause.")
		}
	}

	if err := validateSRFNestedForbidden("select", stmt.Fields); err != nil {
		return err
	}
	if err := validateMultiSRFForbidden("select", stmt.Fields); err != nil {
		return err
	}
	return validateSRFForbidden(stmt)
}

func validateSRFNestedForbidden(clause string, node ast.Node) error {
	if isSRFNested(node) {
		return fmt.Errorf("%s clause shouldn't has nested set-returning-functions", clause)
	}
	return nil
}

func validateMultiSRFForbidden(clause string, node ast.Node) error {
	firstSRF := false
	nextSRF := false
	ast.WalkFunc(node, func(n ast.Node) bool {
		switch f := n.(type) {
		case *ast.Call:
			if f.FuncType == ast.FuncTypeSrf {
				if !firstSRF {
					firstSRF = true
				} else {
					nextSRF = true
					return false
				}
			}
		}
		return true
	})
	if nextSRF {
		return fmt.Errorf("%s clause shouldn't has multi set-returning-functions", clause)
	}
	return nil
}

func validateSRFForbidden(node ast.Node) error {
	if isSRFExists(node) {
		return fmt.Errorf("select statement shouldn't has srf except fields")
	}
	return nil
}

func isSRFNested(node ast.Node) bool {
	srfNested := false
	ast.WalkFunc(node, func(n ast.Node) bool {
		switch f := n.(type) {
		case *ast.Call:
			for _, arg := range f.Args {
				exists := isSRFExists(arg)
				if exists {
					srfNested = true
					return false
				}
			}
			return true
		}
		return true
	})
	return srfNested
}

func isSRFExists(node ast.Node) bool {
	exists := false
	ast.WalkFunc(node, func(n ast.Node) bool {
		switch f := n.(type) {
		// skip checking Fields
		case ast.Fields:
			return false
		case *ast.Call:
			if f.FuncType == ast.FuncTypeSrf {
				exists = true
				return false
			}
		}
		return true
	})
	return exists
}
