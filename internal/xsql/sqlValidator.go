// Copyright 2021 EMQ Technologies Co., Ltd.
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
	if ast.HasAggFuncs(stmt.Condition) {
		return fmt.Errorf("Not allowed to call aggregate functions in WHERE clause.")
	}

	for _, d := range stmt.Dimensions {
		if ast.HasAggFuncs(d.Expr) {
			return fmt.Errorf("Not allowed to call aggregate functions in GROUP BY clause.")
		}
	}
	return nil
}
