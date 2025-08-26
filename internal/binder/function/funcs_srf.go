// Copyright 2023-2024 EMQ Technologies Co., Ltd.
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

package function

import (
	"fmt"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

func registerSetReturningFunc() {
	builtins["unnest"] = builtinFunc{
		fType: ast.FuncTypeSrf,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg := args[0]
			argArray, ok := arg.([]interface{})
			if !ok {
				return arg, true
			}
			return argArray, true
		},
		val:   ValidateOneArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["extract"] = builtinFunc{
		fType: ast.FuncTypeSrf,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg := args[0]
			argMap, ok := arg.(map[string]interface{})
			if !ok {
				return fmt.Errorf("extract should use map"), false
			}
			return []interface{}{argMap}, true
		},
		val:   ValidateOneArg,
		check: returnNilIfHasAnyNil,
	}

}
