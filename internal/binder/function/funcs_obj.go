// Copyright 2023 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

func registerObjectFunc() {
	builtins["keys"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg := args[0]
			if arg, ok := arg.(map[string]interface{}); ok {
				list := make([]string, 0, len(arg))
				for key := range arg {
					list = append(list, key)
				}
				return list, true
			}
			return fmt.Errorf("the arg for keys should be map[string]interface{}"), false
		},
		val: ValidateOneArg,
	}
}
