// Copyright 2022 EMQ Technologies Co., Ltd.
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

// ProduceErrInfo Index is starting from 0
func ProduceErrInfo(index int, expect string) (err error) {
	index++
	err = fmt.Errorf("Expect %s type for parameter %d", expect, index)
	return
}

func ValidateLen(exp, actual int) error {
	if actual != exp {
		return fmt.Errorf("Expect %d arguments but found %d.", exp, actual)
	}
	return nil
}

// Shared validating functions

func ValidateNoArg(_ api.FunctionContext, args []ast.Expr) error {
	return ValidateLen(0, len(args))
}

func ValidateOneArg(_ api.FunctionContext, args []ast.Expr) error {
	return ValidateLen(1, len(args))
}

func ValidateOneNumberArg(_ api.FunctionContext, args []ast.Expr) error {
	if err := ValidateLen(1, len(args)); err != nil {
		return err
	}
	if ast.IsStringArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) {
		return ProduceErrInfo(0, "number - float or int")
	}
	return nil
}

func ValidateTwoNumberArg(_ api.FunctionContext, args []ast.Expr) error {
	if err := ValidateLen(2, len(args)); err != nil {
		return err
	}
	if ast.IsStringArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) {
		return ProduceErrInfo(0, "number - float or int")
	}
	if ast.IsStringArg(args[1]) || ast.IsTimeArg(args[1]) || ast.IsBooleanArg(args[1]) {
		return ProduceErrInfo(1, "number - float or int")
	}
	return nil
}

func ValidateTwoIntArg(_ api.FunctionContext, args []ast.Expr) error {
	if err := ValidateLen(2, len(args)); err != nil {
		return err
	}
	if ast.IsFloatArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) {
		return ProduceErrInfo(0, "int")
	}
	if ast.IsFloatArg(args[1]) || ast.IsStringArg(args[1]) || ast.IsTimeArg(args[1]) || ast.IsBooleanArg(args[1]) {
		return ProduceErrInfo(1, "int")
	}
	return nil
}

func ValidateTwoStrArg(_ api.FunctionContext, args []ast.Expr) error {
	if err := ValidateLen(2, len(args)); err != nil {
		return err
	}
	for i := 0; i < 2; i++ {
		if ast.IsNumericArg(args[i]) || ast.IsTimeArg(args[i]) || ast.IsBooleanArg(args[i]) {
			return ProduceErrInfo(i, "string")
		}
	}
	return nil
}

func ValidateOneStrArg(_ api.FunctionContext, args []ast.Expr) error {
	if err := ValidateLen(1, len(args)); err != nil {
		return err
	}
	if ast.IsNumericArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) {
		return ProduceErrInfo(0, "string")
	}
	return nil
}

func ValidateOneStrOneInt(_ api.FunctionContext, args []ast.Expr) error {
	if err := ValidateLen(2, len(args)); err != nil {
		return err
	}
	if ast.IsNumericArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) {
		return ProduceErrInfo(0, "string")
	}
	if ast.IsFloatArg(args[1]) || ast.IsTimeArg(args[1]) || ast.IsBooleanArg(args[1]) || ast.IsStringArg(args[1]) {
		return ProduceErrInfo(1, "int")
	}
	return nil
}

func ValidateJsonFunc(_ api.FunctionContext, args []ast.Expr) error {
	l := len(args)
	if err := ValidateLen(2, l); err != nil {
		return err
	}
	if !ast.IsStringArg(args[1]) {
		return ProduceErrInfo(1, "string")
	}
	return nil
}
