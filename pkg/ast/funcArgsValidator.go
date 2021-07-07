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

package ast

import "fmt"

// ProduceErrInfo Index is starting from 0
func ProduceErrInfo(name string, index int, expect string) (err error) {
	index++
	err = fmt.Errorf("Expect %s type for %d parameter of function %s.", expect, index, name)
	return
}

func ValidateLen(funcName string, exp, actual int) error {
	if actual != exp {
		return fmt.Errorf("The arguments for %s should be %d.", funcName, exp)
	}
	return nil
}

func IsNumericArg(arg Expr) bool {
	if _, ok := arg.(*NumberLiteral); ok {
		return true
	} else if _, ok := arg.(*IntegerLiteral); ok {
		return true
	}
	return false
}

func IsIntegerArg(arg Expr) bool {
	if _, ok := arg.(*IntegerLiteral); ok {
		return true
	}
	return false
}

func IsFloatArg(arg Expr) bool {
	if _, ok := arg.(*NumberLiteral); ok {
		return true
	}
	return false
}

func IsBooleanArg(arg Expr) bool {
	switch t := arg.(type) {
	case *BooleanLiteral:
		return true
	case *BinaryExpr:
		switch t.OP {
		case AND, OR, EQ, NEQ, LT, LTE, GT, GTE:
			return true
		default:
			return false
		}
	default:
		return false
	}
}

func IsStringArg(arg Expr) bool {
	if _, ok := arg.(*StringLiteral); ok {
		return true
	}
	return false
}

func IsTimeArg(arg Expr) bool {
	if _, ok := arg.(*TimeLiteral); ok {
		return true
	}
	return false
}
