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

package ast

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
		case AND, OR, EQ, NEQ, LT, LTE, GT, GTE, BETWEEN, NOTBETWEEN, IN, NOTIN, LIKE, NOTLIKE:
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

func IsFieldRefArg(arg Expr) bool {
	if _, ok := arg.(*FieldRef); ok {
		return true
	}
	return false
}
