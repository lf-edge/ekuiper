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
