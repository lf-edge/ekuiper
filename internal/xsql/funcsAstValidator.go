package xsql

import (
	"fmt"
	"github.com/emqx/kuiper/pkg/ast"
	"strings"
)

type AllowTypes struct {
	types []ast.Literal
}

func validateFuncs(funcName string, args []ast.Expr) error {
	lowerName := strings.ToLower(funcName)
	switch ast.FuncFinderSingleton().FuncType(lowerName) {
	case ast.NotFoundFunc:
		nf, _, err := parserFuncRuntime.Get(funcName)
		if err != nil {
			return fmt.Errorf("error getting function %s: %v", funcName, err)
		}
		var targs []interface{}
		for _, arg := range args {
			targs = append(targs, arg)
		}
		return nf.Validate(targs)
	case ast.AggFunc:
		return validateAggFunc(lowerName, args)
	case ast.MathFunc:
		return validateMathFunc(lowerName, args)
	case ast.ConvFunc:
		return validateConvFunc(lowerName, args)
	case ast.StrFunc:
		return validateStrFunc(lowerName, args)
	case ast.HashFunc:
		return validateHashFunc(lowerName, args)
	case ast.JsonFunc:
		return validateJsonFunc(lowerName, args)
	case ast.OtherFunc:
		return validateOtherFunc(lowerName, args)
	default:
		return fmt.Errorf("unkndow function %s", lowerName)
	}
}

func validateMathFunc(name string, args []ast.Expr) error {
	len := len(args)
	switch name {
	case "abs", "acos", "asin", "atan", "ceil", "cos", "cosh", "exp", "ln", "log", "round", "sign", "sin", "sinh",
		"sqrt", "tan", "tanh":
		if err := ast.ValidateLen(name, 1, len); err != nil {
			return err
		}
		if ast.IsStringArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) {
			return ast.ProduceErrInfo(name, 0, "number - float or int")
		}
	case "bitand", "bitor", "bitxor":
		if err := ast.ValidateLen(name, 2, len); err != nil {
			return err
		}
		if ast.IsFloatArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) {
			return ast.ProduceErrInfo(name, 0, "int")
		}
		if ast.IsFloatArg(args[1]) || ast.IsStringArg(args[1]) || ast.IsTimeArg(args[1]) || ast.IsBooleanArg(args[1]) {
			return ast.ProduceErrInfo(name, 1, "int")
		}

	case "bitnot":
		if err := ast.ValidateLen(name, 1, len); err != nil {
			return err
		}
		if ast.IsFloatArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) {
			return ast.ProduceErrInfo(name, 0, "int")
		}

	case "atan2", "mod", "power":
		if err := ast.ValidateLen(name, 2, len); err != nil {
			return err
		}
		if ast.IsStringArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) {
			return ast.ProduceErrInfo(name, 0, "number - float or int")
		}
		if ast.IsStringArg(args[1]) || ast.IsTimeArg(args[1]) || ast.IsBooleanArg(args[1]) {
			return ast.ProduceErrInfo(name, 1, "number - float or int")
		}

	case "rand":
		if err := ast.ValidateLen(name, 0, len); err != nil {
			return err
		}
	}
	return nil
}

func validateStrFunc(name string, args []ast.Expr) error {
	len := len(args)
	switch name {
	case "concat":
		if len == 0 {
			return fmt.Errorf("The arguments for %s should be at least one.\n", name)
		}
		for i, a := range args {
			if ast.IsNumericArg(a) || ast.IsTimeArg(a) || ast.IsBooleanArg(a) {
				return ast.ProduceErrInfo(name, i, "string")
			}
		}
	case "endswith", "indexof", "regexp_matches", "startswith":
		if err := ast.ValidateLen(name, 2, len); err != nil {
			return err
		}
		for i := 0; i < 2; i++ {
			if ast.IsNumericArg(args[i]) || ast.IsTimeArg(args[i]) || ast.IsBooleanArg(args[i]) {
				return ast.ProduceErrInfo(name, i, "string")
			}
		}
	case "format_time":
		if err := ast.ValidateLen(name, 2, len); err != nil {
			return err
		}

		if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
			return ast.ProduceErrInfo(name, 0, "datetime")
		}
		if ast.IsNumericArg(args[1]) || ast.IsTimeArg(args[1]) || ast.IsBooleanArg(args[1]) {
			return ast.ProduceErrInfo(name, 1, "string")
		}

	case "regexp_replace":
		if err := ast.ValidateLen(name, 3, len); err != nil {
			return err
		}
		for i := 0; i < 3; i++ {
			if ast.IsNumericArg(args[i]) || ast.IsTimeArg(args[i]) || ast.IsBooleanArg(args[i]) {
				return ast.ProduceErrInfo(name, i, "string")
			}
		}
	case "length", "lower", "ltrim", "numbytes", "rtrim", "trim", "upper":
		if err := ast.ValidateLen(name, 1, len); err != nil {
			return err
		}
		if ast.IsNumericArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) {
			return ast.ProduceErrInfo(name, 0, "string")
		}
	case "lpad", "rpad":
		if err := ast.ValidateLen(name, 2, len); err != nil {
			return err
		}
		if ast.IsNumericArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) {
			return ast.ProduceErrInfo(name, 0, "string")
		}
		if ast.IsFloatArg(args[1]) || ast.IsTimeArg(args[1]) || ast.IsBooleanArg(args[1]) || ast.IsStringArg(args[1]) {
			return ast.ProduceErrInfo(name, 1, "int")
		}
	case "substring":
		if len != 2 && len != 3 {
			return fmt.Errorf("the arguments for substring should be 2 or 3")
		}
		if ast.IsNumericArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) {
			return ast.ProduceErrInfo(name, 0, "string")
		}
		for i := 1; i < len; i++ {
			if ast.IsFloatArg(args[i]) || ast.IsTimeArg(args[i]) || ast.IsBooleanArg(args[i]) || ast.IsStringArg(args[i]) {
				return ast.ProduceErrInfo(name, i, "int")
			}
		}

		if s, ok := args[1].(*ast.IntegerLiteral); ok {
			sv := s.Val
			if sv < 0 {
				return fmt.Errorf("The start index should not be a nagtive integer.")
			}
			if len == 3 {
				if e, ok1 := args[2].(*ast.IntegerLiteral); ok1 {
					ev := e.Val
					if ev < sv {
						return fmt.Errorf("The end index should be larger than start index.")
					}
				}
			}
		}
	case "split_value":
		if len != 3 {
			return fmt.Errorf("the arguments for split_value should be 3")
		}
		if ast.IsNumericArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) {
			return ast.ProduceErrInfo(name, 0, "string")
		}
		if ast.IsNumericArg(args[1]) || ast.IsTimeArg(args[1]) || ast.IsBooleanArg(args[1]) {
			return ast.ProduceErrInfo(name, 1, "string")
		}
		if ast.IsFloatArg(args[2]) || ast.IsTimeArg(args[2]) || ast.IsBooleanArg(args[2]) || ast.IsStringArg(args[2]) {
			return ast.ProduceErrInfo(name, 2, "int")
		}
		if s, ok := args[2].(*ast.IntegerLiteral); ok {
			if s.Val < 0 {
				return fmt.Errorf("The index should not be a nagtive integer.")
			}
		}
	}
	return nil
}

func validateConvFunc(name string, args []ast.Expr) error {
	len := len(args)
	switch name {
	case "cast":
		if err := ast.ValidateLen(name, 2, len); err != nil {
			return err
		}
		a := args[1]
		if !ast.IsStringArg(a) {
			return ast.ProduceErrInfo(name, 1, "string")
		}
		if av, ok := a.(*ast.StringLiteral); ok {
			if !(av.Val == "bigint" || av.Val == "float" || av.Val == "string" || av.Val == "boolean" || av.Val == "datetime") {
				return fmt.Errorf("Expect one of following value for the 2nd parameter: bigint, float, string, boolean, datetime.")
			}
		}
	case "chr":
		if err := ast.ValidateLen(name, 1, len); err != nil {
			return err
		}
		if ast.IsFloatArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) {
			return ast.ProduceErrInfo(name, 0, "int")
		}
	case "encode":
		if err := ast.ValidateLen(name, 2, len); err != nil {
			return err
		}

		if ast.IsNumericArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) {
			return ast.ProduceErrInfo(name, 0, "string")
		}

		a := args[1]
		if !ast.IsStringArg(a) {
			return ast.ProduceErrInfo(name, 1, "string")
		}
		if av, ok := a.(*ast.StringLiteral); ok {
			if av.Val != "base64" {
				return fmt.Errorf("Only base64 is supported for the 2nd parameter.")
			}
		}
	case "trunc":
		if err := ast.ValidateLen(name, 2, len); err != nil {
			return err
		}

		if ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) || ast.IsStringArg(args[0]) {
			return ast.ProduceErrInfo(name, 0, "number - float or int")
		}

		if ast.IsFloatArg(args[1]) || ast.IsTimeArg(args[1]) || ast.IsBooleanArg(args[1]) || ast.IsStringArg(args[1]) {
			return ast.ProduceErrInfo(name, 1, "int")
		}
	}
	return nil
}

func validateHashFunc(name string, args []ast.Expr) error {
	len := len(args)
	switch name {
	case "md5", "sha1", "sha224", "sha256", "sha384", "sha512":
		if err := ast.ValidateLen(name, 1, len); err != nil {
			return err
		}

		if ast.IsNumericArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) {
			return ast.ProduceErrInfo(name, 0, "string")
		}
	}
	return nil
}

func validateOtherFunc(name string, args []ast.Expr) error {
	len := len(args)
	switch name {
	case "isNull":
		if err := ast.ValidateLen(name, 1, len); err != nil {
			return err
		}
	case "cardinality":
		if err := ast.ValidateLen(name, 1, len); err != nil {
			return err
		}
	case "nanvl":
		if err := ast.ValidateLen(name, 2, len); err != nil {
			return err
		}
		if ast.IsIntegerArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) || ast.IsStringArg(args[0]) {
			return ast.ProduceErrInfo(name, 1, "float")
		}
	case "newuuid":
		if err := ast.ValidateLen(name, 0, len); err != nil {
			return err
		}
	case "mqtt":
		if err := ast.ValidateLen(name, 1, len); err != nil {
			return err
		}
		if ast.IsIntegerArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsFloatArg(args[0]) {
			return ast.ProduceErrInfo(name, 0, "meta reference")
		}
		if p, ok := args[0].(*ast.MetaRef); ok {
			name := strings.ToLower(p.Name)
			if name != "topic" && name != "messageid" {
				return fmt.Errorf("Parameter of mqtt function can be only topic or messageid.")
			}
		}
	case "meta":
		if err := ast.ValidateLen(name, 1, len); err != nil {
			return err
		}
		if _, ok := args[0].(*ast.MetaRef); ok {
			return nil
		}
		expr := args[0]
		for {
			if be, ok := expr.(*ast.BinaryExpr); ok {
				if _, ok := be.LHS.(*ast.MetaRef); ok && be.OP == ast.ARROW {
					return nil
				}
				expr = be.LHS
			} else {
				break
			}
		}
		return ast.ProduceErrInfo(name, 0, "meta reference")
	}
	return nil
}

func validateJsonFunc(name string, args []ast.Expr) error {
	len := len(args)
	if err := ast.ValidateLen(name, 2, len); err != nil {
		return err
	}
	if !ast.IsStringArg(args[1]) {
		return ast.ProduceErrInfo(name, 1, "string")
	}
	return nil
}

func validateAggFunc(name string, args []ast.Expr) error {
	len := len(args)
	switch name {
	case "avg", "max", "min", "sum":
		if err := ast.ValidateLen(name, 1, len); err != nil {
			return err
		}
		if ast.IsStringArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) {
			return ast.ProduceErrInfo(name, 0, "number - float or int")
		}
	case "count":
		if err := ast.ValidateLen(name, 1, len); err != nil {
			return err
		}
	case "collect":
		if err := ast.ValidateLen(name, 1, len); err != nil {
			return err
		}
	case "deduplicate":
		if err := ast.ValidateLen(name, 2, len); err != nil {
			return err
		}
		if !ast.IsBooleanArg(args[1]) {
			return ast.ProduceErrInfo(name, 1, "bool")
		}
	}
	return nil
}
