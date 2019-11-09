package xsql

import (
	"fmt"
	"strings"
)

type AllowTypes struct {
	types []Literal
}

func validateFuncs(funcName string, args []Expr) (error) {
	lowerName := strings.ToLower(funcName)
	if _, ok := mathFuncMap[lowerName]; ok {
		return validateMathFunc(funcName, args)
	} else if _, ok := strFuncMap[lowerName]; ok {
		return validateStrFunc(funcName, args)
	} else if _, ok := convFuncMap[lowerName]; ok {
		return validateConvFunc(lowerName, args)
	} else if _, ok := hashFuncMap[lowerName]; ok {
		return validateHashFunc(lowerName, args)
	} else if _, ok := otherFuncMap[lowerName]; ok {
		return validateOtherFunc(lowerName, args)
	}
	return nil
}

func validateMathFunc(name string, args []Expr) (error) {
	len := len(args)
	switch name {
	case "abs", "acos", "asin", "atan", "ceil", "cos", "cosh", "exp", "ln", "log", "round", "sign", "sin", "sinh",
	"sqrt", "tan", "tanh" :
		if err := validateLen(name, 1, len); err != nil {
			return  err
		}
		if isStringArg(args[0]) || isTimeArg(args[0]) || isBooleanArg(args[0]) {
			return produceErrInfo(name, 0, "number - float or int")
		}
	case "bitand", "bitor", "bitxor":
		if err := validateLen(name, 2, len); err != nil {
			return  err
		}
		if isFloatArg(args[0]) || isStringArg(args[0]) || isTimeArg(args[0]) || isBooleanArg(args[0]){
			return produceErrInfo(name, 0, "int")
		}
		if isFloatArg(args[1]) || isStringArg(args[1]) || isTimeArg(args[1]) || isBooleanArg(args[1]) {
			return produceErrInfo(name, 1, "int")
		}

	case "bitnot":
		if err := validateLen(name, 1, len); err != nil {
			return  err
		}
		if isFloatArg(args[0]) || isStringArg(args[0]) || isTimeArg(args[0]) || isBooleanArg(args[0])  {
			return produceErrInfo(name, 0, "int")
		}

	case "atan2", "mod", "power":
		if err := validateLen(name, 2, len); err != nil {
			return  err
		}
		if isStringArg(args[0]) || isTimeArg(args[0]) || isBooleanArg(args[0]) {
			return produceErrInfo(name, 0, "number - float or int")
		}
		if isStringArg(args[1]) || isTimeArg(args[1]) || isBooleanArg(args[1]){
			return produceErrInfo(name, 1, "number - float or int")
		}

	case "rand":
		if err := validateLen(name, 0, len); err != nil {
			return  err
		}
	}
	return nil
}

func validateStrFunc(name string, args []Expr) (error) {
	len := len(args)
	switch name {
	case "concat":
		if len == 0 {
			return fmt.Errorf("The arguments for %s should be at least one.\n", name)
		}
		for i, a := range args {
			if isNumericArg(a) || isTimeArg(a) || isBooleanArg(a) {
				return produceErrInfo(name, i, "string")
			}
		}
	case "endswith", "indexof", "regexp_matches", "startswith":
		if err := validateLen(name, 2, len); err != nil {
			return  err
		}
		for i := 0; i < 2; i++ {
			if isNumericArg(args[i]) || isTimeArg(args[i])|| isBooleanArg(args[i]) {
				return produceErrInfo(name, i, "string")
			}
		}
	case "format_time":
		if err := validateLen(name, 2, len); err != nil {
			return  err
		}

		if isNumericArg(args[0]) || isStringArg(args[0])|| isBooleanArg(args[0]) {
			return produceErrInfo(name, 0, "datetime")
		}
		if isNumericArg(args[1]) || isTimeArg(args[1])|| isBooleanArg(args[1]) {
			return produceErrInfo(name, 1, "string")
		}

	case "regexp_replace":
		if err := validateLen(name, 3, len); err != nil {
			return  err
		}
		for i := 0; i < 3; i++ {
			if isNumericArg(args[i]) || isTimeArg(args[i])|| isBooleanArg(args[i]) {
				return produceErrInfo(name, i, "string")
			}
		}
	case "length", "lower", "ltrim", "numbytes", "rtrim", "trim", "upper":
		if err := validateLen(name, 1, len); err != nil {
			return  err
		}
		if isNumericArg(args[0]) || isTimeArg(args[0]) || isBooleanArg(args[0]) {
			return produceErrInfo(name, 0, "string")
		}
	case "lpad", "rpad":
		if err := validateLen(name, 2, len); err != nil {
			return  err
		}
		if isNumericArg(args[0]) || isTimeArg(args[0]) || isBooleanArg(args[0]) {
			return produceErrInfo(name, 0, "string")
		}
		if isFloatArg(args[1]) || isTimeArg(args[1]) || isBooleanArg(args[1]) || isStringArg(args[1]) {
			return produceErrInfo(name, 1, "int")
		}
	case "substring":
		if len != 2 && len != 3 {
			return fmt.Errorf("the arguments for substring should be 2 or 3")
		}
		if isNumericArg(args[0]) || isTimeArg(args[0]) || isBooleanArg(args[0]) {
			return produceErrInfo(name, 0, "string")
		}
		for i := 1; i < len; i++ {
			if isFloatArg(args[i]) || isTimeArg(args[i]) || isBooleanArg(args[i]) || isStringArg(args[i]) {
				return produceErrInfo(name, i, "int")
			}
		}

		if s, ok := args[1].(*IntegerLiteral); ok {
			sv := s.Val
			if sv < 0 {
				return fmt.Errorf("The start index should not be a nagtive integer.")
			}
			if len == 3{
				if e, ok1 := args[2].(*IntegerLiteral); ok1 {
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
		if isNumericArg(args[0]) || isTimeArg(args[0]) || isBooleanArg(args[0]) {
			return produceErrInfo(name, 0, "string")
		}
		if isNumericArg(args[1]) || isTimeArg(args[1]) || isBooleanArg(args[1]) {
			return produceErrInfo(name, 1, "string")
		}
		if isFloatArg(args[2]) || isTimeArg(args[2]) || isBooleanArg(args[2]) || isStringArg(args[2]) {
			return produceErrInfo(name, 2, "int")
		}
		if s, ok := args[2].(*IntegerLiteral); ok {
			if s.Val < 0 {
				return fmt.Errorf("The index should not be a nagtive integer.")
			}
		}
	}
	return nil
}

func validateConvFunc(name string, args []Expr) (error) {
	len := len(args)
	switch name {
	case "cast":
		if err := validateLen(name, 2, len); err != nil {
			return  err
		}
		a := args[1]
		if !isStringArg(a) {
			return produceErrInfo(name, 1, "string")
		}
		if av, ok := a.(*StringLiteral); ok {
			if !(av.Val == "bigint" || av.Val == "float" || av.Val == "string" || av.Val == "boolean" || av.Val == "datetime") {
				return fmt.Errorf("Expect one of following value for the 2nd parameter: bigint, float, string, boolean, datetime.")
			}
		}
	case "chr":
		if err := validateLen(name, 1, len); err != nil {
			return  err
		}
		if isFloatArg(args[0]) || isTimeArg(args[0]) || isBooleanArg(args[0]) {
			return produceErrInfo(name, 0, "int")
		}
	case "encode":
		if err := validateLen(name, 2, len); err != nil {
			return  err
		}

		if isNumericArg(args[0]) || isTimeArg(args[0]) || isBooleanArg(args[0]) {
			return produceErrInfo(name, 0, "string")
		}

		a := args[1]
		if !isStringArg(a) {
			return produceErrInfo(name, 1, "string")
		}
		if av, ok := a.(*StringLiteral); ok {
			if av.Val != "base64" {
				return fmt.Errorf("Only base64 is supported for the 2nd parameter.")
			}
		}
	case "trunc":
		if err := validateLen(name, 2, len); err != nil {
			return  err
		}

		if isTimeArg(args[0]) || isBooleanArg(args[0]) || isStringArg(args[0]) {
			return produceErrInfo(name, 0, "number - float or int")
		}

		if isFloatArg(args[1]) || isTimeArg(args[1]) || isBooleanArg(args[1]) || isStringArg(args[1]) {
			return produceErrInfo(name, 1, "int")
		}
	}
	return nil
}

func validateHashFunc(name string, args []Expr) (error) {
	len := len(args)
	switch name {
	case "md5", "sha1", "sha224", "sha256", "sha384", "sha512":
		if err := validateLen(name, 1, len); err != nil {
			return err
		}

		if isNumericArg(args[0]) || isTimeArg(args[0]) || isBooleanArg(args[0]) {
			return produceErrInfo(name, 0, "string")
		}
	}
	return nil
}

func validateOtherFunc(name string, args []Expr) (error) {
	len := len(args)
	switch name {
	case "isNull":
		if err := validateLen(name, 1, len); err != nil {
			return err
		}
	case "nanvl":
		if err := validateLen(name, 2, len); err != nil {
			return err
		}
		if isIntegerArg(args[0]) || isTimeArg(args[0]) || isBooleanArg(args[0]) || isStringArg(args[0]) {
			return produceErrInfo(name, 1, "float")
		}
	case "newuuid":
		if err := validateLen(name, 0, len); err != nil {
			return  err
		}
	case "mqtt":
		if err := validateLen(name, 1, len); err != nil {
			return err
		}
		if isIntegerArg(args[0]) || isTimeArg(args[0]) || isBooleanArg(args[0]) || isStringArg(args[0]) || isFloatArg(args[0]) {
			return produceErrInfo(name, 0, "field reference")
		}
		if p, ok := args[0].(*FieldRef); ok {
			if _, ok := SpecialKeyMapper[p.Name]; !ok {
				return fmt.Errorf("Parameter of mqtt function can be only topic or messageid.")
			}
		}
	}
	return nil
}


// Index is starting from 0
func produceErrInfo(name string, index int, expect string) (err error) {
	index++
	err = fmt.Errorf("Expect %s type for %d parameter of function %s.", expect, index, name)
	return
}

func validateLen(funcName string, exp, actual int) (error) {
	if actual != exp {
		return fmt.Errorf("The arguments for %s should be %d.", funcName, exp)
	}
	return nil
}

func isNumericArg(arg Expr) bool {
	if _, ok := arg.(*NumberLiteral); ok {
		return true
	} else if _, ok := arg.(*IntegerLiteral); ok {
		return true
	}
	return false
}

func isIntegerArg(arg Expr) bool {
	if _, ok := arg.(*IntegerLiteral); ok {
		return true
	}
	return false
}

func isFloatArg(arg Expr) bool {
	if _, ok := arg.(*NumberLiteral); ok {
		return true
	}
	return false
}

func isBooleanArg(arg Expr) bool {
	if _, ok := arg.(*BooleanLiteral); ok {
		return true
	}
	return false
}

func isStringArg(arg Expr) bool {
	if _, ok := arg.(*StringLiteral); ok {
		return true
	}
	return false
}

func isTimeArg(arg Expr) bool {
	if _, ok := arg.(*TimeLiteral); ok {
		return true
	}
	return false
}
