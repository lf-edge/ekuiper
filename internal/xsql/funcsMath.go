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
	"math"
	"math/rand"
)

func mathCall(name string, args []interface{}) (interface{}, bool) {
	switch name {
	case "abs":
		if v, ok := args[0].(int); ok {
			t := float64(v)
			var ret = int(math.Abs(t))
			return ret, true
		} else if v, ok := args[0].(float64); ok {
			return math.Abs(v), true
		} else {
			return fmt.Errorf("only float64 & int type are supported"), false
		}
	case "acos":
		if v, e := toF64(args[0]); e == nil {
			return math.Acos(v), true
		} else {
			return e, false
		}
	case "asin":
		if v, e := toF64(args[0]); e == nil {
			return math.Asin(v), true
		} else {
			return e, false
		}
	case "atan":
		if v, e := toF64(args[0]); e == nil {
			return math.Atan(v), true
		} else {
			return e, false
		}
	case "atan2":
		if v1, e := toF64(args[0]); e == nil {
			if v2, e1 := toF64(args[1]); e1 == nil {
				return math.Atan2(v1, v2), true
			} else {
				return e1, false
			}
		} else {
			return e, false
		}
	case "bitand":
		v1, ok1 := args[0].(int)
		v2, ok2 := args[1].(int)
		if ok1 && ok2 {
			return v1 & v2, true
		} else {
			return fmt.Errorf("Expect int type for both operands."), false
		}
	case "bitor":
		v1, ok1 := args[0].(int)
		v2, ok2 := args[1].(int)
		if ok1 && ok2 {
			return v1 | v2, true
		} else {
			return fmt.Errorf("Expect int type for both operands."), false
		}
	case "bitxor":
		v1, ok1 := args[0].(int)
		v2, ok2 := args[1].(int)
		if ok1 && ok2 {
			return v1 ^ v2, true
		} else {
			return fmt.Errorf("Expect int type for both operands."), false
		}
	case "bitnot":
		v1, ok1 := args[0].(int)
		if ok1 {
			return ^v1, true
		} else {
			return fmt.Errorf("Expect int type for operand."), false
		}
	case "ceil":
		if v, e := toF64(args[0]); e == nil {
			return math.Ceil(v), true
		} else {
			return e, false
		}
	case "cos":
		if v, e := toF64(args[0]); e == nil {
			return math.Cos(v), true
		} else {
			return e, false
		}
	case "cosh":
		if v, e := toF64(args[0]); e == nil {
			return math.Cosh(v), true
		} else {
			return e, false
		}
	case "exp":
		if v, e := toF64(args[0]); e == nil {
			return math.Exp(v), true
		} else {
			return e, false
		}
	case "ln":
		if v, e := toF64(args[0]); e == nil {
			return math.Log2(v), true
		} else {
			return e, false
		}
	case "log":
		if v, e := toF64(args[0]); e == nil {
			return math.Log10(v), true
		} else {
			return e, false
		}
	case "mod":
		if v, e := toF64(args[0]); e == nil {
			if v1, e1 := toF64(args[1]); e == nil {
				return math.Mod(v, v1), true
			} else {
				return e1, false
			}
		} else {
			return e, false
		}
	case "power":
		if v1, e := toF64(args[0]); e == nil {
			if v2, e2 := toF64(args[1]); e2 == nil {
				return math.Pow(v1, v2), true
			} else {
				return e2, false
			}
		} else {
			return e, false
		}
	case "rand":
		return rand.Float64(), true
	case "round":
		if v, e := toF64(args[0]); e == nil {
			return math.Round(v), true
		} else {
			return e, false
		}
	case "sign":
		if v, e := toF64(args[0]); e == nil {
			if v > 0 {
				return 1, true
			} else if v < 0 {
				return -1, true
			} else {
				return 0, true
			}
		} else {
			return e, false
		}
	case "sin":
		if v, e := toF64(args[0]); e == nil {
			return math.Sin(v), true
		} else {
			return e, false
		}
	case "sinh":
		if v, e := toF64(args[0]); e == nil {
			return math.Sinh(v), true
		} else {
			return e, false
		}
	case "sqrt":
		if v, e := toF64(args[0]); e == nil {
			return math.Sqrt(v), true
		} else {
			return e, false
		}

	case "tan":
		if v, e := toF64(args[0]); e == nil {
			return math.Tan(v), true
		} else {
			return e, false
		}

	case "tanh":
		if v, e := toF64(args[0]); e == nil {
			return math.Tanh(v), true
		} else {
			return e, false
		}
	}

	return fmt.Errorf("Unknown math function name."), false
}

func toF64(arg interface{}) (float64, error) {
	if v, ok := arg.(float64); ok {
		return v, nil
	} else if v, ok := arg.(int); ok {
		return float64(v), nil
	}
	return 0, fmt.Errorf("only float64 & int type are supported")
}
