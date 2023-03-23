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
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"
)

func registerMiscFunc() {
	builtins["cast"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, ok := args[1].(string); ok {
				switch v {
				case "bigint":
					if v1, ok1 := args[0].(int); ok1 {
						return v1, true
					} else if v1, ok1 := args[0].(float64); ok1 {
						return int(v1), true
					} else if v1, ok1 := args[0].(string); ok1 {
						if temp, err := strconv.Atoi(v1); err == nil {
							return temp, true
						} else {
							return err, false
						}
					} else if v1, ok1 := args[0].(bool); ok1 {
						if v1 {
							return 1, true
						} else {
							return 0, true
						}
					} else {
						return fmt.Errorf("Not supported type conversion."), false
					}
				case "float":
					if v1, ok1 := args[0].(int); ok1 {
						return float64(v1), true
					} else if v1, ok1 := args[0].(float64); ok1 {
						return v1, true
					} else if v1, ok1 := args[0].(string); ok1 {
						if temp, err := strconv.ParseFloat(v1, 64); err == nil {
							return temp, true
						} else {
							return err, false
						}
					} else if v1, ok1 := args[0].(bool); ok1 {
						if v1 {
							return 1.0, true
						} else {
							return 0.0, true
						}
					} else {
						return fmt.Errorf("Not supported type conversion."), false
					}
				case "string":
					r, e := cast.ToString(args[0], cast.CONVERT_ALL)
					if e != nil {
						return fmt.Errorf("Not supported type conversion, got error %v.", e), false
					} else {
						return r, true
					}
				case "boolean":
					if v1, ok1 := args[0].(int); ok1 {
						if v1 == 0 {
							return false, true
						} else {
							return true, true
						}
					} else if v1, ok1 := args[0].(float64); ok1 {
						if v1 == 0.0 {
							return false, true
						} else {
							return true, true
						}
					} else if v1, ok1 := args[0].(string); ok1 {
						if temp, err := strconv.ParseBool(v1); err == nil {
							return temp, true
						} else {
							return err, false
						}
					} else if v1, ok1 := args[0].(bool); ok1 {
						return v1, true
					} else {
						return fmt.Errorf("Not supported type conversion."), false
					}
				case "datetime":
					dt, err := cast.InterfaceToTime(args[0], "")
					if err != nil {
						return err, false
					} else {
						return dt, true
					}
				default:
					return fmt.Errorf("Unknow type, only support bigint, float, string, boolean and datetime."), false
				}
			} else {
				return fmt.Errorf("Expect string type for the 2nd parameter."), false
			}
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(2, len(args)); err != nil {
				return err
			}
			a := args[1]
			if !ast.IsStringArg(a) {
				return ProduceErrInfo(1, "string")
			}
			if av, ok := a.(*ast.StringLiteral); ok {
				if !(av.Val == "bigint" || av.Val == "float" || av.Val == "string" || av.Val == "boolean" || av.Val == "datetime") {
					return fmt.Errorf("Expect one of following value for the 2nd parameter: bigint, float, string, boolean, datetime.")
				}
			}
			return nil
		},
	}
	builtins["to_json"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if args[0] == nil {
				return "null", true
			}
			rr, err := json.Marshal(args[0])
			if err != nil {
				return fmt.Errorf("fail to convert %v to json", args[0]), false
			}
			return string(rr), true
		},
		val: ValidateOneArg,
	}
	builtins["parse_json"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if args[0] == nil || args[0] == "null" {
				return nil, true
			}
			text, err := cast.ToString(args[0], cast.CONVERT_SAMEKIND)
			if err != nil {
				return fmt.Errorf("fail to convert %v to string", args[0]), false
			}
			var data interface{}
			err = json.Unmarshal([]byte(text), &data)
			if err != nil {
				return fmt.Errorf("fail to parse json: %v", err), false
			}
			return data, true
		},
		val: ValidateOneStrArg,
	}
	builtins["chr"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, ok := args[0].(int); ok {
				return rune(v), true
			} else if v, ok := args[0].(float64); ok {
				temp := int(v)
				return rune(temp), true
			} else if v, ok := args[0].(string); ok {
				if len(v) > 1 {
					return fmt.Errorf("Parameter length cannot larger than 1."), false
				}
				r := []rune(v)
				return r[0], true
			} else {
				return fmt.Errorf("Only bigint, float and string type can be convert to char type."), false
			}
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}
			if ast.IsFloatArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "int")
			}
			return nil
		},
	}
	builtins["encode"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, ok := args[1].(string); ok {
				if strings.EqualFold(v, "base64") {
					if v1, ok1 := args[0].(string); ok1 {
						return b64.StdEncoding.EncodeToString([]byte(v1)), true
					} else {
						return fmt.Errorf("Only string type can be encoded."), false
					}
				} else {
					return fmt.Errorf("Only base64 encoding is supported."), false
				}
			}
			return nil, false
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(2, len(args)); err != nil {
				return err
			}

			if ast.IsNumericArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "string")
			}

			a := args[1]
			if !ast.IsStringArg(a) {
				return ProduceErrInfo(1, "string")
			}
			if av, ok := a.(*ast.StringLiteral); ok {
				if av.Val != "base64" {
					return fmt.Errorf("Only base64 is supported for the 2nd parameter.")
				}
			}
			return nil
		},
	}
	builtins["decode"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, ok := args[1].(string); ok {
				if strings.EqualFold(v, "base64") {
					if v1, ok1 := args[0].(string); ok1 {
						r, e := b64.StdEncoding.DecodeString(v1)
						if e != nil {
							return fmt.Errorf("fail to decode base64 string: %v", e), false
						}
						return r, true
					} else {
						return fmt.Errorf("Only string type can be decoded."), false
					}
				} else {
					return fmt.Errorf("Only base64 decoding is supported."), false
				}
			}
			return nil, false
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(2, len(args)); err != nil {
				return err
			}

			if ast.IsNumericArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "string")
			}

			a := args[1]
			if !ast.IsStringArg(a) {
				return ProduceErrInfo(1, "string")
			}
			if av, ok := a.(*ast.StringLiteral); ok {
				if av.Val != "base64" {
					return fmt.Errorf("Only base64 is supported for the 2nd parameter.")
				}
			}
			return nil
		},
	}
	builtins["trunc"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			var v0 float64
			if v1, ok := args[0].(int); ok {
				v0 = float64(v1)
			} else if v1, ok := args[0].(float64); ok {
				v0 = v1
			} else {
				return fmt.Errorf("Only int and float type can be truncated."), false
			}
			if v2, ok := args[1].(int); ok {
				return toFixed(v0, v2), true
			} else {
				return fmt.Errorf("The 2nd parameter must be int value."), false
			}
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(2, len(args)); err != nil {
				return err
			}

			if ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) || ast.IsStringArg(args[0]) {
				return ProduceErrInfo(0, "number - float or int")
			}

			if ast.IsFloatArg(args[1]) || ast.IsTimeArg(args[1]) || ast.IsBooleanArg(args[1]) || ast.IsStringArg(args[1]) {
				return ProduceErrInfo(1, "int")
			}
			return nil
		},
	}
	builtins["md5"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if args[0] == nil {
				return nil, true
			}
			arg0 := cast.ToStringAlways(args[0])
			h := md5.New()
			_, err := io.WriteString(h, arg0)
			if err != nil {
				return err, false
			}
			return fmt.Sprintf("%x", h.Sum(nil)), true
		},
		val: ValidateOneStrArg,
	}
	builtins["sha1"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if args[0] == nil {
				return nil, true
			}
			arg0 := cast.ToStringAlways(args[0])
			h := sha1.New()
			_, err := io.WriteString(h, arg0)
			if err != nil {
				return err, false
			}
			return fmt.Sprintf("%x", h.Sum(nil)), true
		},
		val: ValidateOneStrArg,
	}
	builtins["sha256"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if args[0] == nil {
				return nil, true
			}
			arg0 := cast.ToStringAlways(args[0])
			h := sha256.New()
			_, err := io.WriteString(h, arg0)
			if err != nil {
				return err, false
			}
			return fmt.Sprintf("%x", h.Sum(nil)), true
		},
		val: ValidateOneStrArg,
	}
	builtins["sha384"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if args[0] == nil {
				return nil, true
			}
			arg0 := cast.ToStringAlways(args[0])
			h := sha512.New384()
			_, err := io.WriteString(h, arg0)
			if err != nil {
				return err, false
			}
			return fmt.Sprintf("%x", h.Sum(nil)), true
		},
		val: ValidateOneStrArg,
	}
	builtins["sha512"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if args[0] == nil {
				return nil, true
			}
			arg0 := cast.ToStringAlways(args[0])
			h := sha512.New()
			_, err := io.WriteString(h, arg0)
			if err != nil {
				return err, false
			}
			return fmt.Sprintf("%x", h.Sum(nil)), true
		},
		val: ValidateOneStrArg,
	}
	builtinStatfulFuncs["compress"] = func() api.Function {
		conf.Log.Infof("initializing compress function")
		return &compressFunc{}
	}
	builtinStatfulFuncs["decompress"] = func() api.Function {
		conf.Log.Infof("initializing decompress function")
		return &decompressFunc{}
	}
	builtins["isnull"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if args[0] == nil {
				return true, true
			} else {
				v := reflect.ValueOf(args[0])
				switch v.Kind() {
				case reflect.Slice, reflect.Map:
					return v.IsNil(), true
				default:
					return false, true
				}
			}
		},
		val: ValidateOneArg,
	}
	builtins["coalesce"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			for _, arg := range args {
				if arg != nil {
					return arg, true
				}
			}
			return nil, true
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			if len(args) == 0 {
				return fmt.Errorf("The arguments should be at least one.")
			}
			return nil
		},
	}
	builtins["newuuid"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if newUUID, err := uuid.NewUUID(); err != nil {
				return err, false
			} else {
				return newUUID.String(), true
			}
		},
		val: ValidateNoArg,
	}
	builtins["tstamp"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			return conf.GetNowInMilli(), true
		},
		val: ValidateNoArg,
	}
	builtins["mqtt"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, ok := args[0].(string); ok {
				return v, true
			}
			return nil, false
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}
			if ast.IsIntegerArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsFloatArg(args[0]) {
				return ProduceErrInfo(0, "meta reference")
			}
			if p, ok := args[0].(*ast.MetaRef); ok {
				name := strings.ToLower(p.Name)
				if name != "topic" && name != "messageid" {
					return fmt.Errorf("Parameter of mqtt function can be only topic or messageid.")
				}
			}
			return nil
		},
	}
	builtins["meta"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			return args[0], true
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
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
			return ProduceErrInfo(0, "meta reference")
		},
	}
	builtins["cardinality"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			val := reflect.ValueOf(args[0])
			if val.Kind() == reflect.Slice {
				return val.Len(), true
			}
			return 0, true
		},
		val: ValidateOneArg,
	}
	builtins["json_path_query"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			result, err := jsonCall(ctx, args)
			if err != nil {
				return err, false
			}
			return result, true
		},
		val: ValidateJsonFunc,
	}
	builtins["json_path_query_first"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			result, err := jsonCall(ctx, args)
			if err != nil {
				return err, false
			}
			if arr, ok := result.([]interface{}); ok {
				return arr[0], true
			} else {
				return fmt.Errorf("query result (%v) is not an array", result), false
			}
		},
		val: ValidateJsonFunc,
	}
	builtins["json_path_exists"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			result, err := jsonCall(ctx, args)
			if err != nil {
				return false, true
			}
			if result == nil {
				return false, true
			}
			e := true
			switch reflect.TypeOf(result).Kind() {
			case reflect.Slice, reflect.Array:
				e = reflect.ValueOf(result).Len() > 0
			default:
				e = result != nil
			}
			return e, true
		},
		val: ValidateJsonFunc,
	}
	builtins["window_start"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec:  nil, // directly return in the valuer
		val:   ValidateNoArg,
	}
	builtins["window_end"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec:  nil, // directly return in the valuer
		val:   ValidateNoArg,
	}
	builtins["object_construct"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			result := make(map[string]interface{})
			for i := 0; i < len(args); i += 2 {
				if args[i+1] != nil {
					s, err := cast.ToString(args[i], cast.CONVERT_SAMEKIND)
					if err != nil {
						return fmt.Errorf("key %v is not a string", args[i]), false
					}
					result[s] = args[i+1]
				}
			}
			return result, true
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			if len(args)%2 != 0 {
				return fmt.Errorf("the args must be key value pairs")
			}
			for i, arg := range args {
				if i%2 == 0 {
					if ast.IsNumericArg(arg) || ast.IsTimeArg(arg) || ast.IsBooleanArg(arg) {
						return ProduceErrInfo(i, "string")
					}
				}
			}
			return nil
		},
	}

}

func round(num float64) int {
	return int(num + math.Copysign(0.5, num))
}

func toFixed(num float64, precision int) float64 {
	output := math.Pow(10, float64(precision))
	return float64(round(num*output)) / output
}

func jsonCall(ctx api.StreamContext, args []interface{}) (interface{}, error) {
	jp, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("invalid jsonPath, must be a string but got %v", args[1])
	}
	return ctx.ParseJsonPath(jp, args[0])
}

// page Rotate storage for in memory cache
// Not thread safe!
type ringqueue struct {
	data []interface{}
	h    int
	t    int
	l    int
	size int
}

func newRingqueue(size int) *ringqueue {
	return &ringqueue{
		data: make([]interface{}, size),
		h:    0, // When deleting, head++, if tail == head, it is empty
		t:    0, // When append, tail++, if tail== head, it is full
		size: size,
	}
}

// fill item will fill the queue with item value
func (p *ringqueue) fill(item interface{}) {
	for {
		if !p.append(item) {
			return
		}
	}
}

// append item if list is not full and return true; otherwise return false
func (p *ringqueue) append(item interface{}) bool {
	if p.l == p.size { // full
		return false
	}
	p.data[p.t] = item
	p.t++
	if p.t == p.size {
		p.t = 0
	}
	p.l++
	return true
}

// fetch get the first item in the cache and remove
func (p *ringqueue) fetch() (interface{}, bool) {
	if p.l == 0 {
		return nil, false
	}
	result := p.data[p.h]
	p.h++
	if p.h == p.size {
		p.h = 0
	}
	p.l--
	return result, true
}

// peek get the first item in the cache but keep it
func (p *ringqueue) peek() (interface{}, bool) {
	if p.l == 0 {
		return nil, false
	}
	result := p.data[p.h]
	return result, true
}

func (p *ringqueue) isFull() bool {
	return p.l == p.size
}
