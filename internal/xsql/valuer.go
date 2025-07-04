// Copyright 2022-2025 EMQ Technologies Co., Ltd.
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
	"reflect"
	"regexp"
	"time"

	"github.com/lf-edge/ekuiper/v2/internal/binder/function"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

var (
	// implicitValueFuncs is a set of functions that event implicitly passes the value.
	implicitValueFuncs = map[string]bool{
		"window_start":   true,
		"window_end":     true,
		"event_time":     true,
		"window_trigger": true,
	}
	// ImplicitStateFuncs is a set of functions that read/update global state implicitly.
	ImplicitStateFuncs = map[string]bool{
		"last_hit_time":      true,
		"last_hit_count":     true,
		"last_agg_hit_time":  true,
		"last_agg_hit_count": true,
	}
)

/*
 *  Valuer definitions
 */

// Valuer is the interface that wraps the Value() method.
type Valuer interface {
	// Value returns the value and existence flag for a given key.
	Value(key, table string) (interface{}, bool)
	Meta(key, table string) (interface{}, bool)
}

// AliasValuer is used to calculate and cache the alias value
type AliasValuer interface {
	// AliasValue Get the value of alias
	AliasValue(name string) (interface{}, bool)
	// AppendAlias set the alias result
	AppendAlias(key string, value interface{}) bool
}

// CallValuer implements the Call method for evaluating function calls.
type CallValuer interface {
	Valuer

	// Call is invoked to evaluate a function call (if possible).
	Call(name string, funcId int, args []interface{}) (interface{}, bool)
}

// FuncValuer can calculate function type value like window_start and window_end
type FuncValuer interface {
	FuncValue(key string) (interface{}, bool)
}

type AggregateCallValuer interface {
	CallValuer
	GetAllTuples() AggregateData
	GetSingleCallValuer() CallValuer
}

type WildcardValuer struct {
	Data Wildcarder
}

func (wv *WildcardValuer) Value(key, table string) (interface{}, bool) {
	if key == "*" {
		return wv.Data.All(table)
	}
	return nil, false
}

func (wv *WildcardValuer) Meta(_, _ string) (interface{}, bool) {
	return nil, false
}

// MultiValuer returns a Valuer that iterates over multiple Valuer instances
// to find a match.
func MultiValuer(valuers ...Valuer) Valuer {
	return multiValuer(valuers)
}

type multiValuer []Valuer

func (a multiValuer) Value(key, table string) (interface{}, bool) {
	for _, valuer := range a {
		if v, ok := valuer.Value(key, table); ok {
			return v, true
		}
	}
	return nil, false
}

func (a multiValuer) Meta(key, table string) (interface{}, bool) {
	for _, valuer := range a {
		if v, ok := valuer.Meta(key, table); ok {
			return v, true
		}
	}
	return nil, false
}

func (a multiValuer) AppendAlias(key string, value interface{}) bool {
	for _, valuer := range a {
		if vv, ok := valuer.(AliasValuer); ok {
			if ok := vv.AppendAlias(key, value); ok {
				return true
			}
		}
	}
	return false
}

func (a multiValuer) AliasValue(key string) (interface{}, bool) {
	for _, valuer := range a {
		if vv, ok := valuer.(AliasValuer); ok {
			return vv.AliasValue(key)
		}
	}
	return nil, false
}

func (a multiValuer) FuncValue(key string) (interface{}, bool) {
	for _, valuer := range a {
		if vv, ok := valuer.(FuncValuer); ok {
			if r, ok := vv.FuncValue(key); ok {
				return r, true
			}
		}
	}
	return nil, false
}

func (a multiValuer) Call(name string, funcId int, args []interface{}) (interface{}, bool) {
	for _, valuer := range a {
		if valuer, ok := valuer.(CallValuer); ok {
			if v, ok := valuer.Call(name, funcId, args); ok {
				return v, true
			} else {
				return fmt.Errorf("call func %s error: %v", name, v), false
			}
		}
	}
	return nil, false
}

func (a multiValuer) ValueByIndex(index int, sourceIndex int) (any, bool) {
	for _, valuer := range a {
		if iv, ok := valuer.(model.IndexValuer); ok {
			return iv.ValueByIndex(index, sourceIndex)
		}
	}
	return nil, false
}

func (a multiValuer) SetByIndex(index int, value any) {
	for _, valuer := range a {
		if iv, ok := valuer.(model.IndexValuer); ok {
			iv.SetByIndex(index, value)
			return
		}
	}
	panic("implement me")
}

func (a multiValuer) TempByIndex(index int) any {
	for _, valuer := range a {
		if iv, ok := valuer.(model.IndexValuer); ok {
			return iv.TempByIndex(index)
		}
	}
	return nil
}

func (a multiValuer) SetTempByIndex(index int, value any) {
	for _, valuer := range a {
		if iv, ok := valuer.(model.IndexValuer); ok {
			iv.SetTempByIndex(index, value)
			return
		}
	}
	panic("implement me")
}

type multiAggregateValuer struct {
	data AggregateData
	multiValuer
	singleCallValuer CallValuer
}

func MultiAggregateValuer(data AggregateData, singleCallValuer CallValuer, valuers ...Valuer) Valuer {
	return &multiAggregateValuer{
		data:             data,
		multiValuer:      valuers,
		singleCallValuer: singleCallValuer,
	}
}

func (a *multiAggregateValuer) Call(name string, funcId int, args []interface{}) (interface{}, bool) {
	// assume the aggFuncMap already cache the custom agg funcs in IsAggFunc()
	isAgg := function.IsAggFunc(name)
	for _, valuer := range a.multiValuer {
		if a, ok := valuer.(AggregateCallValuer); ok && isAgg {
			if v, ok := a.Call(name, funcId, args); ok {
				return v, true
			} else {
				return fmt.Errorf("call func %s error: %v", name, v), false
			}
		} else if c, ok := valuer.(CallValuer); ok && !isAgg {
			if v, ok := c.Call(name, funcId, args); ok {
				return v, true
			}
		}
	}
	return nil, false
}

func (a *multiAggregateValuer) GetAllTuples() AggregateData {
	return a.data
}

func (a *multiAggregateValuer) GetSingleCallValuer() CallValuer {
	return a.singleCallValuer
}

func (a *multiAggregateValuer) AppendAlias(key string, value interface{}) bool {
	if vv, ok := a.data.(AliasValuer); ok {
		if ok := vv.AppendAlias(key, value); ok {
			return true
		}
		return false
	} else {
		return a.multiValuer.AppendAlias(key, value)
	}
}

func (a *multiAggregateValuer) AliasValue(key string) (interface{}, bool) {
	if vv, ok := a.data.(AliasValuer); ok {
		return vv.AliasValue(key)
	} else {
		return a.multiValuer.AliasValue(key)
	}
}

/*
 * Eval Logics
 */

// Eval evaluates expr against a map.
func Eval(expr ast.Expr, m Valuer) interface{} {
	eval := ValuerEval{Valuer: m}
	return eval.Eval(expr)
}

// ValuerEval will evaluate an expression using the Valuer.
type ValuerEval struct {
	Valuer Valuer

	// IntegerFloatDivision will set the eval system to treat
	// a division between two integers as a floating point division.
	IntegerFloatDivision bool
}

// Eval evaluates an expression and returns a value.
// map the expression to the correct valuer
func (v *ValuerEval) Eval(expr ast.Expr) interface{} {
	if expr == nil {
		return nil
	}
	switch et := expr.(type) {
	case *ast.BinaryExpr:
		return v.evalBinaryExpr(et)
	case *ast.IntegerLiteral:
		return et.Val
	case *ast.NumberLiteral:
		return et.Val
	case *ast.ParenExpr:
		return v.Eval(et.Expr)
	case *ast.StringLiteral:
		return et.Val
	case *ast.BooleanLiteral:
		return et.Val
	case *ast.ColonExpr:
		s, e := v.Eval(et.Start), v.Eval(et.End)
		si, err := cast.ToInt(s, cast.CONVERT_SAMEKIND)
		if err != nil {
			return fmt.Errorf("colon start %v is not int: %v", et.Start, err)
		}
		ei, err := cast.ToInt(e, cast.CONVERT_SAMEKIND)
		if err != nil {
			return fmt.Errorf("colon end %v is not int: %v", et.End, err)
		}
		return &BracketEvalResult{Start: si, End: ei}
	case *ast.IndexExpr:
		i := v.Eval(et.Index)
		ii, err := cast.ToInt(i, cast.CONVERT_SAMEKIND)
		if err != nil {
			return fmt.Errorf("index %v is not int: %v", et.Index, err)
		}
		return &BracketEvalResult{Start: ii, End: ii}
	case *ast.Call:
		// The analytic functions are calculated prior to all ops, so just get the cached field value
		if et.Cached && et.CachedField != "" {
			var val any
			if et.CacheIndex >= 0 {
				if iv, ok := v.Valuer.(model.IndexValuer); ok {
					val = iv.TempByIndex(et.CacheIndex)
				} else {
					return fmt.Errorf("cannot calculate cacheIndex for %s", et.CachedField)
				}
			} else {
				val, _ = v.Valuer.Value(et.CachedField, "")
			}
			// nil is also cached
			return val
		}
		if _, ok := implicitValueFuncs[et.Name]; ok {
			if vv, ok := v.Valuer.(FuncValuer); ok {
				val, ok := vv.FuncValue(et.Name)
				if ok {
					return val
				}
			}
		} else {
			if valuer, ok := v.Valuer.(CallValuer); ok {
				var (
					args []interface{}
					ft   = et.FuncType
				)
				if _, ok := ImplicitStateFuncs[et.Name]; ok {
					args = make([]interface{}, 1)
					// This is the implicit arg set by the filter planner
					// If set, it will only return the value, no updating the value
					if et.Cached {
						args[0] = false
					} else {
						args[0] = true
					}
					if et.Name == "last_hit_time" || et.Name == "last_agg_hit_time" {
						if vv, ok := v.Valuer.(FuncValuer); ok {
							val, ok := vv.FuncValue("event_time")
							if ok {
								args = append(args, val)
							} else {
								return fmt.Errorf("call %s error: %v", et.Name, val)
							}
						} else {
							return fmt.Errorf("call %s error: %v", et.Name, "cannot get current time")
						}
					}
					val, _ := valuer.Call(et.Name, et.FuncId, args)
					return val
				}
				if len(et.Args) > 0 {
					switch ft {
					case ast.FuncTypeAgg:
						args = make([]interface{}, len(et.Args))
						for i, arg := range et.Args {
							if aggreValuer, ok := valuer.(AggregateCallValuer); ok {
								args[i] = aggreValuer.GetAllTuples().AggregateEval(arg, aggreValuer.GetSingleCallValuer())
							} else {
								args[i] = v.Eval(arg)
								if _, ok := args[i].(error); ok {
									return args[i]
								}
							}
						}
					case ast.FuncTypeScalar, ast.FuncTypeSrf:
						args = make([]interface{}, len(et.Args))
						for i, arg := range et.Args {
							args[i] = v.Eval(arg)
							if _, ok := args[i].(error); ok {
								return args[i]
							}
						}
					case ast.FuncTypeCols:
						var keys []string
						for _, arg := range et.Args { // In the parser, the col func arguments must be ColField
							cf, ok := arg.(*ast.ColFuncField)
							if !ok {
								// won't happen
								return fmt.Errorf("expect colFuncField but got %v", arg)
							}
							temp := v.Eval(cf.Expr)
							if _, ok := temp.(error); ok {
								return temp
							}
							switch cf.Expr.(type) {
							case *ast.Wildcard:
								m, ok := temp.(map[string]interface{})
								if !ok {
									return fmt.Errorf("wildcarder return non map result")
								}
								for kk, vv := range m {
									args = append(args, vv)
									keys = append(keys, kk)
								}
							default:
								args = append(args, temp)
								keys = append(keys, cf.Name)
							}
						}
						args = append(args, keys)
					default:
						// won't happen
						return fmt.Errorf("unknown function type")
					}
				}
				if function.IsAnalyticFunc(et.Name) {
					// this data should be recorded or not ? default answer is yes
					if et.WhenExpr != nil {
						validData := true
						temp := v.Eval(et.WhenExpr)
						whenExprVal, ok := temp.(bool)
						if ok {
							validData = whenExprVal
						}

						args = append(args, validData)
					} else {
						args = append(args, true)
					}

					// analytic func must put the partition key into the args
					if et.Partition != nil && len(et.Partition.Exprs) > 0 {
						pk := ""
						for _, pe := range et.Partition.Exprs {
							temp := v.Eval(pe)
							if _, ok := temp.(error); ok {
								return temp
							}
							pk += fmt.Sprintf("%v", temp)
						}
						args = append(args, pk)
					} else {
						args = append(args, "self")
					}
				}
				val, _ := valuer.Call(et.Name, et.FuncId, args)
				return val
			}
		}
		return nil
	case *ast.FieldRef:
		if et.HasIndex {
			if indexValuer, ok := v.Valuer.(model.IndexValuer); ok {
				val, ok := indexValuer.ValueByIndex(et.Index, et.SourceIndex)
				if !ok {
					r := v.Eval(et.Expression)
					indexValuer.SetByIndex(et.Index, r)
					val = r
				}
				return val
			}
		}

		var t, n string
		if et.IsAlias() {
			if valuer, ok := v.Valuer.(AliasValuer); ok {
				val, ok := valuer.AliasValue(et.Name)
				if ok {
					return val
				} else {
					r := v.Eval(et.Expression)
					// TODO possible performance elevation to eliminate this cal
					valuer.AppendAlias(et.Name, r)
					return r
				}
			}
		} else if et.StreamName == ast.DefaultStream {
			n = et.Name
		} else {
			t = string(et.StreamName)
			n = et.Name
		}
		if n != "" {
			val, ok := v.Valuer.Value(n, t)
			if ok {
				return val
			}
		}
		return nil
	case *ast.MetaRef:
		if et.StreamName == "" || et.StreamName == ast.DefaultStream {
			val, _ := v.Valuer.Meta(et.Name, "")
			return val
		} else {
			// The field specified with stream source
			val, _ := v.Valuer.Meta(et.Name, string(et.StreamName))
			return val
		}
	case *ast.JsonFieldRef:
		val, ok := v.Valuer.Value(et.Name, "")
		if ok {
			return val
		} else {
			return nil
		}
	case *ast.Wildcard:
		all, _ := v.Valuer.Value("*", "")
		al, ok := all.(map[string]interface{})
		if !ok {
			return fmt.Errorf("unexpected wildcard value %v", all)
		}
		val := make(map[string]interface{})
		for k, v := range al {
			if !contains(et.Except, k) {
				val[k] = v
			}
		}
		for _, field := range et.Replace {
			vi := v.Eval(field.Expr)
			if e, ok := vi.(error); ok {
				return e
			}
			val[field.AName] = vi
		}
		return val
	case *ast.CaseExpr:
		return v.evalCase(et)
	case *ast.ValueSetExpr:
		return v.evalValueSet(et)
	case *ast.BetweenExpr:
		lower := v.Eval(et.Lower)
		higher := v.Eval(et.Higher)
		if lower == nil || higher == nil {
			return nil
		}
		return []interface{}{
			v.Eval(et.Lower), v.Eval(et.Higher),
		}
	case *ast.LikePattern:
		if et.Pattern != nil {
			return et.Pattern
		}
		v := v.Eval(et.Expr)
		str, ok := v.(string)
		if !ok {
			return fmt.Errorf("invalid LIKE pattern, must be a string but got %v", v)
		}
		re, err := et.Compile(str)
		if err != nil {
			return err
		}
		return re
	default:
		return nil
	}
}

func (v *ValuerEval) evalBinaryExpr(expr *ast.BinaryExpr) interface{} {
	lhs := v.Eval(expr.LHS)
	switch val := lhs.(type) {
	case map[string]interface{}:
		return v.evalJsonExpr(val, expr.OP, expr.RHS)
	case Message:
		return v.evalJsonExpr(map[string]interface{}(val), expr.OP, expr.RHS)
	case error:
		return val
	}
	// shortcut for bool
	switch expr.OP {
	case ast.AND:
		if bv, ok := lhs.(bool); ok && !bv {
			return false
		}
	case ast.OR:
		if bv, ok := lhs.(bool); ok && bv {
			return true
		}
	}
	if isSliceOrArray(lhs) {
		return v.evalJsonExpr(lhs, expr.OP, expr.RHS)
	}
	rhs := v.Eval(expr.RHS)
	if _, ok := rhs.(error); ok {
		return rhs
	}
	if isSetOperator(expr.OP) {
		return v.evalSetsExpr(lhs, expr.OP, rhs)
	}
	switch expr.OP {
	case ast.BETWEEN, ast.NOTBETWEEN:
		if lhs == nil || rhs == nil {
			return false
		}
		arr, ok := rhs.([]interface{})
		if !ok {
			return fmt.Errorf("between operator expects two arguments, but found %v", rhs)
		}
		andLeft := v.SimpleDataEval(lhs, arr[0], ast.GTE)
		switch andLeft.(type) {
		case error:
			return fmt.Errorf("between operator cannot compare %[1]T(%[1]v) and %[2]T(%[2]v)", lhs, arr[0])
		}
		andRight := v.SimpleDataEval(lhs, arr[1], ast.LTE)
		switch andRight.(type) {
		case error:
			return fmt.Errorf("between operator cannot compare %[1]T(%[1]v) and %[2]T(%[2]v)", lhs, arr[1])
		}
		r := v.SimpleDataEval(andLeft, andRight, ast.AND)
		br, ok := r.(bool)
		if expr.OP == ast.NOTBETWEEN && ok {
			return !br
		} else {
			return r
		}
	case ast.LIKE, ast.NOTLIKE:
		ls, ok := lhs.(string)
		if !ok {
			return fmt.Errorf("LIKE operator left operand expects string, but found %v", lhs)
		}
		var result bool
		rs, ok := rhs.(*regexp.Regexp)
		if !ok {
			return fmt.Errorf("LIKE operator right operand expects string, but found %v", rhs)
		}
		result = rs.MatchString(ls)
		if expr.OP == ast.NOTLIKE {
			result = !result
		}
		return result
	default:
		return v.SimpleDataEval(lhs, rhs, expr.OP)
	}
}

func (v *ValuerEval) evalCase(expr *ast.CaseExpr) interface{} {
	if expr.Value != nil { // compare value to all when clause
		ev := v.Eval(expr.Value)
		for _, w := range expr.WhenClauses {
			wv := v.Eval(w.Expr)
			switch r := v.SimpleDataEval(ev, wv, ast.EQ).(type) {
			case error:
				return fmt.Errorf("evaluate case expression error: %s", r)
			case bool:
				if r {
					return v.Eval(w.Result)
				}
			}
		}
	} else {
		for _, w := range expr.WhenClauses {
			switch r := v.Eval(w.Expr).(type) {
			case error:
				return fmt.Errorf("evaluate case expression error: %s", r)
			case bool:
				if r {
					return v.Eval(w.Result)
				}
			}
		}
	}
	if expr.ElseClause != nil {
		return v.Eval(expr.ElseClause)
	}
	return nil
}

func (v *ValuerEval) evalValueSet(expr *ast.ValueSetExpr) interface{} {
	var valueSet []interface{}

	if expr.LiteralExprs != nil {
		for _, exp := range expr.LiteralExprs {
			valueSet = append(valueSet, v.Eval(exp))
		}
		return valueSet
	}

	value := v.Eval(expr.ArrayExpr)
	if isSliceOrArray(value) {
		return value
	}
	return nil
}

func (v *ValuerEval) evalSetsExpr(lhs interface{}, op ast.Token, rhsSet interface{}) interface{} {
	switch op {
	/*Semantic rules

	When using the IN operator, the following semantics apply in this order:

	Returns FALSE if value_set is empty.
	Returns NULL if search_value is NULL.
	Returns TRUE if value_set contains a value equal to search_value.
	Returns NULL if value_set contains a NULL.
	Returns FALSE.
	When using the NOT IN operator, the following semantics apply in this order:

	Returns TRUE if value_set is empty.
	Returns NULL if search_value is NULL.
	Returns FALSE if value_set contains a value equal to search_value.
	Returns NULL if value_set contains a NULL.
	Returns TRUE.
	*/
	case ast.IN, ast.NOTIN:
		if rhsSet == nil {
			if op == ast.IN {
				return false
			} else {
				return true
			}
		}
		if lhs == nil {
			return false
		}
		rhsSetVals := reflect.ValueOf(rhsSet)
		for i := 0; i < rhsSetVals.Len(); i++ {
			switch r := v.SimpleDataEval(lhs, rhsSetVals.Index(i).Interface(), ast.EQ).(type) {
			case error:
				return fmt.Errorf("evaluate in expression error: %s", r)
			case bool:
				if r {
					if op == ast.IN {
						return true
					} else {
						return false
					}
				}
			}
		}
		if op == ast.IN {
			return false
		} else {
			return true
		}
	default:
		return fmt.Errorf("%v is an invalid operation for %T", op, lhs)
	}
}

func (v *ValuerEval) evalJsonExpr(result interface{}, op ast.Token, expr ast.Expr) interface{} {
	switch op {
	case ast.ARROW:
		if val, ok := result.(map[string]interface{}); ok {
			switch e := expr.(type) {
			case *ast.JsonFieldRef:
				ve := &ValuerEval{Valuer: Message(val)}
				return ve.Eval(e)
			default:
				return fmt.Errorf("the right expression is not a field reference node")
			}
		} else {
			return fmt.Errorf("the result %v is not a type of map[string]interface{}", result)
		}
	case ast.SUBSET:
		if isSliceOrArray(result) {
			return v.subset(result, expr)
		} else {
			return fmt.Errorf("%v is an invalid operation for %T", op, result)
		}
	default:
		return fmt.Errorf("%v is an invalid operation for %T", op, result)
	}
}

func (v *ValuerEval) subset(result interface{}, expr ast.Expr) interface{} {
	val := reflect.ValueOf(result)
	ber := v.Eval(expr)
	if berVal, ok1 := ber.(*BracketEvalResult); ok1 {
		if berVal.isIndex() {
			if 0 > berVal.Start {
				if 0 > berVal.Start+val.Len() {
					return fmt.Errorf("out of index: %d of %d", berVal.Start, val.Len())
				}
				berVal.Start += val.Len()
			} else if berVal.Start >= val.Len() {
				return fmt.Errorf("out of index: %d of %d", berVal.Start, val.Len())
			}
			return val.Index(berVal.Start).Interface()
		} else {
			if 0 > berVal.Start {
				if 0 > berVal.Start+val.Len() {
					return fmt.Errorf("out of index: %d of %d", berVal.Start, val.Len())
				}
				berVal.Start += val.Len()
			} else if berVal.Start >= val.Len() {
				return fmt.Errorf("start value is out of index: %d of %d", berVal.Start, val.Len())
			}
			if math.MinInt32 == berVal.End {
				berVal.End = val.Len()
			} else if 0 > berVal.End {
				if 0 > berVal.End+val.Len() {
					return fmt.Errorf("out of index: %d of %d", berVal.End, val.Len())
				}
				berVal.End += val.Len()
			} else if berVal.End > val.Len() {
				return fmt.Errorf("end value is out of index: %d of %d", berVal.End, val.Len())
			} else if berVal.Start >= berVal.End {
				return fmt.Errorf("start cannot be greater than end. start:%d  end:%d", berVal.Start, berVal.End)
			}
			return val.Slice(berVal.Start, berVal.End).Interface()
		}
	} else {
		return fmt.Errorf("invalid evaluation result - %v", berVal)
	}
}

// SimpleDataEval lhs and rhs are non-nil
func (v *ValuerEval) SimpleDataEval(lhs, rhs any, op ast.Token) any {
	if lhs == nil || rhs == nil {
		// for relationship, return false
		switch op {
		case ast.AND, ast.OR, ast.BITWISE_AND, ast.BITWISE_OR, ast.BITWISE_XOR, ast.EQ, ast.NEQ, ast.GT, ast.GTE, ast.LT, ast.LTE:
			return false
		default:
			return nil
		}
	}
	lhs = convertNum(lhs)
	rhs = convertNum(rhs)
	// Evaluate if both sides are simple types.
	switch lhs := lhs.(type) {
	case bool:
		originRHS := rhs
		rhs, ok := originRHS.(bool)
		if !ok {
			return invalidOpError(lhs, op, originRHS)
		}
		switch op {
		case ast.AND:
			return lhs && rhs
		case ast.OR:
			return lhs || rhs
		case ast.BITWISE_AND:
			return lhs && rhs
		case ast.BITWISE_OR:
			return lhs || rhs
		case ast.BITWISE_XOR:
			return lhs != rhs
		case ast.EQ:
			return lhs == rhs
		case ast.NEQ:
			return lhs != rhs
		default:
			return invalidOpError(lhs, op, rhs)
		}
	case float64:
		// Try the rhs as a float64, int64, or uint64
		rhsf, ok := rhs.(float64)
		if !ok {
			switch val := rhs.(type) {
			case int64:
				rhsf, ok = float64(val), true
			case uint64:
				rhsf, ok = float64(val), true
			}
		}
		if !ok {
			return invalidOpError(lhs, op, rhs)
		}
		rhs := rhsf
		switch op {
		case ast.EQ:
			return lhs == rhs
		case ast.NEQ:
			return lhs != rhs
		case ast.LT:
			return lhs < rhs
		case ast.LTE:
			return lhs <= rhs
		case ast.GT:
			return lhs > rhs
		case ast.GTE:
			return lhs >= rhs
		case ast.ADD:
			return lhs + rhs
		case ast.SUB:
			return lhs - rhs
		case ast.MUL:
			return lhs * rhs
		case ast.DIV:
			if rhs == 0 {
				return fmt.Errorf("divided by zero")
			}
			return lhs / rhs
		case ast.MOD:
			if rhs == 0 {
				return fmt.Errorf("divided by zero")
			}
			return math.Mod(lhs, rhs)
		default:
			return invalidOpError(lhs, op, rhs)
		}
	case int64:
		// Try as a float64 to see if a float cast is required.
		switch rhs := rhs.(type) {
		case float64:
			lhs := float64(lhs)
			switch op {
			case ast.EQ:
				return lhs == rhs
			case ast.NEQ:
				return lhs != rhs
			case ast.LT:
				return lhs < rhs
			case ast.LTE:
				return lhs <= rhs
			case ast.GT:
				return lhs > rhs
			case ast.GTE:
				return lhs >= rhs
			case ast.ADD:
				return lhs + rhs
			case ast.SUB:
				return lhs - rhs
			case ast.MUL:
				return lhs * rhs
			case ast.DIV:
				if rhs == 0 {
					return fmt.Errorf("divided by zero")
				}
				return lhs / rhs
			case ast.MOD:
				if rhs == 0 {
					return fmt.Errorf("divided by zero")
				}
				return math.Mod(lhs, rhs)
			default:
				return invalidOpError(lhs, op, rhs)
			}
		case int64:
			switch op {
			case ast.EQ:
				return lhs == rhs
			case ast.NEQ:
				return lhs != rhs
			case ast.LT:
				return lhs < rhs
			case ast.LTE:
				return lhs <= rhs
			case ast.GT:
				return lhs > rhs
			case ast.GTE:
				return lhs >= rhs
			case ast.ADD:
				return lhs + rhs
			case ast.SUB:
				return lhs - rhs
			case ast.MUL:
				return lhs * rhs
			case ast.DIV:
				if v.IntegerFloatDivision {
					if rhs == 0 {
						return fmt.Errorf("divided by zero")
					}
					return float64(lhs) / float64(rhs)
				}

				if rhs == 0 {
					return fmt.Errorf("divided by zero")
				}
				return lhs / rhs
			case ast.MOD:
				if rhs == 0 {
					return fmt.Errorf("divided by zero")
				}
				return lhs % rhs
			case ast.BITWISE_AND:
				return lhs & rhs
			case ast.BITWISE_OR:
				return lhs | rhs
			case ast.BITWISE_XOR:
				return lhs ^ rhs
			default:
				return invalidOpError(lhs, op, rhs)
			}
		case uint64:
			switch op {
			case ast.EQ:
				return uint64(lhs) == rhs
			case ast.NEQ:
				return uint64(lhs) != rhs
			case ast.LT:
				if lhs < 0 {
					return true
				}
				return uint64(lhs) < rhs
			case ast.LTE:
				if lhs < 0 {
					return true
				}
				return uint64(lhs) <= rhs
			case ast.GT:
				if lhs < 0 {
					return false
				}
				return uint64(lhs) > rhs
			case ast.GTE:
				if lhs < 0 {
					return false
				}
				return uint64(lhs) >= rhs
			case ast.ADD:
				return uint64(lhs) + rhs
			case ast.SUB:
				return uint64(lhs) - rhs
			case ast.MUL:
				return uint64(lhs) * rhs
			case ast.DIV:
				if rhs == 0 {
					return fmt.Errorf("divided by zero")
				}
				return uint64(lhs) / rhs
			case ast.MOD:
				if rhs == 0 {
					return fmt.Errorf("divided by zero")
				}
				return uint64(lhs) % rhs
			case ast.BITWISE_AND:
				return uint64(lhs) & rhs
			case ast.BITWISE_OR:
				return uint64(lhs) | rhs
			case ast.BITWISE_XOR:
				return uint64(lhs) ^ rhs
			default:
				return invalidOpError(lhs, op, rhs)
			}
		default:
			return invalidOpError(lhs, op, rhs)
		}
	case uint64:
		// Try as a float64 to see if a float cast is required.
		switch rhs := rhs.(type) {
		case float64:
			lhs := float64(lhs)
			switch op {
			case ast.EQ:
				return lhs == rhs
			case ast.NEQ:
				return lhs != rhs
			case ast.LT:
				return lhs < rhs
			case ast.LTE:
				return lhs <= rhs
			case ast.GT:
				return lhs > rhs
			case ast.GTE:
				return lhs >= rhs
			case ast.ADD:
				return lhs + rhs
			case ast.SUB:
				return lhs - rhs
			case ast.MUL:
				return lhs * rhs
			case ast.DIV:
				if rhs == 0 {
					return fmt.Errorf("divided by zero")
				}
				return lhs / rhs
			case ast.MOD:
				if rhs == 0 {
					return fmt.Errorf("divided by zero")
				}
				return math.Mod(lhs, rhs)
			default:
				return invalidOpError(lhs, op, rhs)
			}
		case int64:
			switch op {
			case ast.EQ:
				return lhs == uint64(rhs)
			case ast.NEQ:
				return lhs != uint64(rhs)
			case ast.LT:
				if rhs < 0 {
					return false
				}
				return lhs < uint64(rhs)
			case ast.LTE:
				if rhs < 0 {
					return false
				}
				return lhs <= uint64(rhs)
			case ast.GT:
				if rhs < 0 {
					return true
				}
				return lhs > uint64(rhs)
			case ast.GTE:
				if rhs < 0 {
					return true
				}
				return lhs >= uint64(rhs)
			case ast.ADD:
				return lhs + uint64(rhs)
			case ast.SUB:
				return lhs - uint64(rhs)
			case ast.MUL:
				return lhs * uint64(rhs)
			case ast.DIV:
				if rhs == 0 {
					return fmt.Errorf("divided by zero")
				}
				return lhs / uint64(rhs)
			case ast.MOD:
				if rhs == 0 {
					return fmt.Errorf("divided by zero")
				}
				return lhs % uint64(rhs)
			case ast.BITWISE_AND:
				return lhs & uint64(rhs)
			case ast.BITWISE_OR:
				return lhs | uint64(rhs)
			case ast.BITWISE_XOR:
				return lhs ^ uint64(rhs)
			default:
				return invalidOpError(lhs, op, rhs)
			}
		case uint64:
			switch op {
			case ast.EQ:
				return lhs == rhs
			case ast.NEQ:
				return lhs != rhs
			case ast.LT:
				return lhs < rhs
			case ast.LTE:
				return lhs <= rhs
			case ast.GT:
				return lhs > rhs
			case ast.GTE:
				return lhs >= rhs
			case ast.ADD:
				return lhs + rhs
			case ast.SUB:
				return lhs - rhs
			case ast.MUL:
				return lhs * rhs
			case ast.DIV:
				if rhs == 0 {
					return fmt.Errorf("divided by zero")
				}
				return lhs / rhs
			case ast.MOD:
				if rhs == 0 {
					return fmt.Errorf("divided by zero")
				}
				return lhs % rhs
			case ast.BITWISE_AND:
				return lhs & rhs
			case ast.BITWISE_OR:
				return lhs | rhs
			case ast.BITWISE_XOR:
				return lhs ^ rhs
			default:
				return invalidOpError(lhs, op, rhs)
			}
		default:
			return invalidOpError(lhs, op, rhs)
		}
	case string:
		rhss, ok := rhs.(string)
		if !ok {
			return invalidOpError(lhs, op, rhs)
		}
		switch op {
		case ast.EQ:
			return lhs == rhss
		case ast.NEQ:
			return lhs != rhss
		case ast.LT:
			return lhs < rhss
		case ast.LTE:
			return lhs <= rhss
		case ast.GT:
			return lhs > rhss
		case ast.GTE:
			return lhs >= rhss
		default:
			return invalidOpError(lhs, op, rhs)
		}
	case time.Time:
		rt, err := cast.InterfaceToTime(rhs, "")
		if err != nil {
			return invalidOpError(lhs, op, rhs)
		}
		switch op {
		case ast.EQ:
			return lhs.Equal(rt)
		case ast.NEQ:
			return !lhs.Equal(rt)
		case ast.LT:
			return lhs.Before(rt)
		case ast.LTE:
			return lhs.Before(rt) || lhs.Equal(rt)
		case ast.GT:
			return lhs.After(rt)
		case ast.GTE:
			return lhs.After(rt) || lhs.Equal(rt)
		default:
			return invalidOpError(lhs, op, rhs)
		}
	default:
		return invalidOpError(lhs, op, rhs)
	}
}

/*
 * Helper functions
 */

type BracketEvalResult struct {
	Start, End int
}

func (ber *BracketEvalResult) isIndex() bool {
	return ber.Start == ber.End
}

func isSliceOrArray(v interface{}) bool {
	kind := reflect.ValueOf(v).Kind()
	return kind == reflect.Array || kind == reflect.Slice
}

func isSetOperator(op ast.Token) bool {
	return op == ast.IN || op == ast.NOTIN
}

func invalidOpError(lhs interface{}, op ast.Token, rhs interface{}) error {
	return fmt.Errorf("invalid operation %[1]T(%[1]v) %s %[3]T(%[3]v)", lhs, ast.Tokens[op], rhs)
}

func convertNum(para interface{}) interface{} {
	if isInt(para) {
		// Already check type of para so that there will be no error, just ignore error
		para, _ = cast.ToInt64(para, cast.CONVERT_SAMEKIND)
	} else if isFloat(para) {
		para, _ = cast.ToFloat64(para, cast.CONVERT_SAMEKIND)
	}
	return para
}

func isInt(para interface{}) bool {
	switch para.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	}
	return false
}

func isFloat(para interface{}) bool {
	switch para.(type) {
	case float32, float64:
		return true
	}
	return false
}
