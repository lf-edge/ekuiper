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
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"
)

var implicitValueFuncs = map[string]bool{
	"window_start": true,
	"window_end":   true,
}

// Valuer is the interface that wraps the Value() method.
type Valuer interface {
	// Value returns the value and existence flag for a given key.
	Value(key string) (interface{}, bool)
	Meta(key string) (interface{}, bool)
	AppendAlias(key string, value interface{}) bool
}

// CallValuer implements the Call method for evaluating function calls.
type CallValuer interface {
	Valuer

	// Call is invoked to evaluate a function call (if possible).
	Call(name string, args []interface{}) (interface{}, bool)
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

type Wildcarder interface {
	// Value returns the value and existence flag for a given key.
	All(stream string) (interface{}, bool)
}

type DataValuer interface {
	Valuer
	Wildcarder
	Clone() DataValuer
}

type WildcardValuer struct {
	Data Wildcarder
}

//TODO deal with wildcard of a stream, e.g. SELECT Table.* from Table inner join Table1
func (wv *WildcardValuer) Value(key string) (interface{}, bool) {
	if key == "" {
		return wv.Data.All(key)
	} else {
		a := strings.Index(key, ast.COLUMN_SEPARATOR+"*")
		if a <= 0 {
			return nil, false
		} else {
			return wv.Data.All(key[:a])
		}
	}
}

func (wv *WildcardValuer) Meta(string) (interface{}, bool) {
	return nil, false
}

func (wv *WildcardValuer) AppendAlias(string, interface{}) bool {
	// do nothing
	return false
}

type SortingData interface {
	Len() int
	Swap(i, j int)
	Index(i int) Valuer
}

// multiSorter implements the Sort interface, sorting the changes within.Hi
type MultiSorter struct {
	SortingData
	fields ast.SortFields
	valuer CallValuer
	values []map[string]interface{}
}

// OrderedBy returns a Sorter that sorts using the less functions, in order.
// Call its Sort method to sort the data.
func OrderedBy(fields ast.SortFields, fv *FunctionValuer) *MultiSorter {
	return &MultiSorter{
		fields: fields,
		valuer: fv,
	}
}

// Less is part of sort.Interface. It is implemented by looping along the
// less functions until it finds a comparison that discriminates between
// the two items (one is less than the other). Note that it can call the
// less functions twice per call. We could change the functions to return
// -1, 0, 1 and reduce the number of calls for greater efficiency: an
// exercise for the reader.
func (ms *MultiSorter) Less(i, j int) bool {
	p, q := ms.values[i], ms.values[j]
	v := &ValuerEval{Valuer: MultiValuer(ms.valuer)}
	for _, field := range ms.fields {
		n := field.Name
		vp, _ := p[n]
		vq, _ := q[n]
		if vp == nil && vq != nil {
			return false
		} else if vp != nil && vq == nil {
			ms.valueSwap(true, i, j)
			return true
		} else if vp == nil && vq == nil {
			return false
		}
		switch {
		case v.simpleDataEval(vp, vq, ast.LT):
			ms.valueSwap(field.Ascending, i, j)
			return field.Ascending
		case v.simpleDataEval(vq, vp, ast.LT):
			ms.valueSwap(!field.Ascending, i, j)
			return !field.Ascending
		}
	}
	return false
}

func (ms *MultiSorter) valueSwap(s bool, i, j int) {
	if s {
		ms.values[i], ms.values[j] = ms.values[j], ms.values[i]
	}
}

// Sort sorts the argument slice according to the less functions passed to OrderedBy.
func (ms *MultiSorter) Sort(data SortingData) error {
	ms.SortingData = data
	types := make([]string, len(ms.fields))
	ms.values = make([]map[string]interface{}, data.Len())
	//load and validate data
	for i := 0; i < data.Len(); i++ {
		ms.values[i] = make(map[string]interface{})
		p := data.Index(i)
		vep := &ValuerEval{Valuer: MultiValuer(p, ms.valuer)}
		for j, field := range ms.fields {
			n := field.Name
			vp, _ := vep.Valuer.Value(n)
			if err, ok := vp.(error); ok {
				return err
			} else {
				if types[j] == "" && vp != nil {
					types[j] = fmt.Sprintf("%T", vp)
				}
				if err := validate(types[j], vp); err != nil {
					return err
				} else {
					ms.values[i][n] = vp
				}
			}
		}
	}
	sort.Sort(ms)
	return nil
}

func validate(t string, v interface{}) error {
	if v == nil || t == "" {
		return nil
	}
	vt := fmt.Sprintf("%T", v)
	switch t {
	case "int", "int64", "float64", "uint64":
		if vt == "int" || vt == "int64" || vt == "float64" || vt == "uint64" {
			return nil
		} else {
			return fmt.Errorf("incompatible types for comparison: %s and %s", t, vt)
		}
	case "bool":
		if vt == "bool" {
			return nil
		} else {
			return fmt.Errorf("incompatible types for comparison: %s and %s", t, vt)
		}
	case "string":
		if vt == "string" {
			return nil
		} else {
			return fmt.Errorf("incompatible types for comparison: %s and %s", t, vt)
		}
	case "time.Time":
		_, err := cast.InterfaceToTime(v, "")
		if err != nil {
			return fmt.Errorf("incompatible types for comparison: %s and %s", t, vt)
		} else {
			return nil
		}
	default:
		return fmt.Errorf("incompatible types for comparison: %s and %s", t, vt)
	}
}

type EvalResultMessage struct {
	Emitter string
	Result  interface{}
	Message Message
}

type ResultsAndMessages []EvalResultMessage

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

// MultiValuer returns a Valuer that iterates over multiple Valuer instances
// to find a match.
func MultiValuer(valuers ...Valuer) Valuer {
	return multiValuer(valuers)
}

type multiValuer []Valuer

func (a multiValuer) Value(key string) (interface{}, bool) {
	for _, valuer := range a {
		if v, ok := valuer.Value(key); ok {
			return v, true
		}
	}
	return nil, false
}

func (a multiValuer) Meta(key string) (interface{}, bool) {
	for _, valuer := range a {
		if v, ok := valuer.Meta(key); ok {
			return v, true
		}
	}
	return nil, false
}

func (a multiValuer) AppendAlias(key string, value interface{}) bool {
	for _, valuer := range a {
		if ok := valuer.AppendAlias(key, value); ok {
			return true
		}
	}
	return false
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

func (a multiValuer) Call(name string, args []interface{}) (interface{}, bool) {
	for _, valuer := range a {
		if valuer, ok := valuer.(CallValuer); ok {
			if v, ok := valuer.Call(name, args); ok {
				return v, true
			} else {
				return fmt.Errorf("call func %s error: %v", name, v), false
			}
		}
	}
	return nil, false
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

func (a *multiAggregateValuer) Call(name string, args []interface{}) (interface{}, bool) {
	// assume the aggFuncMap already cache the custom agg funcs in IsAggFunc()
	isAgg := ast.FuncFinderSingleton().FuncType(name) == ast.AggFunc
	for _, valuer := range a.multiValuer {
		if a, ok := valuer.(AggregateCallValuer); ok && isAgg {
			if v, ok := a.Call(name, args); ok {
				return v, true
			} else {
				return fmt.Errorf("call func %s error: %v", name, v), false
			}
		} else if c, ok := valuer.(CallValuer); ok && !isAgg {
			if v, ok := c.Call(name, args); ok {
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

type BracketEvalResult struct {
	Start, End int
}

func (ber *BracketEvalResult) isIndex() bool {
	return ber.Start == ber.End
}

// Eval evaluates an expression and returns a value.
func (v *ValuerEval) Eval(expr ast.Expr) interface{} {
	if expr == nil {
		return nil
	}
	switch expr := expr.(type) {
	case *ast.BinaryExpr:
		return v.evalBinaryExpr(expr)
	case *ast.IntegerLiteral:
		return expr.Val
	case *ast.NumberLiteral:
		return expr.Val
	case *ast.ParenExpr:
		return v.Eval(expr.Expr)
	case *ast.StringLiteral:
		return expr.Val
	case *ast.BooleanLiteral:
		return expr.Val
	case *ast.ColonExpr:
		s, e := v.Eval(expr.Start), v.Eval(expr.End)
		si, err := cast.ToInt(s, cast.CONVERT_SAMEKIND)
		if err != nil {
			return fmt.Errorf("colon start %v is not int: %v", expr.Start, err)
		}
		ei, err := cast.ToInt(e, cast.CONVERT_SAMEKIND)
		if err != nil {
			return fmt.Errorf("colon end %v is not int: %v", expr.End, err)
		}
		return &BracketEvalResult{Start: si, End: ei}
	case *ast.IndexExpr:
		i := v.Eval(expr.Index)
		ii, err := cast.ToInt(i, cast.CONVERT_SAMEKIND)
		if err != nil {
			return fmt.Errorf("index %v is not int: %v", expr.Index, err)
		}
		return &BracketEvalResult{Start: ii, End: ii}
	case *ast.Call:
		if _, ok := implicitValueFuncs[expr.Name]; ok {
			if vv, ok := v.Valuer.(FuncValuer); ok {
				val, ok := vv.FuncValue(expr.Name)
				if ok {
					return val
				}
			}
		} else {
			if valuer, ok := v.Valuer.(CallValuer); ok {
				var args []interface{}
				if len(expr.Args) > 0 {
					args = make([]interface{}, len(expr.Args))
					for i, arg := range expr.Args {
						if aggreValuer, ok := valuer.(AggregateCallValuer); ast.FuncFinderSingleton().IsAggFunc(expr) && ok {
							args[i] = aggreValuer.GetAllTuples().AggregateEval(arg, aggreValuer.GetSingleCallValuer())
						} else {
							args[i] = v.Eval(arg)
							if _, ok := args[i].(error); ok {
								return args[i]
							}
						}
					}
				}
				val, _ := valuer.Call(expr.Name, args)
				return val
			}
		}
		return nil
	case *ast.FieldRef:
		var n string
		if expr.IsAlias() { // alias is renamed internally to avoid accidentally evaled as a col with the same name
			n = fmt.Sprintf("%s%s", PRIVATE_PREFIX, expr.Name)
		} else if expr.StreamName == ast.DefaultStream {
			n = expr.Name
		} else {
			n = fmt.Sprintf("%s%s%s", string(expr.StreamName), ast.COLUMN_SEPARATOR, expr.Name)
		}
		if n != "" {
			val, ok := v.Valuer.Value(n)
			if ok {
				return val
			}
		}
		if expr.IsAlias() {
			r := v.Eval(expr.Expression)
			v.Valuer.AppendAlias(expr.Name, r)
			return r
		}
		return nil
	case *ast.MetaRef:
		if expr.StreamName == "" || expr.StreamName == ast.DefaultStream {
			val, _ := v.Valuer.Meta(expr.Name)
			return val
		} else {
			//The field specified with stream source
			val, _ := v.Valuer.Meta(string(expr.StreamName) + ast.COLUMN_SEPARATOR + expr.Name)
			return val
		}
	case *ast.JsonFieldRef:
		val, ok := v.Valuer.Value(expr.Name)
		if ok {
			return val
		} else {
			return nil
		}
	case *ast.Wildcard:
		val, _ := v.Valuer.Value("")
		return val
	case *ast.CaseExpr:
		return v.evalCase(expr)
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
	return v.simpleDataEval(lhs, rhs, expr.OP)
}

func (v *ValuerEval) evalCase(expr *ast.CaseExpr) interface{} {
	if expr.Value != nil { // compare value to all when clause
		ev := v.Eval(expr.Value)
		for _, w := range expr.WhenClauses {
			wv := v.Eval(w.Expr)
			switch r := v.simpleDataEval(ev, wv, ast.EQ).(type) {
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

func isSliceOrArray(v interface{}) bool {
	kind := reflect.ValueOf(v).Kind()
	return kind == reflect.Array || kind == reflect.Slice
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

//lhs and rhs are non-nil
func (v *ValuerEval) simpleDataEval(lhs, rhs interface{}, op ast.Token) interface{} {
	if lhs == nil || rhs == nil {
		switch op {
		case ast.EQ, ast.LTE, ast.GTE:
			if lhs == nil && rhs == nil {
				return true
			} else {
				return false
			}
		case ast.NEQ:
			if lhs == nil && rhs == nil {
				return false
			} else {
				return true
			}
		case ast.LT, ast.GT:
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
		rhs, ok := rhs.(bool)
		if !ok {
			return invalidOpError(lhs, op, rhs)
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

func invalidOpError(lhs interface{}, op ast.Token, rhs interface{}) error {
	return fmt.Errorf("invalid operation %[1]T(%[1]v) %s %[3]T(%[3]v)", lhs, ast.Tokens[op], rhs)
}

func convertNum(para interface{}) interface{} {
	if isInt(para) {
		para = toInt64(para)
	} else if isFloat(para) {
		para = toFloat64(para)
	}
	return para
}

func isInt(para interface{}) bool {
	switch para.(type) {
	case int:
		return true
	case int8:
		return true
	case int16:
		return true
	case int32:
		return true
	case int64:
		return true
	}
	return false
}

func toInt64(para interface{}) int64 {
	if v, ok := para.(int); ok {
		return int64(v)
	} else if v, ok := para.(int8); ok {
		return int64(v)
	} else if v, ok := para.(int16); ok {
		return int64(v)
	} else if v, ok := para.(int32); ok {
		return int64(v)
	} else if v, ok := para.(int64); ok {
		return v
	}
	return 0
}

func isFloat(para interface{}) bool {
	switch para.(type) {
	case float32:
		return true
	case float64:
		return true
	}
	return false
}

func toFloat64(para interface{}) float64 {
	if v, ok := para.(float32); ok {
		return float64(v)
	} else if v, ok := para.(float64); ok {
		return v
	}
	return 0
}
