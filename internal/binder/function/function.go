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

package function

import (
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"strings"
)

type funcType int

const (
	NotFoundFunc funcType = iota - 1
	AggFunc
	MathFunc
	StrFunc
	ConvFunc
	HashFunc
	JsonFunc
	OtherFunc
)

var maps = []map[string]string{
	aggFuncMap, mathFuncMap, strFuncMap, convFuncMap, hashFuncMap, jsonFuncMap, otherFuncMap,
}

var aggFuncMap = map[string]string{"avg": "",
	"count": "",
	"max":   "", "min": "",
	"sum":         "",
	"collect":     "",
	"deduplicate": "",
}

var funcWithAsteriskSupportMap = map[string]string{
	"collect": "",
	"count":   "",
}

var mathFuncMap = map[string]string{"abs": "", "acos": "", "asin": "", "atan": "", "atan2": "",
	"bitand": "", "bitor": "", "bitxor": "", "bitnot": "",
	"ceil": "", "cos": "", "cosh": "",
	"exp": "",
	"ln":  "", "log": "",
	"mod":   "",
	"power": "",
	"rand":  "", "round": "",
	"sign": "", "sin": "", "sinh": "", "sqrt": "",
	"tan": "", "tanh": "",
}

var strFuncMap = map[string]string{"concat": "",
	"endswith":    "",
	"format_time": "",
	"indexof":     "",
	"length":      "", "lower": "", "lpad": "", "ltrim": "",
	"numbytes":       "",
	"regexp_matches": "", "regexp_replace": "", "regexp_substr": "", "rpad": "", "rtrim": "",
	"substring": "", "startswith": "", "split_value": "",
	"trim":  "",
	"upper": "",
}

var convFuncMap = map[string]string{"concat": "", "cast": "", "chr": "",
	"encode": "",
	"trunc":  "",
}

var hashFuncMap = map[string]string{"md5": "",
	"sha1": "", "sha256": "", "sha384": "", "sha512": "",
}

var jsonFuncMap = map[string]string{
	"json_path_query": "", "json_path_query_first": "", "json_path_exists": "",
}

var otherFuncMap = map[string]string{"isnull": "",
	"newuuid": "", "tstamp": "", "mqtt": "", "meta": "", "cardinality": "",
	"window_start": "",
	"window_end":   "",
}

func getFuncType(name string) funcType {
	for i, m := range maps {
		if _, ok := m[strings.ToLower(name)]; ok {
			return funcType(i)
		}
	}
	return NotFoundFunc
}

type funcExecutor struct{}

func (f *funcExecutor) ValidateWithName(args []ast.Expr, name string) error {
	var eargs []ast.Expr
	for _, arg := range args {
		if t, ok := arg.(ast.Expr); ok {
			eargs = append(eargs, t)
		} else {
			// should never happen
			return fmt.Errorf("receive invalid arg %v", arg)
		}
	}
	return validateFuncs(name, eargs)
}

func (f *funcExecutor) Validate(_ []interface{}) error {
	return fmt.Errorf("unknow name")
}

func (f *funcExecutor) Exec(_ []interface{}, _ api.FunctionContext) (interface{}, bool) {
	return fmt.Errorf("unknow name"), false
}

func (f *funcExecutor) ExecWithName(args []interface{}, _ api.FunctionContext, name string) (interface{}, bool) {
	lowerName := strings.ToLower(name)
	switch getFuncType(lowerName) {
	case AggFunc:
		return aggCall(lowerName, args)
	case MathFunc:
		return mathCall(lowerName, args)
	case ConvFunc:
		return convCall(lowerName, args)
	case StrFunc:
		return strCall(lowerName, args)
	case HashFunc:
		return hashCall(lowerName, args)
	case JsonFunc:
		return jsonCall(lowerName, args)
	case OtherFunc:
		return otherCall(lowerName, args)
	}
	return fmt.Errorf("unknow name"), false
}

func (f *funcExecutor) IsAggregate() bool {
	return false
}

func (f *funcExecutor) IsAggregateWithName(name string) bool {
	lowerName := strings.ToLower(name)
	return getFuncType(lowerName) == AggFunc
}

var staticFuncExecutor = &funcExecutor{}

type Manager struct{}

func (m *Manager) Function(name string) (api.Function, error) {
	ft := getFuncType(name)
	if ft != NotFoundFunc {
		return staticFuncExecutor, nil
	}
	return nil, nil
}

func (m *Manager) HasFunctionSet(name string) bool {
	return name == "internal"
}

var m = &Manager{}

func GetManager() *Manager {
	return m
}
