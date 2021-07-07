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
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	"strings"
)

type FunctionRegister interface {
	HasFunction(name string) bool
	Function(name string) (api.Function, error)
}

// ONLY use NewFunctionValuer function to initialize
type FunctionValuer struct {
	runtime *funcRuntime
}

//Should only be called by stream to make sure a single instance for an operation
func NewFunctionValuer(p *funcRuntime) *FunctionValuer {
	fv := &FunctionValuer{
		runtime: p,
	}
	return fv
}

func (*FunctionValuer) Value(string) (interface{}, bool) {
	return nil, false
}

func (*FunctionValuer) Meta(string) (interface{}, bool) {
	return nil, false
}

func (*FunctionValuer) AppendAlias(string, interface{}) bool {
	return false
}

func (fv *FunctionValuer) Call(name string, args []interface{}) (interface{}, bool) {
	lowerName := strings.ToLower(name)
	switch ast.FuncFinderSingleton().FuncType(lowerName) {
	case ast.NotFoundFunc:
		nf, fctx, err := fv.runtime.Get(name)
		switch err {
		case errorx.NotFoundErr:
			return nil, false
		case nil:
			// do nothing, continue
		default:
			return err, false
		}
		if nf.IsAggregate() {
			return nil, false
		}
		logger := fctx.GetLogger()
		logger.Debugf("run func %s", name)
		return nf.Exec(args, fctx)
	case ast.AggFunc:
		return nil, false
	case ast.MathFunc:
		return mathCall(lowerName, args)
	case ast.ConvFunc:
		return convCall(lowerName, args)
	case ast.StrFunc:
		return strCall(lowerName, args)
	case ast.HashFunc:
		return hashCall(lowerName, args)
	case ast.JsonFunc:
		return jsonCall(lowerName, args)
	case ast.OtherFunc:
		return otherCall(lowerName, args)
	default:
		return nil, false
	}
}
