// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/pkg/errorx"
)

// FunctionValuer ONLY use NewFunctionValuer function to initialize
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

func (*FunctionValuer) Value(_, _ string) (interface{}, bool) {
	return nil, false
}

func (*FunctionValuer) Meta(_, _ string) (interface{}, bool) {
	return nil, false
}

func (*FunctionValuer) AppendAlias(string, interface{}) bool {
	return false
}

func (*FunctionValuer) AliasValue(string) (interface{}, bool) {
	return nil, false
}

func (fv *FunctionValuer) Call(name string, funcId int, args []interface{}) (interface{}, bool) {
	nf, fctx, err := fv.runtime.Get(name, funcId)
	switch err {
	case errorx.NotFoundErr:
		return nil, false
	case nil:
		// do nothing, continue
	default:
		return err, false
	}
	return ExecFunc(name, nf, args, fctx)
}
