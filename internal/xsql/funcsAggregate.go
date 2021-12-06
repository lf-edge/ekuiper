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
	"github.com/lf-edge/ekuiper/pkg/errorx"
)

type AggregateFunctionValuer struct {
	data AggregateData
	fv   *FunctionValuer
}

func NewFunctionValuersForOp(ctx api.StreamContext) (*FunctionValuer, *AggregateFunctionValuer) {
	p := NewFuncRuntime(ctx)
	return NewAggregateFunctionValuers(p)
}

//Should only be called by stream to make sure a single instance for an operation
func NewAggregateFunctionValuers(p *funcRuntime) (*FunctionValuer, *AggregateFunctionValuer) {
	fv := NewFunctionValuer(p)
	return fv, &AggregateFunctionValuer{
		fv: fv,
	}
}

func (v *AggregateFunctionValuer) SetData(data AggregateData) {
	v.data = data
}

func (v *AggregateFunctionValuer) GetSingleCallValuer() CallValuer {
	return v.fv
}

func (v *AggregateFunctionValuer) Value(_, _ string) (interface{}, bool) {
	return nil, false
}

func (v *AggregateFunctionValuer) Meta(_, _ string) (interface{}, bool) {
	return nil, false
}

func (v *AggregateFunctionValuer) FuncValue(key string) (interface{}, bool) {
	if vv, ok := v.data.(FuncValuer); ok {
		return vv.FuncValue(key)
	}
	return nil, false
}

func (*AggregateFunctionValuer) AppendAlias(string, interface{}) bool {
	return false
}

func (v *AggregateFunctionValuer) AliasValue(_ string) (interface{}, bool) {
	return nil, false
}

func (v *AggregateFunctionValuer) Call(name string, args []interface{}) (interface{}, bool) {
	nf, fctx, err := v.fv.runtime.Get(name)
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

func (v *AggregateFunctionValuer) GetAllTuples() AggregateData {
	return v.data
}
