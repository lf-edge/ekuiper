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
	"github.com/lf-edge/ekuiper/internal/binder/function"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	"sync"
)

//Manage the function plugin instances
//Each operator has a single instance of this to hold the context
type funcRuntime struct {
	sync.Mutex
	regs      []*funcReg
	parentCtx api.StreamContext
}

type funcReg struct {
	ins api.Function
	ctx api.FunctionContext
}

func NewFuncRuntime(ctx api.StreamContext) *funcRuntime {
	return &funcRuntime{
		parentCtx: ctx,
	}
}

func (fp *funcRuntime) Get(name string, funcId int) (api.Function, api.FunctionContext, error) {
	fp.Lock()
	defer fp.Unlock()
	if len(fp.regs) <= funcId {
		for i := len(fp.regs); i <= funcId; i++ {
			fp.regs = append(fp.regs, nil)
		}
	}
	if reg := fp.regs[funcId]; reg == nil {
		var (
			nf  api.Function
			err error
		)
		// Check service extension and plugin extension if set
		nf, err = function.Function(name)
		if nf == nil {
			if err == nil {
				return nil, nil, errorx.NotFoundErr
			} else {
				return nil, nil, err
			}
		}
		fctx := context.NewDefaultFuncContext(fp.parentCtx, funcId)
		fp.regs[funcId] = &funcReg{
			ins: nf,
			ctx: fctx,
		}
		return nf, fctx, nil
	} else {
		return reg.ins, reg.ctx, nil
	}
}
