package xsql

import (
	"github.com/emqx/kuiper/internal/topo/context"
	"github.com/emqx/kuiper/pkg/api"
	"github.com/emqx/kuiper/pkg/errorx"
	"sync"
)

//Manage the function plugin instances
//Each operator has a single instance of this to hold the context
type funcRuntime struct {
	sync.Mutex
	regs          map[string]*funcReg
	parentCtx     api.StreamContext
	funcRegisters []FunctionRegister
}

type funcReg struct {
	ins api.Function
	ctx api.FunctionContext
}

func NewFuncRuntime(ctx api.StreamContext, registers []FunctionRegister) *funcRuntime {
	return &funcRuntime{
		parentCtx:     ctx,
		funcRegisters: registers,
	}
}

func (fp *funcRuntime) Get(name string) (api.Function, api.FunctionContext, error) {
	fp.Lock()
	defer fp.Unlock()
	if fp.regs == nil {
		fp.regs = make(map[string]*funcReg)
	}
	if reg, ok := fp.regs[name]; !ok {
		var (
			nf  api.Function
			err error
		)
		// Check service extension and plugin extension if set
		for _, r := range fp.funcRegisters {
			if r.HasFunction(name) {
				nf, err = r.Function(name)
				if err != nil {
					return nil, nil, err
				}
				break
			}
		}
		if nf == nil {
			return nil, nil, errorx.NotFoundErr
		}
		fctx := context.NewDefaultFuncContext(fp.parentCtx, len(fp.regs))
		fp.regs[name] = &funcReg{
			ins: nf,
			ctx: fctx,
		}
		return nf, fctx, nil
	} else {
		return reg.ins, reg.ctx, nil
	}
}
