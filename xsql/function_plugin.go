package xsql

import (
	"github.com/emqx/kuiper/plugins"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/contexts"
)

//Manage the function plugin instances
//Each operator has a single instance of this
type funcPlugins struct {
	plugins   map[string]*funcReg
	parentCtx api.StreamContext
}

type funcReg struct {
	ins api.Function
	ctx api.FunctionContext
}

func NewFuncPlugins(ctx api.StreamContext) *funcPlugins {
	return &funcPlugins{
		parentCtx: ctx,
	}
}

func (fp *funcPlugins) GetFuncFromPlugin(name string) (api.Function, api.FunctionContext, error) {
	if fp.plugins == nil {
		fp.plugins = make(map[string]*funcReg)
	}
	if reg, ok := fp.plugins[name]; !ok {
		nf, err := plugins.GetFunction(name)
		if err != nil {
			return nil, nil, err
		}
		fctx := contexts.NewDefaultFuncContext(fp.parentCtx, len(fp.plugins))
		fp.plugins[name] = &funcReg{
			ins: nf,
			ctx: fctx,
		}
		return nf, fctx, nil
	} else {
		return reg.ins, reg.ctx, nil
	}
}
