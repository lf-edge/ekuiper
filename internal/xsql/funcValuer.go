package xsql

import (
	"github.com/emqx/kuiper/pkg/api"
	"github.com/emqx/kuiper/pkg/ast"
	"github.com/emqx/kuiper/pkg/errorx"
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
