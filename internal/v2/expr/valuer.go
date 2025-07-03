package expr

import (
	"context"
	"fmt"

	"github.com/lf-edge/ekuiper/v2/internal/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/v2/function"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

type Valuer interface {
	ValueByKey(string) (*api.Datum, bool)
	ValueByColumnIndex(int) (*api.Datum, bool)
	ValueByAffiliateRowIndex(int) (*api.Datum, bool)
}

type ValuerEval struct {
	values []Valuer
}

func (v *ValuerEval) Eval(ctx context.Context, expr ast.Expr) (*api.Datum, error) {
	if expr == nil {
		return nil, nil
	}
	switch et := expr.(type) {
	case *ast.IntegerLiteral:
		return api.NewI64Datum(et.Val), nil
	case *ast.NumberLiteral:
		return api.NewF64Datum(et.Val), nil
	case *ast.FieldRef:
		for _, vr := range v.values {
			d, ok := vr.ValueByKey(et.Name)
			if ok {
				return d, nil
			}
		}
		return nil, fmt.Errorf("field not found: %s", et.Name)
	case *ast.Call:
		datumArgs := make([]*api.Datum, 0)
		for _, arg := range et.Args {
			argD, err := v.Eval(ctx, arg)
			if err != nil {
				return nil, err
			}
			datumArgs = append(datumArgs, argD)
		}
		return function.CallFunction(ctx, et.Name, datumArgs)
	}
	return nil, nil
}
