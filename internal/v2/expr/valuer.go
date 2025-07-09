package expr

import (
	"context"

	"github.com/lf-edge/ekuiper/v2/internal/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/v2/function"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

type ValuerEval struct {
	Tuple *api.Tuple
}

func NewValuerEval(tuple *api.Tuple) *ValuerEval {
	return &ValuerEval{Tuple: tuple}
}

func (v *ValuerEval) EvalFieldRef(ctx context.Context, f *ast.FieldRef) (*api.Datum, error) {
	if f.AliasRef != nil {
		var d *api.Datum
		var ok bool
		var err error
		d, _, ok = v.Tuple.AffiliateRow.ValueByKey("", f.Name)
		if !ok {
			d, err = v.Eval(ctx, f.AliasRef.Expression)
			if err != nil {
				return nil, err
			}
			v.Tuple.AffiliateRow.Append("", f.Name, d)
		}
		return d, nil
	}
	if f.SourceIndex != -1 {
		d, streamName, key, ok := v.Tuple.ValueByColumnIndex(f.Index)
		if !ok {
			return nil, nil
		}
		if f.Name == key && string(f.StreamName) == streamName {
			return d, nil
		}
	}
	if f.Index != -1 {
		d, streamName, key, ok := v.Tuple.ValueByAffiliateRowIndex(f.Index)
		if !ok {
			return nil, nil
		}
		if f.Name == key && string(f.StreamName) == streamName {
			return d, nil
		}
	}
	d, columnIndex, affIndex, ok := v.Tuple.ValueByKey(string(f.StreamName), f.Name)
	if !ok {
		return nil, nil
	}
	f.SourceIndex = columnIndex
	f.Index = affIndex
	return d, nil
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
		return v.EvalFieldRef(ctx, et)
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

type TupleValuer struct {
	tuple      *api.Tuple
	cacheIndex map[string]int64
}
