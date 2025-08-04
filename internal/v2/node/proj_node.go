package node

import (
	"context"

	"github.com/lf-edge/ekuiper/v2/internal/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/v2/expr"
	"github.com/lf-edge/ekuiper/v2/internal/v2/planner"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

type ProjectNode struct {
	Fields ast.Fields
	*BaseNode
}

func NewProjectNode(pp *planner.PhysicalProject) *ProjectNode {
	pn := &ProjectNode{
		BaseNode: NewBaseNode(pp.GetIndex(), "project", len(pp.GetChildren())),
	}
	pn.Fields = pp.Fields
	return pn
}

func (pn *ProjectNode) Run(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-pn.Input:
				processed, send := pn.HandleNodeMsg(ctx, msg)
				if processed {
					if send {
						pn.BroadCast(msg)
					}
					continue
				}
				for index, tuple := range msg.Tuples {
					nt, err := pn.evalTuple(ctx, tuple)
					if err != nil {
						msg.Err = err
						break
					}
					msg.Tuples[index] = nt
				}
				pn.BroadCast(msg)
			}
		}
	}()
}

func (pn *ProjectNode) evalTuple(ctx context.Context, tuple *api.Tuple) (*api.Tuple, error) {
	newTuple := api.NewTupleFromCtx(tuple.Ctx, tuple.Meta)
	valuer := expr.NewValuerEval(tuple)
	for _, field := range pn.Fields {
		if field.IsWildcard() {
			pn.evalWildcard(ctx, field.Expr.(*ast.Wildcard), tuple, newTuple)
			continue
		}
		if err := pn.evalField(ctx, valuer, field, tuple, newTuple); err != nil {
			return nil, err
		}
	}
	return newTuple, nil
}

func (pn *ProjectNode) evalField(ctx context.Context, valuer *expr.ValuerEval, field ast.Field, tuple *api.Tuple, newTuple *api.Tuple) error {
	filedName := field.Name
	if field.AName != "" {
		filedName = field.AName
	}
	v, err := valuer.Eval(ctx, field.Expr)
	if err != nil {
		return err
	}
	tuple.AppendAffiliateRow(filedName, v)
	newTuple.AppendAffiliateRow(filedName, v)
	return nil
}

func (pn *ProjectNode) evalWildcard(ctx context.Context, _ *ast.Wildcard, tuple *api.Tuple, newTuple *api.Tuple) {
	for index, k := range tuple.Columns.Keys {
		newTuple.AppendColumn(tuple.Columns.Streams[index], k, tuple.Columns.Values[index])
	}
}
