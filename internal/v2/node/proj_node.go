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
		filedName := field.Name
		if field.AName != "" {
			filedName = field.AName
		}
		v, err := valuer.Eval(ctx, field.Expr)
		if err != nil {
			return nil, err
		}
		tuple.AppendAffiliateRow(filedName, v)
		newTuple.AppendAffiliateRow(filedName, v)
	}
	return newTuple, nil
}
