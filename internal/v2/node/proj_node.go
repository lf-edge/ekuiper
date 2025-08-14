package node

import (
	"context"
	"sync"

	"github.com/lf-edge/ekuiper/v2/internal/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/v2/expr"
	"github.com/lf-edge/ekuiper/v2/internal/v2/planner"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

type ProjectNode struct {
	Fields  ast.Fields
	handler *OrderNodeMessageHandler
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
	pn.handler = NewOrderNodeMessageHandler(ctx, 2, pn.handleNodeMessage)
	wg := sync.WaitGroup{}
	go func() {
		for {
			select {
			case <-ctx.Done():
				pn.handler.QuickClose()
				return
			case msg := <-pn.Input:
				if msg.IsControlSignal(StopRuleSignal) {
					pn.handler.GraceClose()
					wg.Wait()
				}
				processed, send := pn.HandleNodeMsg(ctx, msg)
				if processed {
					if send {
						pn.BroadCast(msg)
					}
					continue
				}
				pn.handler.In <- msg
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-pn.handler.Out:
				if !ok {
					return
				}
				pn.BroadCast(msg)
			}
		}
	}()
}

func (pn *ProjectNode) handleNodeMessage(ctx context.Context, data *NodeMessage) *NodeMessage {
	for index, tuple := range data.Tuples {
		nt, err := pn.evalTuple(ctx, tuple)
		if err != nil {
			data.Err = err
			break
		}
		data.Tuples[index] = nt
	}
	return data
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
