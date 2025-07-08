package node

import (
	"context"
	"fmt"

	"github.com/lf-edge/ekuiper/v2/internal/v2/planner"
)

type ProjectNode struct {
	*BaseNode
}

func NewProjectNode(pp *planner.PhysicalProject) *ProjectNode {
	return &ProjectNode{NewBaseNode(pp.GetIndex(), "project", len(pp.GetChildren()))}
}

func (pn *ProjectNode) Run(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-pn.Input:
				processd, send := pn.HandleNodeMsg(msg)
				if processd {
					if send {
						pn.BroadCast(msg)
					}
					continue
				}
				if len(msg.Tuples) > 0 {
					fmt.Println(msg.TupleString())
				}
			}
		}
	}()
}
