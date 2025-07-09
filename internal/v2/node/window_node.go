package node

import (
	"context"

	"github.com/lf-edge/ekuiper/v2/internal/v2/planner"
)

type CountWindowNode struct {
	count int
	*BaseNode
}

func NewCountWindowNode(pw *planner.PhysicalCountWindow) *CountWindowNode {
	cw := &CountWindowNode{BaseNode: NewBaseNode(pw.GetIndex(), "CountWindow", len(pw.GetChildren()))}
	cw.count = pw.Count
	return cw
}

func (pn *CountWindowNode) Run(ctx context.Context) {
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
				pn.BroadCast(msg)
			}
		}
	}()
}
