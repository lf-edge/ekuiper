package node

import (
	"context"
	"fmt"

	"github.com/lf-edge/ekuiper/v2/internal/v2/planner"
)

type SinkNode struct {
	*BaseNode
}

func NewSinkNode(pp *planner.PhysicalDataSink) *SinkNode {
	sn := &SinkNode{BaseNode: NewBaseNode(pp.GetIndex(), "sink", len(pp.GetChildren()))}
	return sn
}

func (pn *SinkNode) Run(ctx context.Context) {
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
				fmt.Println(msg.TupleString())
				pn.BroadCast(msg)
			}
		}
	}()
}
