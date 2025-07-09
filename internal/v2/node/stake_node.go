package node

import (
	"context"

	"github.com/lf-edge/ekuiper/v2/internal/v2/planner"
)

type StakeNode struct {
	IsEnd  bool
	IsRoot bool
	*BaseNode
}

func NewStakeNode(pp *planner.PhysicalStake) *StakeNode {
	sn := &StakeNode{BaseNode: NewBaseNode(pp.GetIndex(), "stake", len(pp.GetChildren()))}
	return sn
}

func (sn *StakeNode) Run(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-sn.Input:
				processed, send := sn.HandleNodeMsg(ctx, msg)
				if processed {
					if send {
						sn.BroadCast(msg)
					}
					continue
				}
				sn.BroadCast(msg)
			}
		}
	}()
}
