package node

import (
	"context"
	"time"

	"github.com/lf-edge/ekuiper/v2/internal/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/v2/catalog"
	"github.com/lf-edge/ekuiper/v2/internal/v2/planner"
)

type SourceNode struct {
	Stream  *catalog.Stream
	Started bool
	*BaseNode
}

func NewSourceNode(pp *planner.PhysicalDataSource) *SourceNode {
	sn := &SourceNode{BaseNode: NewBaseNode(pp.GetIndex(), "source", len(pp.GetChildren()))}
	sn.Stream = pp.Stream
	return sn
}

func (sn *SourceNode) Run(ctx context.Context) {
	var ticker *time.Ticker
	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
	}()
	go func() {
		sn.whenUnstarted(ctx)
		ticker = time.NewTicker(time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-sn.Input:
				processed, send := sn.HandleNodeMsg(ctx, msg)
				if processed {
					if send {
						sn.BroadCast(msg)
						sn.handleControlSignal(ctx, msg)
					}
					continue
				}
				sn.BroadCast(msg)
			case <-ticker.C:
				msg := &NodeMessage{Tuples: make([]*api.Tuple, 0)}
				t, err := api.NewTupleFromData(sn.Stream.StreamName, map[string]any{"key": "value"})
				if err != nil {
					msg.Err = err
				} else {
					msg.Tuples = append(msg.Tuples, t)
				}
				sn.BroadCast(msg)
			}
		}
	}()
}

func (sn *SourceNode) whenUnstarted(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-sn.Input:
			processed, send := sn.HandleNodeMsg(ctx, msg)
			if processed {
				if send {
					sn.BroadCast(msg)
					sn.handleControlSignal(ctx, msg)
				}
				if sn.Started {
					return
				}
				continue
			}
			sn.BroadCast(msg)
		}
	}
}

func (sn *SourceNode) handleControlSignal(ctx context.Context, msg *NodeMessage) {
	switch msg.Control.ControlSignal {
	case StartRuleSignal:
		if !sn.Started {
			sn.Started = true
		}
	case StopRuleSignal:
		sn.Close(ctx)
	}
}

func (sn *SourceNode) Close(ctx context.Context) {
}
