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
				processd, send := sn.HandleNodeMsg(msg)
				if processd {
					if send {
						sn.BroadCast(msg)
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
			processed, send := sn.HandleNodeMsg(msg)
			if processed {
				if send {
					sn.BroadCast(msg)
					if msg.IsSameControlSignal(StartRuleSignal) && !sn.Started {
						sn.Started = true
						break
					}
				}
				continue
			}
			sn.BroadCast(msg)
		}
		if sn.Started {
			return
		}
	}
}
