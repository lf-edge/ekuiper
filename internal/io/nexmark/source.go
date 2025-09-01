package nexmark

import (
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

type NexmarkSourceConfig struct {
	Qps            int  `json:"qps"`
	BufferSize     int  `json:"bufferSize"`
	ExcludePerson  bool `json:"excludePerson"`
	ExcludeAuction bool `json:"excludeAuction"`
	ExcludeBid     bool `json:"excludeBid"`
}

type NexmarkSource struct {
	config    NexmarkSourceConfig
	generator *EventGenerator
}

func (n *NexmarkSource) Provision(ctx api.StreamContext, configs map[string]any) error {
	config := NexmarkSourceConfig{
		Qps:        1,
		BufferSize: 1024,
	}
	if err := cast.MapToStruct(configs, config); err != nil {
		return err
	}
	n.config = config
	ops := make([]WithGenOption, 0)
	if n.config.ExcludeAuction {
		ops = append(ops, WithExcludeAuction())
	}
	if n.config.ExcludeBid {
		ops = append(ops, WithExcludeBid())
	}
	if n.config.ExcludePerson {
		ops = append(ops, WithExcludePerson())
	}
	generator := NewEventGenerator(ctx, n.config.Qps, n.config.BufferSize, ops...)
	n.generator = generator
	return nil
}

func (n *NexmarkSource) Close(ctx api.StreamContext) error {
	n.generator.Close()
	return nil
}

func (n *NexmarkSource) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	return nil
}

func (n *NexmarkSource) Subscribe(ctx api.StreamContext, ingest api.TupleIngest, ingestError api.ErrorIngest) error {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event := <-n.generator.eventChan:
				ingest(ctx, event, map[string]any{"topic": "nexmark"}, time.Now())
			}
		}
	}()
	n.generator.GenStream()
	return nil
}

func GetSource() api.Source {
	return &NexmarkSource{}
}
