// Copyright 2025 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
