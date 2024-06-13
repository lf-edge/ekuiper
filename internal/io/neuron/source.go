// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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

package neuron

import (
	"fmt"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type sc struct {
	Url          string `json:"url,omitempty"`
	BufferLength int    `json:"bufferLength,omitempty"`
}

type source struct {
	c *sc
}

func (s *source) Provision(_ api.StreamContext, props map[string]any) error {
	cc := &sc{
		BufferLength: 1024,
		Url:          DefaultNeuronUrl,
	}
	err := cast.MapToStruct(props, cc)
	if err != nil {
		return err
	}
	s.c = cc
	return nil
}

func (s *source) Connect(ctx api.StreamContext) error {
	_, err := createOrGetConnection(ctx, s.c.Url)
	return err
}

func (s *source) Subscribe(ctx api.StreamContext, ingest api.TupleIngest, _ api.ErrorIngest) error {
	ch := pubsub.CreateSub(TopicPrefix+s.c.Url, nil, fmt.Sprintf("%s_%s_%d", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId()), s.c.BufferLength)
	go func() {
		defer pubsub.CloseSourceConsumerChannel(TopicPrefix+s.c.Url, fmt.Sprintf("%s_%s_%d", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId()))
		err := infra.SafeRun(func() error {
			for {
				select {
				case v := <-ch:
					ingest(ctx, v, nil, timex.GetNow())
				case <-ctx.Done():
					return nil
				}
			}
		})
		if err != nil {
			ctx.GetLogger().Errorf("exit neuron source subscribe for %v", err)
		}
	}()
	return nil
}

func (s *source) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("closing neuron source")
	return closeConnection(ctx, s.c.Url)
}

func GetSource() api.Source {
	return &source{}
}
