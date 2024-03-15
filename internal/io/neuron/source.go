// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/internal/io/memory/pubsub"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/infra"
)

type sc struct {
	Url          string `json:"url,omitempty"`
	BufferLength int    `json:"bufferLength,omitempty"`
}

type source struct {
	c *sc
}

func (s *source) Configure(_ string, props map[string]interface{}) error {
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

func (s *source) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	_, err := createOrGetConnection(ctx, s.c.Url)
	if err != nil {
		infra.DrainError(ctx, err, errCh)
		return
	}
	defer closeConnection(ctx, s.c.Url)
	ch := pubsub.CreateSub(TopicPrefix+s.c.Url, nil, fmt.Sprintf("%s_%s_%d", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId()), s.c.BufferLength)
	defer pubsub.CloseSourceConsumerChannel(TopicPrefix+s.c.Url, fmt.Sprintf("%s_%s_%d", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId()))
	for {
		select {
		case v, opened := <-ch:
			if !opened {
				return
			}
			consumer <- v
		case <-ctx.Done():
			return
		}
	}
}

func (s *source) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("closing neuron source")
	return nil
}

func GetSource() *source {
	return &source{}
}
