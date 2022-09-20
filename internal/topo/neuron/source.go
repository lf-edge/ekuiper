// Copyright 2022 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal/topo/memory/pubsub"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/infra"
)

type source struct {
	url          string
	bufferLength int
}

func (s *source) Configure(_ string, props map[string]interface{}) error {
	s.url = NeuronUrl
	s.bufferLength = 1024
	if c, ok := props["bufferLength"]; ok {
		if bl, err := cast.ToInt(c, cast.STRICT); err != nil || bl > 0 {
			s.bufferLength = bl
		}
	}
	return nil
}

func (s *source) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	err := createOrGetConnection(ctx, s.url)
	if err != nil {
		infra.DrainError(ctx, err, errCh)
		return
	}
	defer closeConnection(ctx, s.url)
	ch := pubsub.CreateSub(NeuronTopic, nil, fmt.Sprintf("%s_%s_%d", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId()), s.bufferLength)
	defer pubsub.CloseSourceConsumerChannel(NeuronTopic, fmt.Sprintf("%s_%s_%d", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId()))
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
