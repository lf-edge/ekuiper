// Copyright 2021 EMQ Technologies Co., Ltd.
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

package memory

import (
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/api"
)

type source struct {
	topic string
	input <-chan map[string]interface{}
}

func (s *source) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, _ chan<- error) {
	ch := getOrCreateSinkConsumerChannel(s.topic, fmt.Sprintf("%s_%s_%d", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId()))
	s.input = ch
	for {
		select {
		case v, opened := <-s.input:
			if !opened {
				return
			}
			consumer <- api.NewDefaultSourceTuple(v, make(map[string]interface{}))
		case <-ctx.Done():
			return
		}
	}
}

func (s *source) Configure(datasource string, _ map[string]interface{}) error {
	s.topic = datasource
	return nil
}

func (s *source) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Debugf("closing memory source")
	return closeSourceConsumerChannel(s.topic, fmt.Sprintf("%s_%s_%d", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId()))
}
