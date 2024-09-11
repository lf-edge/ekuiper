// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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
	"regexp"
	"strings"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type c struct {
	Topic        string `json:"datasource"`
	BufferLength int    `json:"bufferLength"`
}

type source struct {
	topicRegex *regexp.Regexp
	c          *c
}

func (s *source) Provision(ctx api.StreamContext, props map[string]any) error {
	cfg := &c{
		BufferLength: 1024,
	}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if cfg.Topic == "" {
		return fmt.Errorf("topic is required")
	}
	if strings.ContainsAny(cfg.Topic, "+#") {
		r, err := getRegexp(cfg.Topic)
		if err != nil {
			return err
		}
		s.topicRegex = r
	}
	s.c = cfg
	return nil
}

func (s *source) Connect(_ api.StreamContext, sch api.StatusChangeHandler) error {
	sch(api.ConnectionConnected, "")
	return nil
}

// Subscribe For memory source, it can receive a source tuple directly. So just pass it through
func (s *source) Subscribe(ctx api.StreamContext, ingest api.TupleIngest, ingestErr api.ErrorIngest) error {
	ch := pubsub.CreateSub(s.c.Topic, s.topicRegex, fmt.Sprintf("%s_%s_%d", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId()), s.c.BufferLength)
	ctx.GetLogger().Infof("Subscribe to topic %s", s.c.Topic)
	go func() {
		for {
			select {
			case v := <-ch:
				e := infra.SafeRun(func() error {
					ingest(ctx, v, nil, timex.GetNow())
					return nil
				})
				if e != nil {
					ingestErr(ctx, e)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

func getRegexp(topic string) (*regexp.Regexp, error) {
	if len(topic) == 0 {
		return nil, fmt.Errorf("invalid empty topic")
	}

	levels := strings.Split(topic, "/")
	for i, level := range levels {
		if level == "#" && i != len(levels)-1 {
			return nil, fmt.Errorf("invalid topic %s: # must at the last level", topic)
		}
	}
	regstr := strings.Replace(strings.ReplaceAll(topic, "+", "([^/]+)"), "#", ".", 1)
	return regexp.Compile(regstr)
}

func (s *source) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Debugf("closing memory source")
	pubsub.CloseSourceConsumerChannel(s.c.Topic, fmt.Sprintf("%s_%s_%d", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId()))
	return nil
}

func GetSource() api.TupleSource {
	return &source{}
}
