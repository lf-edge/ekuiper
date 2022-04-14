// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/pkg/cast"
	"regexp"
	"strings"
)

type source struct {
	topic        string
	topicRegex   *regexp.Regexp
	bufferLength int
}

func (s *source) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, _ chan<- error) {
	ch := createSub(s.topic, s.topicRegex, fmt.Sprintf("%s_%s_%d", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId()), s.bufferLength)
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

func (s *source) Configure(datasource string, props map[string]interface{}) error {
	s.topic = datasource
	s.bufferLength = 1024
	if c, ok := props["bufferLength"]; ok {
		if bl, err := cast.ToInt(c, cast.STRICT); err != nil || bl > 0 {
			s.bufferLength = bl
		}
	}
	if strings.ContainsAny(datasource, "+#") {
		r, err := getRegexp(datasource)
		if err != nil {
			return err
		}
		s.topicRegex = r
	}
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
	closeSourceConsumerChannel(s.topic, fmt.Sprintf("%s_%s_%d", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId()))
	return nil
}
