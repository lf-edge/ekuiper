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

package memory

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/io/memory/store"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

type lc struct {
	Topic string `json:"datasource"`
	Key   string `json:"key"`
}

// lookupsource is a lookup source that reads data from memory
// The memory lookup table reads a global memory store for data
type lookupsource struct {
	topic      string
	topicRegex *regexp.Regexp
	table      *store.Table
	key        string
}

func (s *lookupsource) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	ctx.GetLogger().Infof("lookup source %s is opened with key %v", s.topic, s.key)
	var err error
	s.table, err = store.Reg(s.topic, s.topicRegex, s.key)
	if err != nil {
		sch(api.ConnectionDisconnected, err.Error())
		return err
	}
	sch(api.ConnectionConnected, "")
	return nil
}

func (s *lookupsource) Provision(ctx api.StreamContext, props map[string]any) error {
	cfg := &lc{}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if cfg.Topic == "" {
		return fmt.Errorf("datasource(topic) is required")
	}
	if strings.ContainsAny(cfg.Topic, "+#") {
		r, err := getRegexp(cfg.Topic)
		if err != nil {
			return err
		}
		s.topicRegex = r
	}
	if cfg.Key == "" {
		return fmt.Errorf("key is required for lookup source")
	}
	s.topic = cfg.Topic
	s.key = cfg.Key
	return nil
}

func (s *lookupsource) Lookup(ctx api.StreamContext, _ []string, keys []string, values []interface{}) ([]map[string]any, error) {
	ctx.GetLogger().Debugf("lookup source %s is looking up keys %v with values %v", s.topic, keys, values)
	tuples, err := s.table.Read(keys, values)
	if err != nil {
		return nil, err
	}
	r := make([]map[string]any, len(tuples))
	for i, t := range tuples {
		r[i] = t.ToMap()
	}
	return r, nil
}

func (s *lookupsource) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("lookup source %s is closing", s.topic)
	return store.Unreg(s.topic, s.key)
}

func GetLookupSource() api.Source {
	return &lookupsource{}
}
