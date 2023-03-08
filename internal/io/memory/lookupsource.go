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

package memory

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/io/memory/store"
	"github.com/lf-edge/ekuiper/pkg/api"
	"regexp"
	"strings"
)

// lookupsource is a lookup source that reads data from memory
// The memory lookup table reads a global memory store for data
type lookupsource struct {
	topic      string
	topicRegex *regexp.Regexp
	table      *store.Table
	key        string
}

func (s *lookupsource) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("lookup source %s is opened with key %v", s.topic, s.key)
	var err error
	s.table, err = store.Reg(s.topic, s.topicRegex, s.key)
	return err
}

func (s *lookupsource) Configure(datasource string, props map[string]interface{}) error {
	s.topic = datasource
	if strings.ContainsAny(datasource, "+#") {
		r, err := getRegexp(datasource)
		if err != nil {
			return err
		}
		s.topicRegex = r
	}
	if k, ok := props["key"]; ok {
		if kk, ok := k.(string); ok {
			s.key = kk
		}
	}
	if s.key == "" {
		return fmt.Errorf("key is required for lookup source")
	}
	return nil
}

func (s *lookupsource) Lookup(ctx api.StreamContext, _ []string, keys []string, values []interface{}) ([]api.SourceTuple, error) {
	ctx.GetLogger().Debugf("lookup source %s is looking up keys %v with values %v", s.topic, keys, values)
	return s.table.Read(keys, values)
}

func (s *lookupsource) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("lookup source %s is closing", s.topic)
	return store.Unreg(s.topic, s.key)
}
