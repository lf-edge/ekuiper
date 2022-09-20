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

package memory

import (
	"github.com/lf-edge/ekuiper/internal/topo/memory/store"
	"github.com/lf-edge/ekuiper/pkg/api"
	"regexp"
	"strings"
)

// lookupsource is a lookup source that reads data from memory
// The memory lookup table reads a global memory store for data
type lookupsource struct {
	topic      string
	topicRegex *regexp.Regexp
	key        string
	keys       []string
	table      *store.Table
}

func (s *lookupsource) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("lookup source %s is opened with keys %v", s.topic, s.keys)
	var err error
	s.table, err = store.Reg(s.topic, s.topicRegex, s.key, s.keys)
	return err
}

func (s *lookupsource) Configure(datasource string, _ map[string]interface{}, keys []string) error {
	s.topic = datasource
	if strings.ContainsAny(datasource, "+#") {
		r, err := getRegexp(datasource)
		if err != nil {
			return err
		}
		s.topicRegex = r
	}
	s.keys = keys
	s.key = strings.Join(keys, ",")
	return nil
}

func (s *lookupsource) Lookup(ctx api.StreamContext, values []interface{}) ([]api.SourceTuple, error) {
	ctx.GetLogger().Debugf("lookup source %s_%s is looking up %v", s.topic, s.key, values)
	return s.table.Read(values)
}

func (s *lookupsource) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("lookup source %s_%s is closing", s.topic, s.key)
	return store.Unreg(s.topic, s.key)
}
