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
	"github.com/lf-edge/ekuiper/pkg/cast"
	"regexp"
	"strings"
)

// lookupsource is a lookup source that reads data from memory
// The memory lookup table reads a global memory store for data
type lookupsource struct {
	topic      string
	topicRegex *regexp.Regexp
	keys       []string
	table      *store.Table
}

func (s *lookupsource) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("lookup source %s is opened with keys %v", s.topic, s.keys)
	var err error
	s.table, err = store.Reg(s.topic, s.topicRegex, s.keys)
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
	if c, ok := props["index"]; ok {
		if bl, err := cast.ToStringSlice(c, cast.CONVERT_SAMEKIND); err != nil {
			s.keys = bl
		}
	}
	return nil
}

func (s *lookupsource) Lookup(ctx api.StreamContext, _ []string, keys []string, values []interface{}) ([]api.SourceTuple, error) {
	ctx.GetLogger().Debugf("lookup source %s is looking up keys %v with values %v", s.topic, keys, values)
	return s.table.Read(keys, values)
}

func (s *lookupsource) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("lookup source %s is closing", s.topic)
	return store.Unreg(s.topic)
}
