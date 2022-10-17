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

package processor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"io"
)

type RulesetProcessor struct {
	r *RuleProcessor
	s *StreamProcessor
}

type ruleset struct {
	Streams map[string]string `json:"streams"`
	Tables  map[string]string `json:"tables"`
	Rules   map[string]string `json:"rules"`
}

func NewRulesetProcessor(r *RuleProcessor, s *StreamProcessor) *RulesetProcessor {
	return &RulesetProcessor{
		r: r,
		s: s,
	}
}

func (rs *RulesetProcessor) Export() (io.ReadSeeker, []int, error) {
	var all ruleset
	allStreams, err := rs.s.GetAll()
	if err != nil {
		return nil, nil, fmt.Errorf("fail to get all streams: %v", err)
	}
	all.Streams = allStreams["streams"]
	all.Tables = allStreams["tables"]
	rules, err := rs.r.GetAllRulesJson()
	if err != nil {
		return nil, nil, fmt.Errorf("fail to get all rules: %v", err)
	}
	all.Rules = rules
	jsonBytes, err := json.Marshal(all)
	if err != nil {
		return nil, nil, err
	}
	counts := []int{len(all.Streams), len(all.Tables), len(all.Rules)}
	return bytes.NewReader(jsonBytes), counts, nil
}

func (rs *RulesetProcessor) Import(content []byte) ([]string, []int, error) {
	all := &ruleset{}
	err := json.Unmarshal(content, all)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid import file: %v", err)
	}
	counts := make([]int, 3)
	// restore streams
	for k, v := range all.Streams {
		_, e := rs.s.ExecStreamSql(v)
		if e != nil {
			conf.Log.Errorf("Fail to import stream %s(%s) with error: %v", k, v, e)
		} else {
			counts[0]++
		}
	}
	// restore tables
	for k, v := range all.Tables {
		_, e := rs.s.ExecStreamSql(v)
		if e != nil {
			conf.Log.Errorf("Fail to import table %s(%s) with error: %v", k, v, e)
		} else {
			counts[1]++
		}
	}
	var rules []string
	// restore rules
	for k, v := range all.Rules {
		_, e := rs.r.ExecCreate(k, v)
		if e != nil {
			conf.Log.Errorf("Fail to import rule %s(%s) with error: %v", k, v, e)
		} else {
			rules = append(rules, k)
			counts[2]++
		}
	}
	return rules, counts, nil
}
