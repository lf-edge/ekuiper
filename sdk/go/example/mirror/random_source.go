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

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"

	"github.com/lf-edge/ekuiper/sdk/go/api"
)

const dedupStateKey = "input"

type randomSourceConfig struct {
	Interval int                    `json:"interval"`
	Seed     int                    `json:"seed"`
	Pattern  map[string]interface{} `json:"pattern"`
	// how long will the source trace for deduplication. If 0, deduplicate is disabled; if negative, deduplicate will be the whole life time
	Deduplicate int    `json:"deduplicate"`
	Format      string `json:"format"`
}

// Emit data randomly with only a string field
type randomSource struct {
	conf *randomSourceConfig
	list [][]byte
}

func (s *randomSource) Configure(_ string, props map[string]interface{}) error {
	cfg := &randomSourceConfig{
		Format: "json",
	}
	err := mapstructure.Decode(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if cfg.Interval <= 0 {
		return fmt.Errorf("source `random` property `interval` must be a positive integer but got %d", cfg.Interval)
	}
	if cfg.Pattern == nil {
		return fmt.Errorf("source `random` property `pattern` is required")
	}
	if cfg.Seed <= 0 {
		return fmt.Errorf("source `random` property `seed` must be a positive integer but got %d", cfg.Seed)
	}
	if !strings.EqualFold(cfg.Format, "json") {
		return fmt.Errorf("random source only supports `json` format")
	}
	s.conf = cfg
	return nil
}

func (s *randomSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, _ chan<- error) {
	logger := ctx.GetLogger()
	logger.Infof("open random source with config %+v", s.conf)

	if s.conf.Deduplicate != 0 {
		var list interface{}
		// dedup not supported yet
		//list, err := ctx.GetState(dedupStateKey)
		//if err != nil {
		//	errCh <- err
		//	return
		//}
		if list == nil {
			list = make([][]byte, 0)
		} else {
			if l, ok := list.([][]byte); ok {
				logger.Debugf("restore list %v", l)
				s.list = l
			} else {
				s.list = make([][]byte, 0)
				logger.Warnf("random source gets invalid state, ignore it")
			}
		}
	}
	t := time.NewTicker(time.Duration(s.conf.Interval) * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			next := randomize(s.conf.Pattern, s.conf.Seed)
			if s.conf.Deduplicate != 0 && s.isDup(ctx, next) {
				logger.Debugf("find duplicate")
				continue
			}
			logger.Debugf("Send out data %v", next)
			consumer <- api.NewDefaultSourceTuple(next, nil)
		case <-ctx.Done():
			return
		}
	}
}

func randomize(p map[string]interface{}, seed int) map[string]interface{} {
	r := make(map[string]interface{})
	for k, v := range p {
		//TODO other data types
		vf, ok := v.(float64)
		if !ok {
			break
		}
		vi := int(vf)
		r[k] = vi + rand.Intn(seed)
	}
	return r
}

func (s *randomSource) isDup(ctx api.StreamContext, next map[string]interface{}) bool {
	logger := ctx.GetLogger()

	ns, err := json.Marshal(next)
	if err != nil {
		logger.Warnf("invalid input data %v", next)
		return true
	}
	for _, ps := range s.list {
		if bytes.Compare(ns, ps) == 0 {
			logger.Debugf("got duplicate %s", ns)
			return true
		}
	}
	logger.Debugf("no duplicate %s", ns)
	if s.conf.Deduplicate > 0 && len(s.list) >= s.conf.Deduplicate {
		s.list = s.list[1:]
	}
	s.list = append(s.list, ns)
	// State not supported yet
	// ctx.PutState(dedupStateKey, s.list)
	return false
}

func (s *randomSource) Close(_ api.StreamContext) error {
	return nil
}
