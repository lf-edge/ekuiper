// Copyright 2021-2026 EMQ Technologies Co., Ltd.
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

package random

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
)

const dedupStateKey = "input"

func init() {
	gob.Register([][]byte{})
}

type randomSourceConfig struct {
	Seed    int                    `json:"seed"`
	Pattern map[string]interface{} `json:"pattern"`
	// how long will the source trace for deduplication. If 0, deduplicate is disabled; if negative, deduplicate will be the whole lifetime
	Deduplicate int    `json:"deduplicate"`
	Format      string `json:"format"`
}

// Emit data randomly with only a string field
type randomSource struct {
	conf *randomSourceConfig
	list [][]byte
}

func (s *randomSource) Provision(ctx api.StreamContext, props map[string]any) error {
	cfg := &randomSourceConfig{
		Format: "json",
	}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if cfg.Pattern == nil {
		return fmt.Errorf("source `random` property `pattern` is required")
	}
	if cfg.Seed <= 0 {
		return fmt.Errorf("source `random` property `seed` must be a positive integer but got %d", cfg.Seed)
	}
	if !strings.EqualFold(cfg.Format, message.FormatJson) {
		return fmt.Errorf("random source only supports `json` format")
	}
	s.conf = cfg
	return nil
}

func (s *randomSource) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	logger := ctx.GetLogger()
	logger.Debugf("open random source with deduplicate %d", s.conf.Deduplicate)
	if s.conf.Deduplicate != 0 {
		// Read the legacy state key for checkpoint compatibility. Keep an
		// owned copy because the context state belongs to the checkpoint
		// object graph.
		list, err := ctx.GetState(dedupStateKey)
		if err != nil {
			return err
		}
		if list == nil {
			s.list = make([][]byte, 0)
		} else {
			if l, ok := list.([][]byte); ok {
				logger.Debugf("restore list %v", l)
				s.list = cloneDedupList(l)
			} else {
				s.list = make([][]byte, 0)
				logger.Warnf("random source gets invalid state, ignore it")
			}
		}
	}
	sch(api.ConnectionConnected, "")
	return nil
}

func (s *randomSource) Pull(ctx api.StreamContext, trigger time.Time, ingest api.TupleIngest, ingestError api.ErrorIngest) {
	next := randomize(s.conf.Pattern, s.conf.Seed)
	if s.conf.Deduplicate != 0 && s.isDup(ctx, next) {
		ctx.GetLogger().Debugf("find duplicate")
		return
	}
	ctx.GetLogger().Debugf("Send out data %v", next)
	ingest(ctx, next, nil, trigger)
}

func randomize(p map[string]interface{}, seed int) map[string]interface{} {
	r := make(map[string]interface{})
	for k, v := range p {
		// TODO other data types
		vi, err := cast.ToInt(v, cast.STRICT)
		if err != nil {
			break
		}
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
		if bytes.Equal(ns, ps) {
			logger.Debugf("got duplicate %s", ns)
			return true
		}
	}
	logger.Debugf("no duplicate %s", ns)
	if s.conf.Deduplicate > 0 && len(s.list) >= s.conf.Deduplicate {
		// Offsets already published to SourceNode are immutable. Build a new
		// outer slice before evicting an entry so a checkpoint cannot observe
		// the replacement through an older slice header. The byte entries are
		// immutable after json.Marshal and can be shared.
		limit := s.conf.Deduplicate
		nextList := make([][]byte, limit)
		copy(nextList, s.list[len(s.list)-(limit-1):])
		nextList[limit-1] = ns
		s.list = nextList
		return false
	}
	s.list = append(s.list, ns)
	return false
}

func (s *randomSource) GetOffset() (any, error) {
	if s.conf.Deduplicate == 0 {
		return nil, nil
	}
	// Restrict capacity so callers cannot reslice into storage that a later
	// append may reuse. Existing entries are never modified.
	return s.list[:len(s.list):len(s.list)], nil
}

func (s *randomSource) CheckpointOffsetIsImmutable() {}

func (s *randomSource) Rewind(offset any) error {
	if offset == nil {
		return nil
	}
	list, ok := offset.([][]byte)
	if !ok {
		return fmt.Errorf("random source dedup offset has invalid type %T", offset)
	}
	s.list = cloneDedupList(list)
	return nil
}

func (s *randomSource) ResetOffset(_ map[string]any) error {
	return fmt.Errorf("random source does not support resetting dedup offset")
}

func cloneDedupList(list [][]byte) [][]byte {
	cloned := make([][]byte, len(list))
	for i, item := range list {
		cloned[i] = bytes.Clone(item)
	}
	return cloned
}

func (s *randomSource) Close(_ api.StreamContext) error {
	return nil
}

func GetSource() api.Source {
	return &randomSource{}
}

var (
	_ api.PullTupleSource = &randomSource{}
	_ api.Rewindable      = &randomSource{}
)
