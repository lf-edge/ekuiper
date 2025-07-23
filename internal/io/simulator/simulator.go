// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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

package simulator

import (
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

type SimulatorSource struct {
	index int
	cfg   *sConfig
	eof   api.EOFIngest
}

type sConfig struct {
	Data []map[string]any `json:"data"`
	Loop bool             `json:"loop"`
}

func (s *SimulatorSource) Provision(ctx api.StreamContext, configs map[string]any) error {
	cfg := &sConfig{}
	if err := cast.MapToStruct(configs, cfg); err != nil {
		return err
	}
	s.cfg = cfg
	return nil
}

func (s *SimulatorSource) Close(ctx api.StreamContext) error {
	// Allow to reset in close rule trial run
	s.index = 0
	return nil
}

func (s *SimulatorSource) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	sch(api.ConnectionConnected, "")
	return nil
}

func (s *SimulatorSource) SetEofIngest(eof api.EOFIngest) {
	s.eof = eof
}

func (s *SimulatorSource) Pull(ctx api.StreamContext, trigger time.Time, ingest api.TupleIngest, _ api.ErrorIngest) {
	if s.index >= len(s.cfg.Data) {
		if s.cfg.Loop {
			s.index = 0
		} else {
			if s.eof != nil {
				s.eof(ctx, "")
			}
			return
		}
	}
	ingest(ctx, s.cfg.Data[s.index], nil, trigger)
	s.index++
}

func GetSource() api.Source {
	return &SimulatorSource{}
}

var _ api.PullTupleSource = &SimulatorSource{}
