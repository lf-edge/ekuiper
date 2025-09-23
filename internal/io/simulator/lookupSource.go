// Copyright 2025 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

type SimulatorLookupSource struct {
	cfg *sLookupConfig
}

func (s *SimulatorLookupSource) Provision(ctx api.StreamContext, configs map[string]any) error {
	cfg := &sLookupConfig{
		Data: make([]map[string]any, 0),
	}
	if err := cast.MapToStruct(configs, cfg); err != nil {
		return err
	}
	s.cfg = cfg
	return nil
}

func (s *SimulatorLookupSource) Close(ctx api.StreamContext) error {
	return nil
}

func (s *SimulatorLookupSource) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	return nil
}

func (s *SimulatorLookupSource) Lookup(ctx api.StreamContext, lookupFields []string, cmpKeys []string, cmpValues []any) ([]map[string]any, error) {
	res := make([]map[string]any, 0)
	for _, d := range s.cfg.Data {
		for index, key := range cmpKeys {
			value, ok := d[key]
			if ok && value == cmpValues[index] {
				selectedRow := make(map[string]any)
				for _, field := range lookupFields {
					selectedRow[field] = d[field]
				}
				res = append(res, selectedRow)
			}
		}
	}
	return res, nil
}

type sLookupConfig struct {
	Data []map[string]any `json:"data"`
}
