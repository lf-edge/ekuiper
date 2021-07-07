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

import "github.com/lf-edge/ekuiper/pkg/api"

type memory struct {
	results [][]byte
}

func (m *memory) Open(ctx api.StreamContext) error {
	log := ctx.GetLogger()
	log.Debug("Opening memory sink")
	m.results = make([][]byte, 0)
	return nil
}

func (m *memory) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	if v, ok := item.([]byte); ok {
		logger.Debugf("memory sink receive %s", item)
		m.results = append(m.results, v)
	} else {
		logger.Debug("memory sink receive non byte data")
	}
	return nil
}

func (m *memory) Close(ctx api.StreamContext) error {
	//do nothing
	return nil
}

func (m *memory) Configure(props map[string]interface{}) error {
	return nil
}

func Memory() api.Sink {
	return &memory{}
}
