// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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

package mocknode

import (
	"github.com/lf-edge/ekuiper/contract/v2/api"
)

type MockSink struct {
	results [][]byte
}

func NewMockSink() *MockSink {
	m := &MockSink{}
	return m
}

func (m *MockSink) Info() *api.ModuleInfo {
	return &api.ModuleInfo{
		Id:          "mocksink",
		Description: "A mock sink for testing",
		New:         func() api.Node { return NewMockSink() },
	}
}

func (m *MockSink) Provision(ctx api.StreamContext, _ map[string]any) error {
	ctx.GetLogger().Infof("Mock sink is provisioned")
	return nil
}

func (m *MockSink) Validate(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Mock sink is validated")
	return nil
}

func (m *MockSink) Connect(ctx api.StreamContext) error {
	log := ctx.GetLogger()
	log.Debug("Opening mock sink")
	m.results = make([][]byte, 0)
	return nil
}

func (m *MockSink) Collect(ctx api.StreamContext, item []byte) error {
	ctx.GetLogger().Debugf("Mock sink is collecting %s", string(item))
	m.results = append(m.results, item)
	return nil
}

func (m *MockSink) Close(_ api.StreamContext) error {
	// do nothing
	return nil
}

func (m *MockSink) Configure(_ map[string]interface{}) error {
	return nil
}

func (m *MockSink) GetResults() [][]byte {
	return m.results
}
