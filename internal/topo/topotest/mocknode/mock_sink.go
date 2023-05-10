// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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
	"fmt"

	"github.com/lf-edge/ekuiper/pkg/api"
)

type MockSink struct {
	results [][]byte
}

func NewMockSink() *MockSink {
	m := &MockSink{}
	return m
}

func (m *MockSink) Open(ctx api.StreamContext) error {
	log := ctx.GetLogger()
	log.Debugln("Opening mock sink")
	m.results = make([][]byte, 0)
	return nil
}

func (m *MockSink) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	fmt.Println("mock sink receive ", item)
	if v, _, err := ctx.TransformOutput(item, true); err == nil {
		logger.Debugf("mock sink receive %s", item)
		m.results = append(m.results, v)
	} else {
		logger.Info("mock sink transform data error: %v", err)
	}
	return nil
}

func (m *MockSink) Close(ctx api.StreamContext) error {
	//do nothing
	return nil
}

func (m *MockSink) Configure(props map[string]interface{}) error {
	return nil
}

func (m *MockSink) GetResults() [][]byte {
	return m.results
}
