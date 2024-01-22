// Copyright 2023 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/errorx"
)

type MockResendSink struct {
	results       [][]byte
	resentResults [][]byte
	count         int
	onHit         chan int
}

func NewMockResendSink(onHit chan int) *MockResendSink {
	m := &MockResendSink{onHit: onHit}
	return m
}

func (m *MockResendSink) Open(ctx api.StreamContext) error {
	log := ctx.GetLogger()
	log.Debugln("Opening mock sink")
	m.results = make([][]byte, 0)
	m.resentResults = make([][]byte, 0)
	return nil
}

func (m *MockResendSink) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	defer func() {
		m.count++
		m.onHit <- m.count
	}()
	if m.count%2 == 0 {
		return errorx.NewIOErr(`mock io error`)
	}
	if v, _, err := ctx.TransformOutput(item); err == nil {
		logger.Debugf("mock sink receive %s", item)
		m.results = append(m.results, v)
	} else {
		logger.Info("mock sink transform data error: %v", err)
	}
	return nil
}

func (m *MockResendSink) CollectResend(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	if m.count%3 != 1 {
		return errorx.NewIOErr(`mock io error`)
	}
	if v, _, err := ctx.TransformOutput(item); err == nil {
		logger.Debugf("mock sink resend %s", item)
		m.resentResults = append(m.resentResults, v)
	} else {
		logger.Info("mock sink transform data error: %v", err)
	}
	return nil
}

func (m *MockResendSink) Close(_ api.StreamContext) error {
	// do nothing
	return nil
}

func (m *MockResendSink) Configure(_ map[string]interface{}) error {
	return nil
}

func (m *MockResendSink) GetResults() [][]byte {
	return m.results
}

func (m *MockResendSink) GetResendResults() [][]byte {
	return m.resentResults
}
