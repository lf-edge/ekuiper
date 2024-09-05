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

package mock

import (
	"strings"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/plugin"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
)

type MockFactory struct{}

func NewMockFactory() *MockFactory {
	return &MockFactory{}
}

func (f *MockFactory) Source(name string) (api.Source, error) {
	if strings.HasPrefix(name, "mock") {
		return &mockSource{}, nil
	} else {
		return nil, errorx.NotFoundErr
	}
}

func (f *MockFactory) SourcePluginInfo(_ string) (plugin.EXTENSION_TYPE, string, string) {
	return plugin.INTERNAL, "", ""
}

func (f *MockFactory) LookupSource(name string) (api.Source, error) {
	return nil, nil
}

func (f *MockFactory) Sink(name string) (api.Sink, error) {
	if strings.HasPrefix(name, "mock") {
		return &mockSink{}, nil
	} else {
		return nil, errorx.NotFoundErr
	}
}

func (f *MockFactory) SinkPluginInfo(_ string) (plugin.EXTENSION_TYPE, string, string) {
	return plugin.INTERNAL, "", ""
}

func (f *MockFactory) Function(name string) (api.Function, error) {
	if strings.HasPrefix(name, "mock") {
		return &mockFunc{}, nil
	} else {
		return nil, errorx.NotFoundErr
	}
}

func (f *MockFactory) FunctionPluginInfo(funcName string) (plugin.EXTENSION_TYPE, string, string) {
	return plugin.NONE_EXTENSION, "", ""
}

func (f *MockFactory) ConvName(name string) (string, bool) {
	return name, true
}

func (f *MockFactory) HasFunctionSet(funcName string) bool {
	if strings.HasPrefix(funcName, "mock") {
		return true
	} else {
		return false
	}
}

type mockFunc struct{}

func (m *mockFunc) Validate(_ []interface{}) error {
	return nil
}

func (m *mockFunc) Exec(ctx api.FunctionContext, args []any) (interface{}, bool) {
	return nil, true
}

func (m *mockFunc) IsAggregate() bool {
	return false
}

type mockSource struct{}

func (m *mockSource) Provision(ctx api.StreamContext, configs map[string]any) error {
	return nil
}

func (m *mockSource) Connect(ctx api.StreamContext, _ api.StatusChangeHandler) error {
	return nil
}

func (m *mockSource) Close(_ api.StreamContext) error {
	return nil
}

func (m *mockSource) Subscribe(ctx api.StreamContext, ingest api.TupleIngest, ingestError api.ErrorIngest) error {
	return nil
}

type mockSink struct{}

func (m *mockSink) Provision(ctx api.StreamContext, configs map[string]any) error {
	return nil
}

func (m *mockSink) Connect(ctx api.StreamContext, _ api.StatusChangeHandler) error {
	return nil
}

func (m *mockSink) Collect(_ api.StreamContext, _ interface{}) error {
	return nil
}

func (m *mockSink) Close(_ api.StreamContext) error {
	return nil
}
