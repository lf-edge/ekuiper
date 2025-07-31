// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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

//go:build test

package converter

import (
	"fmt"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/schema"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func init() {
	modules.RegisterConverter("mock", func(_ api.StreamContext, _ string, _ map[string]*ast.JsonStreamField, props map[string]any) (message.Converter, error) {
		return &MockConverter{}, nil
	})
	modules.RegisterSchemaType(modules.CUSTOM, &schema.CustomType{}, ".so")
	modules.RegisterConverterSchemas("mock", "protobuf")
}

// MockConverter mocks a slow converter for benchmark test
type MockConverter struct{}

func (m MockConverter) Encode(ctx api.StreamContext, d any) ([]byte, error) {
	time.Sleep(10 * time.Millisecond)
	now := timex.GetNowInMilli()
	return []byte(fmt.Sprintf(`{"temperature":23.4,"humidity":76,"ts": %d}`, now)), nil
}

func (m MockConverter) Decode(ctx api.StreamContext, b []byte) (any, error) {
	time.Sleep(10 * time.Millisecond)
	return map[string]any{
		"temperature": 23.4,
		"humidity":    76,
		"ts":          timex.GetNowInMilli(),
	}, nil
}
