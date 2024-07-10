// Copyright 2024 EMQ Technologies Co., Ltd.
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

package operator

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestEmitterOpApply(t *testing.T) {
	tests := []struct {
		name   string
		data   any
		result any
	}{
		{
			name: "tuple",
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"a": int64(6),
				},
			},
			result: &xsql.Tuple{
				Emitter: "stream",
				Message: xsql.Message{
					"a": int64(6),
				},
			},
		},
		{
			name: "tuple",
			data: &xsql.RawTuple{
				Emitter: "tbl",
			},
			result: &xsql.RawTuple{
				Emitter: "stream",
			},
		},
		{
			name: "invalid",
			data: &xsql.Message{
				"Emitter": "tbl",
			},
			result: errors.New("run emitter op error: invalid input *xsql.Message(&map[Emitter:tbl])"),
		},
	}

	ctx := mockContext.NewMockContext("testOp", "emitter")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pp := &EmitterOp{Emitter: "stream"}
			result := pp.Apply(ctx, tt.data, nil, nil)
			assert.Equal(t, tt.result, result)
		})
	}
}
