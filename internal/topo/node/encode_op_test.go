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

package node

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/pkg/api"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestEncodeJSON(t *testing.T) {
	tests := []struct {
		name string
		in   any
		out  []byte
	}{
		{
			name: "normal",
			in:   map[string]any{"name": "joe", "age": 20},
			out:  []byte(`{"age":20,"name":"joe"}`),
		},
		{
			name: "list",
			in: []map[string]any{
				{"name": "joe", "age": 20},
				{"name": "tom", "age": 21},
			},
			out: []byte(`[{"age":20,"name":"joe"},{"age":21,"name":"tom"}]`),
		},
		{
			name: "unknown type",
			in:   12,
			out:  []byte(`12`),
		},
		{
			name: "bytes",
			in:   []byte("test"),
			out:  []byte("test"),
		},
	}
	op, err := NewEncodeOp("test", &api.RuleOption{BufferLength: 10, SendError: true}, &SinkConf{Format: "json"})
	assert.NoError(t, err)
	out := make(chan any, 100)
	err = op.AddOutput(out, "test")
	assert.NoError(t, err)
	ctx := mockContext.NewMockContext("test1", "transform_test")
	errCh := make(chan error)
	op.Exec(ctx, errCh)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op.input <- tt.in
			r := <-out
			assert.Equal(t, tt.out, r)
		})
	}
}

func TestEncodeValidate(t *testing.T) {
	_, err := NewEncodeOp("test", &api.RuleOption{BufferLength: 10, SendError: true}, &SinkConf{Format: "cann"})
	assert.Error(t, err)
	assert.Equal(t, "format type cann not supported", err.Error())
}
