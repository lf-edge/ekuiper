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

	mockContext "github.com/lf-edge/ekuiper/internal/io/mock/context"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

func TestLC(t *testing.T) {
	op, err := NewDecodeOp("test", "test1", &api.RuleOption{BufferLength: 10, SendError: true}, &ast.Options{FORMAT: "json"}, false, true, nil)
	assert.NoError(t, err)
	out := make(chan any, 100)
	err = op.AddOutput(out, "test")
	assert.NoError(t, err)
	ctx := mockContext.NewMockContext("test1", "decode_test")
	errCh := make(chan error)
	op.Exec(ctx, errCh)

	cases := []*xsql.Tuple{
		{Emitter: "test", Raw: []byte("{\"a\":1,\"b\":2}"), Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}},
		{Emitter: "test", Raw: []byte("[{\"a\":1,\"b\":2},{\"a\":3,\"b\":4,\"c\":\"hello\"}]"), Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}},
	}
	expects := [][]*xsql.Tuple{
		{&xsql.Tuple{Emitter: "test", Message: map[string]interface{}{"a": 1.0, "b": 2.0}, Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}}},
		{
			&xsql.Tuple{Emitter: "test", Message: map[string]interface{}{"a": 1.0, "b": 2.0}, Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}},
			&xsql.Tuple{Emitter: "test", Message: map[string]interface{}{"a": 3.0, "b": 4.0, "c": "hello"}, Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}},
		},
	}

	for i, c := range cases {
		op.input <- c
		for _, e := range expects[i] {
			r := <-out
			assert.Equal(t, e, r)
		}
	}
}
