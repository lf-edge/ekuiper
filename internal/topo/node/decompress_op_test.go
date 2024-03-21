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

package node

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	mockContext "github.com/lf-edge/ekuiper/pkg/mock/context"
)

func TestNewDecompressOp(t *testing.T) {
	_, err := NewDecompressOp("test", &api.RuleOption{}, "non")
	assert.Error(t, err)
	assert.Equal(t, "get decompressor non fail with error: unsupported decompressor: non", err.Error())
	_, err = NewDecompressOp("test", &api.RuleOption{}, "gzip")
	assert.NoError(t, err)
}

func TestDecompressOp_Exec(t *testing.T) {
	op, err := NewDecompressOp("test", &api.RuleOption{BufferLength: 10, SendError: true}, "gzip")
	assert.NoError(t, err)
	op.tool = &MockCompresser{}
	out := make(chan any, 100)
	err = op.AddOutput(out, "test")
	assert.NoError(t, err)
	ctx := mockContext.NewMockContext("test1", "decode_test")
	errCh := make(chan error)
	op.Exec(ctx, errCh)

	cases := []any{
		&xsql.Tuple{Emitter: "test", Raw: []byte("{\"a\":1,\"b\":2}"), Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}},
		errors.New("go through error"),
		"invalid",
	}
	expects := [][]any{
		{&xsql.Tuple{Emitter: "test", Raw: []byte("mock decompress"), Timestamp: 111, Metadata: map[string]any{"topic": "demo", "qos": 1}}},
		{errors.New("go through error")},
		{errors.New("unsupported data received: invalid")},
	}

	for i, c := range cases {
		op.input <- c
		for _, e := range expects[i] {
			r := <-out
			switch tr := r.(type) {
			case error:
				assert.EqualError(t, e.(error), tr.Error())
			default:
				assert.Equal(t, e, r)
			}
		}
	}
}

type MockCompresser struct{}

func (m *MockCompresser) Decompress(bytes []byte) ([]byte, error) {
	return []byte("mock decompress"), nil
}
