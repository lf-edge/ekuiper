// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestNewEncryptOp(t *testing.T) {
	_, err := NewEncryptOp("test", &def.RuleOption{}, "non", nil)
	assert.Error(t, err)
	assert.Equal(t, "get encryptor non fail with error: encryptor 'non' is not supported", err.Error())
	if conf.Config != nil {
		conf.Config.AesKey = nil
	}
	_, err = NewEncryptOp("test", &def.RuleOption{}, "aes", nil)
	assert.Error(t, err)
	assert.Equal(t, errors.New("AES key is not defined"), err)
}

func TestEncryptOp_Exec(t *testing.T) {
	conf.InitConf()
	op, err := NewEncryptOp("test", &def.RuleOption{BufferLength: 10, SendError: true}, "aes", nil)
	assert.NoError(t, err)
	op.tool = &MockEncryptor{}
	out := make(chan any, 100)
	err = op.AddOutput(out, "test")
	assert.NoError(t, err)
	ctx := mockContext.NewMockContext("test1", "compress_test")
	errCh := make(chan error)
	op.Exec(ctx, errCh)

	cases := []any{
		&xsql.RawTuple{Rawdata: []byte("{\"a\":1,\"b\":2}")},
		errors.New("go through error"),
		"invalid",
		&xsql.RawTuple{Rawdata: []byte(`{"age":20,"name":"joe"}`), Metadata: map[string]any{"topic": "demo"}, Props: map[string]string{"{{.a}}": "1"}},
	}
	expects := [][]any{
		{&xsql.RawTuple{Rawdata: []byte("mock encrypt")}},
		{errors.New("go through error")},
		{errors.New("unsupported data received: invalid")},
		{&xsql.RawTuple{Rawdata: []byte("mock encrypt"), Metadata: map[string]any{"topic": "demo"}, Props: map[string]string{"{{.a}}": "1"}}},
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

type MockEncryptor struct{}

func (m *MockEncryptor) Encrypt(_ []byte) ([]byte, error) {
	return []byte("mock encrypt"), nil
}
