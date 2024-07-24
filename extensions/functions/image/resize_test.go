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

package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestResize(t *testing.T) {
	ctx := mockContext.NewMockContext("testResize", "p[")
	fctx := kctx.NewDefaultFuncContext(ctx, 2)
	err := ResizeWithChan.Validate([]any{})
	assert.EqualError(t, err, "The resize function must have at least 3 parameters, but got 0")
	isAgg := ResizeWithChan.IsAggregate()
	assert.False(t, isAgg)
	payload, err := os.ReadFile("img.png")
	assert.NoError(t, err)
	resized, err := os.ReadFile("resized.png")
	assert.NoError(t, err)
	tests := []struct {
		n    string
		args []any
		e    string
		r    []byte
	}{
		{
			n:    "normal",
			args: []any{payload, 100, 100},
		},
		{
			n:    "wrong payload",
			args: []any{"img.png", 100, 100},
			e:    "arg[0] is not a bytea, got img.png",
		},
		{
			n:    "wrong width",
			args: []any{payload, "100", 100},
			e:    "arg[1] is not a bigint, got 100",
		},
		{
			n:    "wrong height",
			args: []any{payload, 100, "100"},
			e:    "arg[2] is not a bigint, got 100",
		},
		{
			n:    "wrong raw",
			args: []any{payload, 100, 100, 1},
			e:    "arg[3] is not a bool, got 1",
		},
		{
			n:    "not image",
			args: []any{[]byte{0x1, 0x2}, 100, 100, false},
			e:    "image decode error:image: unknown format",
		},
		{
			n:    "raw",
			args: []any{payload, 4, 4, true},
			r:    []byte{0x3c, 0x40, 0x4b, 0x39, 0x3e, 0x4a, 0x39, 0x3e, 0x4a, 0x38, 0x3d, 0x47, 0x36, 0x3a, 0x44, 0x35, 0x39, 0x44, 0x35, 0x39, 0x44, 0x35, 0x39, 0x43, 0x3a, 0x3e, 0x47, 0x3a, 0x3e, 0x49, 0x3a, 0x3d, 0x48, 0x38, 0x3c, 0x46, 0x33, 0x37, 0x40, 0x35, 0x3a, 0x43, 0x37, 0x40, 0x46, 0x32, 0x36, 0x3f},
		},
	}
	for _, tt := range tests {
		t.Run(tt.n, func(t *testing.T) {
			result, success := ResizeWithChan.Exec(tt.args, fctx)
			if tt.e == "" {
				assert.True(t, success)
				if tt.r != nil {
					assert.Equal(t, tt.r, result)
				} else {
					assert.Equal(t, resized, result)
				}
			} else {
				assert.EqualError(t, result.(error), tt.e)
			}
		})
	}
}
