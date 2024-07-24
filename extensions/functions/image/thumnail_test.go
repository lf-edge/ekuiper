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

package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestThumbnail(t *testing.T) {
	ctx := mockContext.NewMockContext("testResize", "p[")
	fctx := kctx.NewDefaultFuncContext(ctx, 2)
	err := Thumbnail.Validate([]any{})
	assert.EqualError(t, err, "The thumbnail function supports 3 parameters, but got 0")
	isAgg := Thumbnail.IsAggregate()
	assert.False(t, isAgg)
	payload, err := os.ReadFile("img.png")
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
			n:    "not image",
			args: []any{[]byte{0x1, 0x2}, 100, 100, false},
			e:    "image decode error:image: unknown format",
		},
	}
	for _, tt := range tests {
		t.Run(tt.n, func(t *testing.T) {
			result, success := Thumbnail.Exec(tt.args, fctx)
			if tt.e == "" {
				assert.True(t, success)
				if tt.r != nil {
					assert.Equal(t, tt.r, result)
				}
			} else {
				assert.EqualError(t, result.(error), tt.e)
			}
		})
	}
}
