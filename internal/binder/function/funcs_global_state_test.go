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

package function

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
)

func TestHitFuncs(t *testing.T) {
	f1, ok := builtins["last_hit_count"]
	if !ok {
		t.Fatal("builtin last_hit_count not found")
	}
	f2, ok := builtins["last_hit_time"]
	if !ok {
		t.Fatal("builtin last_hit_time not found")
	}
	f3, ok := builtins["last_agg_hit_count"]
	if !ok {
		t.Fatal("builtin last_agg_hit_count not found")
	}
	f4, ok := builtins["last_agg_hit_time"]
	if !ok {
		t.Fatal("builtin last_agg_hit_time not found")
	}
	funcs := []builtinFunc{f1, f2, f3, f4}
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 1)
	tests := []struct {
		name   string
		args   []any
		result []any
	}{
		{
			name: "first hit",
			args: []any{
				true,
				int64(10100),
			},
			result: []any{
				0, 0, 0, 0,
			},
		},
		{
			name: "second hit",
			args: []any{
				true,
				int64(10200),
			},
			result: []any{
				1, int64(10100), 1, int64(10100),
			},
		},
		{
			name: "third hit but not update",
			args: []any{
				false,
				int64(10300),
			},
			result: []any{
				2, int64(10200), 2, int64(10200),
			},
		},
		{
			name: "fourth hit",
			args: []any{
				true,
				int64(10400),
			},
			result: []any{
				2, int64(10200), 2, int64(10200),
			},
		},
		{
			name: "fifth hit, no update",
			args: []any{
				false,
				int64(10500),
			},
			result: []any{
				3, int64(10400), 3, int64(10400),
			},
		},
		{
			name: "sixth hit",
			args: []any{
				true,
				int64(10600),
			},
			result: []any{
				3, int64(10400), 3, int64(10400),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, re := range tt.result {
				result, _ := funcs[i].exec(fctx, tt.args)
				assert.Equal(t, re, result, "failed on %d", i)
			}
		})
	}
}
