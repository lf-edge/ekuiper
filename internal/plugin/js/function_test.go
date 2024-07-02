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

package js

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
)

func TestScalarFuncHappyPath(t *testing.T) {
	script := &Script{
		Id:     "area",
		Desc:   "Test script",
		Script: "function area(x, y) { return x * y; }",
		IsAgg:  false,
	}
	err := GetManager().Create(script)
	assert.NoError(t, err)
	defer func() {
		err := GetManager().Delete("area")
		assert.NoError(t, err)
	}()
	ff, err := NewJSFunc("area")
	assert.NoError(t, err)
	err = ff.Validate([]interface{}{})
	assert.NoError(t, err)
	isAgg := ff.IsAggregate()
	assert.Equal(t, false, isAgg)

	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)

	tests := []struct {
		name string
		args []any
		want any
	}{
		{
			name: "normal",
			args: []any{2, 3},
			want: int64(6),
		},
		{
			name: "more args",
			args: []any{2, 3, 4},
			want: int64(6),
		},
		{
			name: "less args",
			args: []any{2},
			want: errors.New("result is NaN"),
		},
		{
			name: "float",
			args: []any{2.0, 3.0},
			want: int64(6),
		},
		{
			name: "float2",
			args: []any{2.5, 3.5},
			want: 8.75,
		},
		{
			name: "string",
			args: []any{"2", "3"},
			want: int64(6),
		},
		{
			name: "string float",
			args: []any{"2.0", 3.0},
			want: int64(6),
		},
		{
			name: "invalid string",
			args: []any{"myname", "hello"},
			want: errors.New("result is NaN"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, _ := ff.Exec(fctx, tt.args)
			assert.Equal(t, tt.want, result)
		})
	}
	err = ff.Close()
	assert.NoError(t, err)
}

func TestNonExistentScript(t *testing.T) {
	_, err := NewJSFunc("nonExistentScript")
	assert.NotNil(t, err)
}

func TestAggFuncHappyPath(t *testing.T) {
	script := &Script{
		Id:     "areas",
		Desc:   "Test script",
		Script: "function areas(x, y) { if(x.length !== y.length) {throw 'length of x and y should be the same'} let result = 0; for (let i = 0; i < x.length; i++) { result+=(x[i] * y[i]); } return result; }",
		IsAgg:  true,
	}
	err := GetManager().Create(script)
	assert.NoError(t, err)
	defer func() {
		err := GetManager().Delete("areas")
		assert.NoError(t, err)
	}()
	ff, err := NewJSFunc("areas")
	assert.NoError(t, err)
	err = ff.Validate([]interface{}{})
	assert.NoError(t, err)
	isAgg := ff.IsAggregate()
	assert.Equal(t, true, isAgg)

	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)

	tests := []struct {
		name string
		args []any
		want any
	}{
		{
			name: "normal",
			args: []any{[]any{2, 3}, []any{3, 4}},
			want: int64(18),
		},
		{
			name: "partial error",
			args: []any{[]any{2, 3}, []any{"ada", 4}},
			want: errors.New("result is NaN"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, _ := ff.Exec(fctx, tt.args)
			assert.Equal(t, tt.want, result)
		})
	}
	err = ff.Close()
	assert.NoError(t, err)
}

func TestScalarFuncComplexType(t *testing.T) {
	script := &Script{
		Id:     "area2",
		Desc:   "Test script",
		Script: "function area2(msg) { return msg.x * msg.y; }",
		IsAgg:  false,
	}
	err := GetManager().Create(script)
	assert.NoError(t, err)
	defer func() {
		err := GetManager().Delete("area2")
		assert.NoError(t, err)
	}()
	ff, err := NewJSFunc("area2")
	assert.NoError(t, err)
	err = ff.Validate([]interface{}{})
	assert.NoError(t, err)
	isAgg := ff.IsAggregate()
	assert.Equal(t, false, isAgg)

	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)

	tests := []struct {
		name string
		args []any
		want any
	}{
		{
			name: "normal",
			args: []any{map[string]any{
				"x": 2,
				"y": 3,
			}},
			want: int64(6),
		},
		{
			name: "more args",
			args: []any{map[string]any{
				"x": 2,
				"y": 3,
				"z": 4,
			}},
			want: int64(6),
		},
		{
			name: "invalid string",
			args: []any{map[string]any{
				"x": 2,
				"y": "ddd",
				"z": 4,
			}},
			want: errors.New("result is NaN"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, _ := ff.Exec(fctx, tt.args)
			assert.Equal(t, tt.want, result)
		})
	}
	err = ff.Close()
	assert.NoError(t, err)
}
