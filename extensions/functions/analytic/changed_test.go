// Copyright 2025 EMQ Technologies Co., Ltd.
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

package analytic

import (
	"testing"

	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestNewChangeCaptureFunc(t *testing.T) {
	f := NewChangeCaptureFunc()
	require.NotNil(t, f, "NewChangeCaptureFunc should not return nil")

	cc, ok := f.(*changeCapture)
	require.True(t, ok, "NewChangeCaptureFunc should return *changeCapture")
	require.True(t, cc.ignoreNull, "Default ignoreNull should be true")
}

func TestNewChangeToFunc(t *testing.T) {
	f := NewChangeToFunc()
	require.NotNil(t, f, "NewChangeToFunc should not return nil")

	_, ok := f.(*changedTo)
	require.True(t, ok, "NewChangeToFunc should return *changedTo")
}

func TestChangeCapture_Validate(t *testing.T) {
	tests := []struct {
		name    string
		args    []any
		wantErr bool
	}{
		{
			name:    "valid with 3 args",
			args:    []any{"captureVal", "monitorVal"},
			wantErr: false,
		},
		{
			name:    "valid with 4 args",
			args:    []any{"captureVal", "monitorVal", "targetVal"},
			wantErr: false,
		},
		{
			name:    "invalid args count",
			args:    []any{"val1"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &changeCapture{}
			err := c.Validate(tt.args)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestChangeCapture_Exec(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		want     any
		wantBool bool
	}{
		{
			name: "first execution",
			args: []any{
				1,
				"monitorVal",
				true,
				"testKey",
			},
			want:     1,
			wantBool: true,
		},
		{
			name: "unchanged value",
			args: []any{
				2,
				"monitorVal",
				true,
				"testKey",
			},
			want:     1,
			wantBool: true,
		},
		{
			name: "changed value",
			args: []any{
				3,
				"newMonitorVal",
				true,
				"testKey",
			},
			want:     3,
			wantBool: true,
		},
		{
			name: "changed value",
			args: []any{
				4,
				"changeAgain",
				false,
				"testKey",
			},
			want:     3,
			wantBool: true,
		},
		{
			name: "ignore null value",
			args: []any{
				5,
				nil,
				true,
				"testKey",
			},
			want:     3,
			wantBool: true,
		},
		{
			name: "change to ignore value",
			args: []any{
				6,
				"changeAgain",
				true,
				"testKey",
			},
			want:     6,
			wantBool: true,
		},
	}

	ctx := mockContext.NewMockFuncContext("ruleC", "op1", 1)
	c := &changeCapture{ignoreNull: true, paraLen: 2}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotBool := c.Exec(ctx, tt.args)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.wantBool, gotBool)
		})
	}
}

func TestChangeToCapture_Exec(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		want     any
		wantBool bool
	}{
		{
			name: "first execution",
			args: []any{
				1,
				0,
				5,
				true,
				"testKey",
			},
			want:     nil,
			wantBool: true,
		},
		{
			name: "unchanged value",
			args: []any{
				2,
				2,
				5,
				true,
				"testKey",
			},
			want:     nil,
			wantBool: true,
		},
		{
			name: "changed value",
			args: []any{
				3,
				5.0,
				5,
				true,
				"testKey",
			},
			want:     3,
			wantBool: true,
		},
		{
			name: "ignore null value",
			args: []any{
				4,
				nil,
				5,
				true,
				"testKey",
			},
			want:     3,
			wantBool: true,
		},
		{
			name: "changed value but not match",
			args: []any{
				5,
				3,
				5,
				true,
				"testKey",
			},
			want:     3,
			wantBool: true,
		},
		{
			name: "change but ignore",
			args: []any{
				6,
				5,
				5,
				false,
				"testKey",
			},
			want:     3,
			wantBool: true,
		},
		{
			name: "change again compare to the one before",
			args: []any{
				7,
				5,
				5,
				true,
				"testKey",
			},
			want:     7,
			wantBool: true,
		},
	}

	ctx := mockContext.NewMockFuncContext("ruleC", "op1", 1)
	c := &changeCapture{ignoreNull: true, paraLen: 3}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotBool := c.Exec(ctx, tt.args)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.wantBool, gotBool)
		})
	}
}

func TestChangedTo_Validate(t *testing.T) {
	tests := []struct {
		name    string
		args    []any
		wantErr bool
	}{
		{
			name:    "valid with 4 args",
			args:    []any{"val1", "val2"},
			wantErr: false,
		},
		{
			name:    "valid with 5 args",
			args:    []any{"val1", "val2", "target"},
			wantErr: false,
		},
		{
			name:    "invalid args count",
			args:    []any{"val1"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &changedTo{}
			err := h.Validate(tt.args)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestChangedTo_Exec(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		want     any
		wantBool bool
	}{
		{
			name: "first execution",
			args: []any{
				1.0,
				1,
				true,
				"testKey",
			},
			want:     true,
			wantBool: true,
		},
		{
			name: "unchanged value",
			args: []any{
				2,
				1,
				true,
				"testKey",
			},
			want:     false,
			wantBool: true,
		},
		{
			name: "changed value",
			args: []any{
				3,
				1,
				true,
				"testKey",
			},
			want:     false,
			wantBool: true,
		},
		{
			name: "changed value with false condition",
			args: []any{
				1,
				1.0,
				false,
				"testKey",
			},
			want:     false,
			wantBool: true,
		},
		{
			name: "ignore null value",
			args: []any{
				nil,
				1,
				true,
				"testKey",
			},
			want:     false,
			wantBool: true,
		},
		{
			name: "change to new value",
			args: []any{
				1,
				1,
				true,
				"testKey",
			},
			want:     true,
			wantBool: true,
		},
	}

	ctx := mockContext.NewMockFuncContext("ruleC", "op1", 1)
	h := &changedTo{ignoreNull: true, paraLen: 2}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotBool := h.Exec(ctx, tt.args)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.wantBool, gotBool)
		})
	}
}
