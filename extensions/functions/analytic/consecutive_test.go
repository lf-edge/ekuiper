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

	"github.com/stretchr/testify/assert"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestConsecutiveCount_Validate(t *testing.T) {
	tests := []struct {
		name    string
		args    []interface{}
		wantErr bool
	}{
		{
			name:    "valid args",
			args:    []interface{}{true},
			wantErr: false,
		},
		{
			name:    "no args",
			args:    []interface{}{},
			wantErr: true,
		},
		{
			name:    "too many args",
			args:    []interface{}{true, true},
			wantErr: true,
		},
	}

	f := NewConsecutiveCountFunc()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := f.Validate(tt.args)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConsecutiveCount_Exec(t *testing.T) {
	tests := []struct {
		name       string
		condition  bool
		validData  bool
		key        string
		wantCount  int
		wantResult bool
	}{
		{
			name:       "condition true, valid data",
			condition:  true,
			validData:  true,
			key:        "test1",
			wantCount:  1,
			wantResult: true,
		},
		{
			name:       "condition false, no valid data",
			condition:  false,
			validData:  false,
			key:        "test1",
			wantCount:  1,
			wantResult: true,
		},
		{
			name:       "condition true, valid data",
			condition:  true,
			validData:  true,
			key:        "test1",
			wantCount:  2,
			wantResult: true,
		},
		{
			name:       "condition false, valid data",
			condition:  false,
			validData:  true,
			key:        "test1",
			wantCount:  0,
			wantResult: true,
		},
		{
			name:       "invalid data",
			condition:  true,
			validData:  false,
			key:        "test1",
			wantCount:  0,
			wantResult: true,
		},
		{
			name:       "condition true again, valid data",
			condition:  true,
			validData:  true,
			key:        "test1",
			wantCount:  1,
			wantResult: true,
		},
	}

	f := NewConsecutiveCountFunc()
	ctx := mockContext.NewMockFuncContext("ruleCC", "op1", 1)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := f.Exec(ctx, []any{tt.condition, tt.validData, tt.key})
			assert.Equal(t, tt.wantResult, ok)
			if ok {
				assert.Equal(t, tt.wantCount, result)
			}
		})
	}
}

func TestConsecutiveStart_Validate(t *testing.T) {
	tests := []struct {
		name    string
		args    []interface{}
		wantErr bool
	}{
		{
			name:    "valid args",
			args:    []interface{}{true, "value"},
			wantErr: false,
		},
		{
			name:    "no args",
			args:    []interface{}{},
			wantErr: true,
		},
		{
			name:    "too many args",
			args:    []interface{}{true, "value", true},
			wantErr: true,
		},
	}

	f := NewConsecutiveStartFunc()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := f.Validate(tt.args)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConsecutiveStart_Exec(t *testing.T) {
	tests := []struct {
		name       string
		condition  bool
		value      string
		validData  bool
		key        string
		wantValue  any
		wantResult bool
	}{
		{
			name:       "condition true, valid data",
			condition:  true,
			value:      "start1",
			validData:  true,
			key:        "test1",
			wantValue:  "start1",
			wantResult: true,
		},
		{
			name:       "condition false, no valid data",
			condition:  false,
			value:      "start2",
			validData:  false,
			key:        "test1",
			wantValue:  "start1",
			wantResult: true,
		},
		{
			name:       "condition true, valid data",
			condition:  true,
			value:      "start3",
			validData:  true,
			key:        "test1",
			wantValue:  "start1",
			wantResult: true,
		},
		{
			name:       "condition false, valid data false",
			condition:  false,
			value:      "start4",
			validData:  false,
			key:        "test1",
			wantValue:  "start1",
			wantResult: true,
		},
		{
			name:       "condition true, valid data",
			condition:  true,
			value:      "start3",
			validData:  true,
			key:        "test1",
			wantValue:  "start1",
			wantResult: true,
		},
		{
			name:       "condition false, valid data true",
			condition:  false,
			value:      "start5",
			validData:  true,
			key:        "test1",
			wantValue:  nil,
			wantResult: true,
		},
		{
			name:       "condition false, valid data true",
			condition:  false,
			value:      "start5",
			validData:  true,
			key:        "test1",
			wantValue:  nil,
			wantResult: true,
		},
		{
			name:       "condition true again, valid data",
			condition:  true,
			value:      "start6",
			validData:  true,
			key:        "test1",
			wantValue:  "start6",
			wantResult: true,
		},
	}

	f := NewConsecutiveStartFunc()
	ctx := mockContext.NewMockFuncContext("ruleCS", "op1", 1)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := f.Exec(ctx, []any{tt.condition, tt.value, tt.validData, tt.key})
			assert.Equal(t, tt.wantResult, ok)
			if ok {
				assert.Equal(t, tt.wantValue, result)
			}
		})
	}
}
