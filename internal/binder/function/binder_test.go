// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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
	"errors"
	"fmt"
	"testing"

	"github.com/lf-edge/ekuiper/internal/binder"
	"github.com/lf-edge/ekuiper/internal/binder/mock"
	"github.com/lf-edge/ekuiper/pkg/errorx"
)

func TestBinding(t *testing.T) {
	// Initialize binding
	m := mock.NewMockFactory()
	e := binder.FactoryEntry{
		Name:    "mock",
		Factory: m,
	}
	err := Initialize([]binder.FactoryEntry{e})
	if err != nil {
		t.Error(err)
		return
	}
	var tests = []struct {
		name      string
		isFunc    bool
		isFuncset bool
		hasAgg    bool
		isAgg     bool
	}{
		{
			name:      "mockFunc1",
			isFunc:    true,
			isFuncset: true,
			hasAgg:    false,
		}, {
			name:      "mockFunc2",
			isFunc:    true,
			isFuncset: true,
			hasAgg:    false,
		}, {
			name:      "count",
			isFunc:    true,
			isFuncset: false,
			hasAgg:    true,
			isAgg:     true,
		}, {
			name:      "echo",
			isFunc:    false,
			isFuncset: false,
			hasAgg:    false,
		}, {
			name:      "internal",
			isFunc:    false,
			isFuncset: true,
			hasAgg:    false,
		}, {
			name:      "cast",
			isFunc:    true,
			isFuncset: false,
			hasAgg:    true,
			isAgg:     false,
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for _, tt := range tests {
		_, err := Function(tt.name)
		isFunc := err == nil
		if tt.isFunc != isFunc {
			t.Errorf("%s is function: expect %v but got %v", tt.name, tt.isFunc, isFunc)
		}
		isFuncset := HasFunctionSet(tt.name)
		if tt.isFuncset != isFuncset {
			t.Errorf("%s is function set: expect %v but got %v", tt.name, tt.isFuncset, isFuncset)
		}
		if tt.hasAgg {
			isAgg := IsAggFunc(tt.name)
			if tt.isAgg != isAgg {
				t.Errorf("%s is aggregate: expect %v but got %v", tt.name, tt.isAgg, isAgg)
			}
		}
	}
}

func TestFunction(t *testing.T) {
	m1 := mock.NewMockFactory()
	m2 := mock.NewMockFactory()
	e1 := binder.FactoryEntry{
		Name:    "mock1",
		Factory: m1,
	}
	e2 := binder.FactoryEntry{
		Name:    "mock2",
		Factory: m2,
	}
	err := Initialize([]binder.FactoryEntry{e1, e2})
	if err != nil {
		t.Error(err)
		return
	}
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		args    args
		isFunc  bool
		wantErr bool
		errs    error
	}{
		{
			name: "mockFunc1",
			args: args{
				name: "mock",
			},
			isFunc:  true,
			wantErr: false,
			errs:    nil,
		},
		{
			name: "mockFunc2",
			args: args{
				name: "echo",
			},
			isFunc:  false,
			wantErr: true,
			errs:    errors.Join(fmt.Errorf("%s:%v", "mock1", errorx.NotFoundErr), fmt.Errorf("%s:%v", "mock2", errorx.NotFoundErr)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			function, err := Function(tt.args.name)
			if (function != nil) != tt.isFunc {
				t.Errorf("Function() function = %v, isFunc %v", function, tt.isFunc)
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("Function() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				if errors.Is(err, tt.errs) {
					t.Errorf("Function() error = %v, wantErr %v", err.Error(), tt.errs.Error())
				}
			}
		})
	}
}
