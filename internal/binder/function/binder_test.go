// Copyright 2021 EMQ Technologies Co., Ltd.
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
	"fmt"
	"github.com/lf-edge/ekuiper/internal/binder"
	"github.com/lf-edge/ekuiper/internal/binder/mock"
	"testing"
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
