// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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

package io

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/binder"
	"github.com/lf-edge/ekuiper/internal/binder/mock"
	"testing"
)

func TestBindings(t *testing.T) {
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
		name           string
		isSource       bool
		isLookupSource bool
		isSink         bool
	}{
		{
			name:           "unknown",
			isSource:       false,
			isLookupSource: false,
			isSink:         false,
		}, {
			name:           "mqtt",
			isSource:       true,
			isLookupSource: false,
			isSink:         true,
		}, {
			name:           "mock1",
			isSource:       true,
			isLookupSource: false,
			isSink:         true,
		}, {
			name:           "rest",
			isSource:       false,
			isLookupSource: false,
			isSink:         true,
		}, {
			name:           "redis",
			isSource:       false,
			isLookupSource: true,
			isSink:         true,
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for _, tt := range tests {
		_, err := Source(tt.name)
		isSource := err == nil
		if tt.isSource != isSource {
			t.Errorf("%s is source: expect %v but got %v", tt.name, tt.isSource, isSource)
		}
		_, err = LookupSource(tt.name)
		if tt.isLookupSource != (err == nil) {
			t.Errorf("%s is lookup source: expect %v but got %v", tt.name, tt.isLookupSource, err == nil)
		}
		_, err = Sink(tt.name)
		isSink := err == nil
		if tt.isSink != isSink {
			t.Errorf("%s is sink: expect %v but got %v", tt.name, tt.isSink, isSink)
		}
	}
}
