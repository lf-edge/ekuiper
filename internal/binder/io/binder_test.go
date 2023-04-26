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

package io

import (
	"errors"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/binder"
	"github.com/lf-edge/ekuiper/internal/binder/mock"
	"github.com/lf-edge/ekuiper/pkg/errorx"
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

func TestSource(t *testing.T) {
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
		isSrc   bool
		wantErr bool
		errs    error
	}{
		{
			name: "mockFunc1",
			args: args{
				name: "mock",
			},
			isSrc:   true,
			wantErr: false,
			errs:    nil,
		},
		{
			name: "mockFunc2",
			args: args{
				name: "echo",
			},
			isSrc:   false,
			wantErr: true,
			errs:    errors.Join(fmt.Errorf("mock1: %v", errorx.NotFoundErr), fmt.Errorf("mock2: %v", errorx.NotFoundErr)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src, err := Source(tt.args.name)
			if (src != nil) != tt.isSrc {
				t.Errorf("Source() src = %v, isSrc = %v", src, tt.isSrc)
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("Source() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				if errors.Is(err, tt.errs) {
					t.Errorf("Source() error = %v, wantErr %v", err.Error(), tt.errs)
				}
			}
		})
	}
}

func TestSink(t *testing.T) {
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
		isSink  bool
		wantErr bool
		errs    error
	}{
		{
			name: "mockFunc1",
			args: args{
				name: "mock",
			},
			isSink:  true,
			wantErr: false,
			errs:    nil,
		},
		{
			name: "mockFunc2",
			args: args{
				name: "echo",
			},
			isSink:  false,
			wantErr: true,
			errs:    errors.Join(fmt.Errorf("mock1: %v", errorx.NotFoundErr), fmt.Errorf("mock2: %v", errorx.NotFoundErr)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sink, err := Sink(tt.args.name)
			if (sink != nil) != tt.isSink {
				t.Errorf("Sink() sink = %v, isSink = %v", sink, tt.isSink)
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("Sink() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				if errors.Is(err, tt.errs) {
					t.Errorf("Sink() error = %v, wantErr %v", err.Error(), tt.errs)
				}
			}
		})
	}
}

func TestLookupSource(t *testing.T) {
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
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		args    args
		isSrc   bool
		wantErr bool
		errs    error
	}{
		{
			name: "mockFunc1",
			args: args{
				name: "mock",
			},
			isSrc:   false,
			wantErr: true,
			errs:    fmt.Errorf("lookup source type mock not found"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src, err := LookupSource(tt.args.name)
			if (src != nil) != tt.isSrc {
				t.Errorf("LookupSource() src = %v, isSrc = %v", src, tt.isSrc)
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("LookupSource() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				if errors.Is(err, tt.errs) {
					t.Errorf("LookupSource() error = %v, wantErr %v", err.Error(), tt.errs)
				}
			}
		})
	}
}
