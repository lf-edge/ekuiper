// Copyright 2023-2024 EMQ Technologies Co., Ltd.
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

package simulator

import (
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/mock"
)

func TestSource_Configure(t *testing.T) {
	tests := []struct {
		name   string
		props  map[string]any
		fields *c
		errStr string
	}{
		{
			name: "valid",
			props: map[string]any{
				"data": []map[string]any{
					{"a": 1}, {"a": 2},
				},
				"interval": 100,
				"loop":     true,
			},
			fields: &c{
				Data: []map[string]any{
					{"a": 1}, {"a": 2},
				},
				Interval: 100,
				Loop:     true,
			},
		},
		{
			name: "invalid prop type",
			props: map[string]any{
				"data":     []map[string]any{},
				"interval": "10s",
				"loop":     true,
			},
			errStr: "1 error(s) decoding:\n\n* 'interval' expected type 'int', got unconvertible type 'string', value: '10s'",
		},
		{
			name: "no data",
			props: map[string]any{
				"data":     []map[string]any{},
				"interval": 100,
				"loop":     true,
			},
			errStr: "data cannot be empty",
		},
		{
			name: "invalid prop interval",
			props: map[string]any{
				"data":     []map[string]any{{"a": 1}, {"a": 2}},
				"interval": -4,
				"loop":     true,
			},
			errStr: "interval must be greater than 1 ms, got -4",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Source{}
			err := m.Configure("", tt.props)
			if err != nil {
				assert.Equal(t, tt.errStr, err.Error())
			} else {
				assert.Equal(t, tt.fields, m.c)
			}
		})
	}
}

func TestSource_Open(t *testing.T) {
	mc := conf.Clock.(*clock.Mock)
	exp := []api.SourceTuple{
		api.NewDefaultSourceTupleWithTime(map[string]interface{}{"a": 1, "b": 2}, nil, mc.Now()),
		api.NewDefaultSourceTupleWithTime(map[string]interface{}{"a": 2, "b": 4}, nil, mc.Now()),
		api.NewDefaultSourceTupleWithTime(map[string]interface{}{"a": 1, "b": 2}, nil, mc.Now()),
	}
	r := &Source{
		c: &c{
			Data: []map[string]any{
				{"a": 1, "b": 2}, {"a": 2, "b": 4},
			},
			Interval: 5,
			Loop:     true,
		},
	}
	mock.TestSourceOpen(r, exp, t)
}

func TestSourceNoLoop_Open(t *testing.T) {
	mc := conf.Clock.(*clock.Mock)
	exp := []api.SourceTuple{
		api.NewDefaultSourceTupleWithTime(map[string]interface{}{"a": 1, "b": 2}, nil, mc.Now()),
		api.NewDefaultSourceTupleWithTime(map[string]interface{}{"a": 2, "b": 4}, nil, mc.Now()),
	}
	r := &Source{
		c: &c{
			Data: []map[string]any{
				{"a": 1, "b": 2}, {"a": 2, "b": 4},
			},
			Interval: 5,
			Loop:     false,
		},
	}
	mock.TestSourceOpen(r, exp, t)
}
