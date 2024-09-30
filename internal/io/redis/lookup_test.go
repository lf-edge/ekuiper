// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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

package redis

import (
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func init() {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	addr = "localhost:" + s.Port()
	// Mock id key data
	s.Set("1", `{"id":1,"name":"John","address":34,"mobile":"334433"}`)
	s.Set("2", `{"id":2,"name":"Susan","address":22,"mobile":"666433"}`)
	// Mock group key list data
	s.Lpush("group1", `{"id":1,"name":"John"}`)
	s.Lpush("group1", `{"id":2,"name":"Susan"}`)
	s.Lpush("group2", `{"id":3,"name":"Nancy"}`)
	s.Lpush("group3", `{"id":4,"name":"Tom"}`)
	mr = s
}

// TestSingle test lookup value of a single map
func TestSingle(t *testing.T) {
	ctx := mockContext.NewMockContext("test", "tt")
	ls := GetLookupSource()
	err := ls.Provision(ctx, map[string]any{"addr": addr, "datatype": "string", "datasource": "0"})
	if err != nil {
		t.Error(err)
		return
	}
	err = ls.Connect(ctx, func(status string, message string) {
		// do nothing
	})
	if err != nil {
		t.Error(err)
		return
	}
	tests := []struct {
		value  int
		result []map[string]any
	}{
		{
			value: 1,
			result: []map[string]any{
				{"id": float64(1), "name": "John", "address": float64(34), "mobile": "334433"},
			},
		}, {
			value: 2,
			result: []map[string]any{
				{"id": float64(2), "name": "Susan", "address": float64(22), "mobile": "666433"},
			},
		}, {
			value:  3,
			result: []map[string]any{},
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			actual, err := ls.(api.LookupSource).Lookup(ctx, []string{}, []string{"id"}, []any{tt.value})
			assert.NoError(t, err)
			assert.Equal(t, tt.result, actual)
		})
	}
}

func TestList(t *testing.T) {
	ctx := mockContext.NewMockContext("test", "tt")
	ls := GetLookupSource()
	err := ls.Provision(ctx, map[string]any{"addr": addr, "datatype": "list", "datasource": "0"})
	if err != nil {
		t.Error(err)
		return
	}
	err = ls.Connect(ctx, func(status string, message string) {
		// do nothing
	})
	if err != nil {
		t.Error(err)
		return
	}
	tests := []struct {
		value  string
		result []map[string]any
	}{
		{
			value: "group1",
			result: []map[string]any{
				{"id": float64(2), "name": "Susan"},
				{"id": float64(1), "name": "John"},
			},
		}, {
			value: "group2",
			result: []map[string]any{
				{"id": float64(3), "name": "Nancy"},
			},
		}, {
			value:  "group4",
			result: []map[string]any{},
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			actual, err := ls.(api.LookupSource).Lookup(ctx, []string{}, []string{"id"}, []any{tt.value})
			assert.NoError(t, err)
			assert.Equal(t, tt.result, actual)
		})
	}
}

func TestLookupSourceDB(t *testing.T) {
	ctx := mockContext.NewMockContext("test", "tt")
	s := &lookupSource{}
	err := s.Provision(ctx, map[string]any{"addr": addr, "datatype": "string", "datasource": "199"})
	require.Error(t, err)
	require.Equal(t, "redis lookup source db should be in range 0-15", err.Error())
}

func TestLookUpPingRedis(t *testing.T) {
	s := &lookupSource{}
	prop := map[string]interface{}{
		"datasource": "1",
		"addr":       addr,
		"datatype":   "string",
	}
	require.NoError(t, s.Ping(context.Background(), prop))
}
