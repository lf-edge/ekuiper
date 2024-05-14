// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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

package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
)

func TestPage(t *testing.T) {
	p := newPage(2)
	if !p.isEmpty() {
		t.Errorf("page is not empty")
	}
	if !p.append([]map[string]interface{}{
		{"a": 1},
	}) {
		t.Fatal("append failed")
	}
	if !p.append([]map[string]interface{}{
		{"a": 2},
	}) {
		t.Fatal("append failed")
	}
	if p.append([]map[string]interface{}{
		{"a": 3},
	}) {
		t.Fatal("should append fail")
	}
	v, ok := p.peak()
	if !ok {
		t.Fatal("peak failed")
	}
	if !reflect.DeepEqual(v, []map[string]interface{}{
		{"a": 1},
	}) {
		t.Fatalf("peak value mismatch, expect 1 but got %v", v)
	}
	if p.append([]map[string]interface{}{
		{"a": 4},
	}) {
		t.Fatal("should append failed")
	}
	if !p.delete() {
		t.Fatal("delete failed")
	}
	v, ok = p.peak()
	if !ok {
		t.Fatal("peak failed")
	}
	if !reflect.DeepEqual(v, []map[string]interface{}{
		{"a": 2},
	}) {
		t.Fatalf("peak value mismatch, expect 2 but got %v", v)
	}
	p.reset()
	if !p.append([]map[string]interface{}{
		{"a": 5},
	}) {
		t.Fatal("append failed")
	}
	if p.isEmpty() {
		t.Fatal("page should not empty")
	}
	if !p.delete() {
		t.Fatal("delete failed")
	}
	if !p.append([]map[string]interface{}{
		{"a": 5},
	}) {
		t.Fatal("append failed")
	}
	if !p.append([]map[string]interface{}{
		{"a": 6},
	}) {
		t.Fatal("append failed")
	}
	if !p.delete() {
		t.Fatal("delete failed")
	}
	if !p.delete() {
		t.Fatal("delete failed")
	}
	if p.delete() {
		t.Fatal("should delete failed")
	}
	if !p.isEmpty() {
		t.Fatal("page should be empty")
	}
}

func TestCache(t *testing.T) {
	testx.InitEnv("cache")
	tempStore, err := state.CreateStore("mock", def.AtMostOnce)
	assert.NoError(t, err)
	deleteCachedb()
	contextLogger := conf.Log.WithField("rule", "TestCache")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger).WithMeta("TestCache", "op1", tempStore)
	s, err := NewSyncCache(ctx, &conf.SinkConf{
		MemoryCacheThreshold: 2,
		MaxDiskCache:         4,
		BufferPageSize:       2,
		EnableCache:          true,
		ResendInterval:       0,
		CleanCacheAtStop:     false,
	})
	assert.NoError(t, err)
	// prepare data
	var tuples = make([]any, 10)
	for i := 0; i < 10; i++ {
		tuples[i] = &xsql.RawTuple{
			Emitter:   "test",
			Timestamp: int64(i),
			Rawdata:   []byte("hello"),
			Metadata:  map[string]any{"topic": "demo"},
		}
	}

	tests := []struct {
		name   string
		inputs []any
		output any
		length int
	}{
		{
			name:   "read empty",
			length: 0,
		},
		{
			name:   "read in mem",
			inputs: tuples[:2],
			output: &xsql.RawTuple{
				Emitter:   "test",
				Timestamp: 0,
				Rawdata:   []byte("hello"),
				Metadata:  map[string]any{"topic": "demo"},
			},
			length: 1,
		},
		{
			name:   "read in mem and disk buffer",
			inputs: tuples[2:4],
			output: &xsql.RawTuple{
				Emitter:   "test",
				Timestamp: 1,
				Rawdata:   []byte("hello"),
				Metadata:  map[string]any{"topic": "demo"},
			},
			length: 2,
		},
		{
			name:   "read in mem and disk",
			inputs: tuples[4:7],
			output: &xsql.RawTuple{
				Emitter:   "test",
				Timestamp: 2,
				Rawdata:   []byte("hello"),
				Metadata:  map[string]any{"topic": "demo"},
			},
			length: 4,
		},
		{
			name:   "read in mem and disk overflow",
			inputs: tuples[7:],
			output: &xsql.RawTuple{
				Emitter:   "test",
				Timestamp: 3,
				Rawdata:   []byte("hello"),
				Metadata:  map[string]any{"topic": "demo"},
			},
			length: 6,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, tuple := range tt.inputs {
				err = s.AddCache(ctx, tuple)
				assert.NoError(t, err)
			}
			r, _ := s.PopCache(ctx)
			assert.Equal(t, tt.output, r)
			assert.Equal(t, tt.length, s.CacheLength, "cache length")
		})
	}
}

func deleteCachedb() {
	loc, err := conf.GetDataLoc()
	if err != nil {
		fmt.Println(err)
	}
	err = os.RemoveAll(filepath.Join(loc, "cache.db"))
	if err != nil {
		fmt.Println(err)
	}
}
