// Copyright 2022-2025 EMQ Technologies Co., Ltd.
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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
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
	s, err := NewSyncCache(ctx, &model.SinkConf{
		MaxDiskCache:     6,
		BufferPageSize:   2,
		EnableCache:      true,
		ResendInterval:   0,
		CleanCacheAtStop: false,
	})
	assert.NoError(t, err)
	require.NoError(t, s.InitStore(ctx))
	// prepare data
	tuples := make([]any, 15)
	for i := 0; i < 15; i++ {
		tuples[i] = &xsql.RawTuple{
			Emitter:   "test",
			Timestamp: time.UnixMilli(int64(i)),
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
			name:   "read in write buffer",
			inputs: tuples[:2],
			output: &xsql.RawTuple{
				Emitter:   "test",
				Timestamp: time.UnixMilli(0),
				Rawdata:   []byte("hello"),
				Metadata:  map[string]any{"topic": "demo"},
			},
			length: 1,
		},
		{
			name:   "read in read and write buffer",
			inputs: tuples[2:4],
			output: &xsql.RawTuple{
				Emitter:   "test",
				Timestamp: time.UnixMilli(1),
				Rawdata:   []byte("hello"),
				Metadata:  map[string]any{"topic": "demo"},
			},
			length: 2,
		},
		{
			name:   "read in disk",
			inputs: tuples[4:7],
			output: &xsql.RawTuple{
				Emitter:   "test",
				Timestamp: time.UnixMilli(2),
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
				Timestamp: time.UnixMilli(8),
				Rawdata:   []byte("hello"),
				Metadata:  map[string]any{"topic": "demo"},
			},
			length: 6,
		},
		{
			name: "read in left read buffer",
			output: &xsql.RawTuple{
				Emitter:   "test",
				Timestamp: time.UnixMilli(9),
				Rawdata:   []byte("hello"),
				Metadata:  map[string]any{"topic": "demo"},
			},
			length: 5,
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

func TestCacheCase2(t *testing.T) {
	testx.InitEnv("cache2")
	tempStore, err := state.CreateStore("mock", def.AtMostOnce)
	assert.NoError(t, err)
	deleteCachedb()
	contextLogger := conf.Log.WithField("rule", "TestCache")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger).WithMeta("TestCache", "op1", tempStore)
	s, err := NewSyncCache(ctx, &model.SinkConf{
		MemoryCacheThreshold: 2,
		MaxDiskCache:         4,
		BufferPageSize:       2,
		EnableCache:          true,
		ResendInterval:       cast.DurationConf(10 * time.Millisecond),
	})
	assert.NoError(t, err)
	require.NoError(t, s.InitStore(ctx))
	// prepare data
	tuples := make([]any, 15)
	for i := 0; i < 15; i++ {
		tuples[i] = &xsql.RawTuple{
			Emitter:   "test",
			Timestamp: time.UnixMilli(int64(i)),
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
			name:   "read in write buffer",
			inputs: tuples[4:5],
			output: &xsql.RawTuple{
				Emitter:   "test",
				Timestamp: time.UnixMilli(4),
				Rawdata:   []byte("hello"),
				Metadata:  map[string]any{"topic": "demo"},
			},
			length: 0,
		},
		{
			name:   "read in write buffer cont",
			inputs: tuples[5:6],
			output: &xsql.RawTuple{
				Emitter:   "test",
				Timestamp: time.UnixMilli(5),
				Rawdata:   []byte("hello"),
				Metadata:  map[string]any{"topic": "demo"},
			},
			length: 0,
		},
		{
			name:   "read in write buffer cont 2",
			inputs: tuples[6:7],
			output: &xsql.RawTuple{
				Emitter:   "test",
				Timestamp: time.UnixMilli(6),
				Rawdata:   []byte("hello"),
				Metadata:  map[string]any{"topic": "demo"},
			},
			length: 0,
		},
		{
			name:   "write more",
			inputs: tuples[7:10],
			output: &xsql.RawTuple{
				Emitter:   "test",
				Timestamp: time.UnixMilli(7),
				Rawdata:   []byte("hello"),
				Metadata:  map[string]any{"topic": "demo"},
			},
			length: 2,
		},
		{
			name:   "write over",
			inputs: tuples[10:11],
			output: &xsql.RawTuple{
				Emitter:   "test",
				Timestamp: time.UnixMilli(8),
				Rawdata:   []byte("hello"),
				Metadata:  map[string]any{"topic": "demo"},
			},
			length: 2,
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

func TestCacheInit(t *testing.T) {
	// Test flush and reload
	testx.InitEnv("cache3")
	tempStore, err := state.CreateStore("mock", def.AtMostOnce)
	assert.NoError(t, err)
	deleteCachedb()
	contextLogger := conf.Log.WithField("rule", "TestCache")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger).WithMeta("TestCache", "op1", tempStore)
	s, err := NewSyncCache(ctx, &model.SinkConf{
		MemoryCacheThreshold: 0,
		MaxDiskCache:         4,
		BufferPageSize:       2,
		EnableCache:          true,
		ResendInterval:       0,
		CleanCacheAtStop:     false,
	})
	assert.NoError(t, err)
	require.NoError(t, s.InitStore(ctx))
	// prepare data
	tuples := make([]any, 10)
	for i := 0; i < 10; i++ {
		tuples[i] = &xsql.RawTuple{
			Emitter:   "test",
			Timestamp: time.UnixMilli(int64(i)),
			Rawdata:   []byte("hello"),
			Metadata:  map[string]any{"topic": "demo"},
		}
		err = s.AddCache(ctx, tuples[i])
		assert.NoError(t, err)
	}
	assert.Equal(t, 6, s.CacheLength, "cache length before flush")
	s.Flush(ctx)
	s = nil
	s, err = NewSyncCache(ctx, &model.SinkConf{
		MemoryCacheThreshold: 0,
		MaxDiskCache:         4,
		BufferPageSize:       2,
		EnableCache:          true,
		ResendInterval:       0,
		CleanCacheAtStop:     false,
	})
	assert.NoError(t, err)
	require.NoError(t, s.InitStore(ctx))
	r, _ := s.PopCache(ctx)
	assert.Equal(t, 3, s.CacheLength, "cache length after pop")
	assert.Equal(t, &xsql.RawTuple{
		Emitter:   "test",
		Timestamp: time.UnixMilli(6),
		Rawdata:   []byte("hello"),
		Metadata:  map[string]any{"topic": "demo"},
	}, r)
	s.Flush(ctx)
	s = nil
	s, err = NewSyncCache(ctx, &model.SinkConf{
		MemoryCacheThreshold: 0,
		MaxDiskCache:         4,
		BufferPageSize:       2,
		EnableCache:          true,
		ResendInterval:       0,
		CleanCacheAtStop:     false,
	})
	assert.NoError(t, err)
	require.NoError(t, s.InitStore(ctx))
	r, _ = s.PopCache(ctx)
	assert.Equal(t, 2, s.CacheLength, "cache length after pop")
	assert.Equal(t, &xsql.RawTuple{
		Emitter:   "test",
		Timestamp: time.UnixMilli(7),
		Rawdata:   []byte("hello"),
		Metadata:  map[string]any{"topic": "demo"},
	}, r)
	s.cacheConf.CleanCacheAtStop = true
	s.Flush(ctx)
	s = nil
	s, err = NewSyncCache(ctx, &model.SinkConf{
		MemoryCacheThreshold: 0,
		MaxDiskCache:         2,
		BufferPageSize:       2,
		EnableCache:          true,
		ResendInterval:       0,
		CleanCacheAtStop:     true,
	})
	assert.NoError(t, err)
	assert.Equal(t, 0, s.CacheLength, "cache length after clean")
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
