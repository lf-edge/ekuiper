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
	"time"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/testx"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/node/metric"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/pkg/api"
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

// TestRun test for
// 1. cache in memory only
// 2. cache in memory and disk buffer only
// 3. cache in memory and disk
// 4. cache in memory and disk buffer and overflow
// Each flow test rule restart
// Each flow use slightly different config like bufferPageSize
func TestRun(t *testing.T) {
	tests := []struct {
		sconf   *conf.SinkConf
		dataIn  [][]map[string]interface{}
		dataOut [][]map[string]interface{}
		stopPt  int // restart the rule in this point
	}{
		{ // 0
			sconf: &conf.SinkConf{
				MemoryCacheThreshold: 4,
				MaxDiskCache:         12,
				BufferPageSize:       2,
				EnableCache:          true,
				ResendInterval:       0,
				CleanCacheAtStop:     false,
			},
			dataIn: [][]map[string]interface{}{
				{{"a": 1}}, {{"a": 2}}, {{"a": 3}}, {{"a": 4}}, {{"a": 5}},
			},
			stopPt: 4,
		},
		{ // 1
			sconf: &conf.SinkConf{
				MemoryCacheThreshold: 4,
				MaxDiskCache:         8,
				BufferPageSize:       2,
				EnableCache:          true,
				ResendInterval:       0,
				CleanCacheAtStop:     false,
			},
			dataIn: [][]map[string]interface{}{
				{{"a": 1}}, {{"a": 2}}, {{"a": 3}}, {{"a": 4}}, {{"a": 5}}, {{"a": 6}},
			},
			stopPt: 5,
		},
		{ // 2
			sconf: &conf.SinkConf{
				MemoryCacheThreshold: 1,
				MaxDiskCache:         8,
				BufferPageSize:       1,
				EnableCache:          true,
				ResendInterval:       0,
				CleanCacheAtStop:     false,
			},
			dataIn: [][]map[string]interface{}{
				{{"a": 1}}, {{"a": 2}}, {{"a": 3}}, {{"a": 4}}, {{"a": 5}}, {{"a": 6}},
			},
			stopPt: 4,
		},
		{ // 3
			sconf: &conf.SinkConf{
				MemoryCacheThreshold: 2,
				MaxDiskCache:         4,
				BufferPageSize:       2,
				EnableCache:          true,
				ResendInterval:       0,
				CleanCacheAtStop:     false,
			},
			dataIn: [][]map[string]interface{}{
				{{"a": 1}}, {{"a": 2}}, {{"a": 3}}, {{"a": 4}}, {{"a": 5}}, {{"a": 6}}, {{"a": 7}}, {{"a": 8}}, {{"a": 9}}, {{"a": 10}}, {{"a": 11}}, {{"a": 12}}, {{"a": 13}},
			},
			dataOut: [][]map[string]interface{}{
				{{"a": 1}}, {{"a": 6}}, {{"a": 7}}, {{"a": 8}}, {{"a": 9}}, {{"a": 10}}, {{"a": 11}}, {{"a": 12}}, {{"a": 13}},
			},
			stopPt: 4,
		},
	}
	testx.InitEnv()
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	tempStore, _ := state.CreateStore("mock", api.AtMostOnce)
	deleteCachedb()
	for i, tt := range tests {
		contextLogger := conf.Log.WithField("rule", fmt.Sprintf("TestRun-%d", i))
		ctx, cancel := context.WithValue(context.Background(), context.LoggerKey, contextLogger).WithMeta(fmt.Sprintf("rule%d", i), fmt.Sprintf("op%d", i), tempStore).WithCancel()
		stats, err := metric.NewStatManager(ctx, "sink")
		if err != nil {
			t.Fatal(err)
			return
		}
		in := make(chan []map[string]interface{})
		errCh := make(chan error)
		var result []interface{}
		go func() {
			err := <-errCh
			t.Log(err)
			return
		}()
		exitCh := make(chan struct{})
		// send data
		_ = NewSyncCacheWithExitChanel(ctx, in, errCh, stats, tt.sconf, 100, exitCh)
		for i := 0; i < tt.stopPt; i++ {
			in <- tt.dataIn[i]
			time.Sleep(1 * time.Millisecond)
		}
		cancel()
		// wait cleanup job done
		<-exitCh

		// send the second half data
		ctx, cancel = context.WithValue(context.Background(), context.LoggerKey, contextLogger).WithMeta(fmt.Sprintf("rule%d", i), fmt.Sprintf("op%d", i), tempStore).WithCancel()
		sc := NewSyncCache(ctx, in, errCh, stats, tt.sconf, 100)
		for i := tt.stopPt; i < len(tt.dataIn); i++ {
			in <- tt.dataIn[i]
			time.Sleep(1 * time.Millisecond)
		}
	loop:
		for range tt.dataIn {
			sc.Ack <- true
			select {
			case r := <-sc.Out:
				result = append(result, r)
			case <-time.After(1 * time.Second):
				t.Log(fmt.Sprintf("test %d no data", i))
				break loop
			}
		}

		cancel()
		if tt.dataOut == nil {
			tt.dataOut = tt.dataIn
		}
		if len(tt.dataOut) != len(result) {
			t.Errorf("test %d data mismatch\nexpect\t%v\nbut got\t%v", i, tt.dataOut, result)
			continue
		}
		for i, v := range result {
			if !reflect.DeepEqual(tt.dataOut[i], v) {
				t.Errorf("test %d data mismatch\nexpect\t%v\nbut got\t%v", i, tt.dataOut, result)
				break
			}
		}
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
