// Copyright 2022 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/testx"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/node/metric"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/pkg/api"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestPage(t *testing.T) {
	p := newPage(2)
	if !p.isEmpty() {
		t.Errorf("page is not empty")
	}
	if !p.append(1) {
		t.Fatal("append failed")
	}
	if !p.append(2) {
		t.Fatal("append failed")
	}
	if p.append(3) {
		t.Fatal("should append fail")
	}
	v, ok := p.peak()
	if !ok {
		t.Fatal("peak failed")
	}
	if v != 1 {
		t.Fatalf("peak value mismatch, expect 3 but got %v", v)
	}
	if p.append(4) {
		t.Fatal("should append failed")
	}
	if !p.delete() {
		t.Fatal("delete failed")
	}
	v, ok = p.peak()
	if !ok {
		t.Fatal("peak failed")
	}
	if v != 2 {
		t.Fatalf("peak value mismatch, expect 2 but got %v", v)
	}
	p.reset()
	if !p.append(5) {
		t.Fatal("append failed")
	}
	if p.isEmpty() {
		t.Fatal("page should not empty")
	}
	if !p.delete() {
		t.Fatal("delete failed")
	}
	if !p.append(5) {
		t.Fatal("append failed")
	}
	if !p.append(6) {
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
	var tests = []struct {
		sconf   *conf.SinkConf
		dataIn  []interface{}
		dataOut []interface{}
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
			dataIn: []interface{}{
				1, 2, 3, 4, 5,
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
			dataIn: []interface{}{
				1, 2, 3, 4, 5, 6,
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
			dataIn: []interface{}{
				1, 2, 3, 4, 5, 6,
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
			dataIn: []interface{}{
				1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,
			},
			dataOut: []interface{}{
				1, 6, 7, 8, 9, 10, 11, 12, 13,
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
		in := make(chan interface{})
		errCh := make(chan error)
		var result []interface{}
		go func() {
			err := <-errCh
			t.Fatal(err)
			return
		}()
		// send data
		sc := NewSyncCache(ctx, in, errCh, stats, tt.sconf, 100)
		for i := 0; i < tt.stopPt; i++ {
			in <- tt.dataIn[i]
			time.Sleep(1 * time.Millisecond)
		}
		cancel()

		// send the second half data
		ctx, cancel = context.WithValue(context.Background(), context.LoggerKey, contextLogger).WithMeta(fmt.Sprintf("rule%d", i), fmt.Sprintf("op%d", i), tempStore).WithCancel()
		sc = NewSyncCache(ctx, in, errCh, stats, tt.sconf, 100)
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
		if !reflect.DeepEqual(tt.dataOut, result) {
			t.Errorf("test %d data mismatch\nexpect\t%v\nbut got\t%v", i, tt.dataOut, result)
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
