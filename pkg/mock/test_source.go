// Copyright 2024 EMQ Technologies Co., Ltd.
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

package mock

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

var count atomic.Value

func TestSourceConnector(t *testing.T, r api.Source, props map[string]any, expected any, sender func()) {
	// init
	c := count.Load()
	if c == nil {
		count.Store(1)
		c = 0
	}
	count.Store(c.(int) + 1)
	// provision
	ctx, cancel := mockContext.NewMockContext(fmt.Sprintf("rule%d", c), "op1").WithCancel()
	err := r.Provision(ctx, props)
	assert.NoError(t, err)
	// connect, subscribe and read data
	err = r.Connect(ctx)
	assert.NoError(t, err)
	// Send data
	go func() {
		sender()
	}()
	time.Sleep(10 * time.Millisecond)
	// Send and receive data
	limit := 0
	switch et := expected.(type) {
	case []api.MessageTuple:
		limit = len(et)
	case []api.RawTuple:
		limit = len(et)
	default:
		t.Fatal("invalid expected type")
	}
	var (
		wg     sync.WaitGroup
		result []api.MessageTuple
	)
	wg.Add(1)
	go func() {
		switch ss := r.(type) {
		case api.BytesSource:
			err = ss.Subscribe(ctx, func(ctx api.StreamContext, payload []byte, meta map[string]any, ts time.Time) {
				result = append(result, model.NewDefaultRawTuple(payload, meta, ts))
				limit--
				if limit == 0 {
					wg.Done()
				}
			}, func(ctx api.StreamContext, err error) {
				panic(err)
			})
		case api.TupleSource:
			err = ss.Subscribe(ctx, func(ctx api.StreamContext, message any, meta map[string]any, ts time.Time) {
				switch mt := message.(type) {
				case []byte:
					result = append(result, model.NewDefaultRawTuple(mt, meta, ts))
				case map[string]any:
					result = append(result, model.NewDefaultSourceTuple(mt, meta, ts))
				default:
					panic("not supported yet")
				}
				limit--
				if limit == 0 {
					wg.Done()
				}
			}, func(ctx api.StreamContext, err error) {
				panic(err)
			})
		default:
			panic("wrong source type")
		}
	}()
	defer func() {
		err = r.Close(ctx)
		assert.NoError(t, err)
	}()

	ticker := time.After(60 * time.Second)
	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()
	select {
	case <-ctx.Done():
	case <-finished:
		cancel()
		assert.Equal(t, expected, result)
	case <-ticker:
		cancel()
		assert.Fail(t, "timeout")
		return
	}
	assert.Equal(t, expected, result)
}
