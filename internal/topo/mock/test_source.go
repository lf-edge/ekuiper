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

package mock

import (
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/api"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

var count atomic.Value

func TestSourceOpen(r api.Source, exp []api.SourceTuple, t *testing.T) {
	c := count.Load()
	if c == nil {
		count.Store(1)
		c = 0
	}
	ctx, cancel := NewMockContext(fmt.Sprintf("rule%d", c), "op1").WithCancel()
	count.Store(c.(int) + 1)
	consumer := make(chan api.SourceTuple)
	errCh := make(chan error)
	go r.Open(ctx, consumer, errCh)
	ticker := time.After(10 * time.Second)
	limit := len(exp)
	var result []api.SourceTuple
outerloop:
	for {
		select {
		case err := <-errCh:
			t.Errorf("received error: %v", err)
			cancel()
			return
		case tuple := <-consumer:
			result = append(result, tuple)
			limit--
			if limit <= 0 {
				break outerloop
			}
		case <-ticker:
			t.Errorf("stop after timeout")
			t.Errorf("expect %v, but got %v", exp, result)
			cancel()
			return
		}
	}
	err := r.Close(ctx)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	if !reflect.DeepEqual(exp, result) {
		t.Errorf("result mismatch:\n  exp=%s\n  got=%s\n\n", exp, result)
	}
}
