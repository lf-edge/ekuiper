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

package node

import (
	"fmt"
	"testing"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
)

func TestBuffer(t *testing.T) {
	b := NewDynamicChannelBuffer()
	b.SetLimit(100)
	stopSign := make(chan struct{})
	mc := conf.Clock.(*clock.Mock)
	go func(done chan struct{}) {
		for i := 0; i < 100; i++ {
			select {
			case b.In <- api.NewDefaultSourceTupleWithTime(map[string]interface{}{"a": 5}, nil, mc.Now()):
				fmt.Printf("feed in %d\n", i)
			default:
				t.Errorf("message %d dropped, should not drop message", i)
			}
		}
		close(done)
	}(stopSign)
	for i := 0; i < 50; i++ {
		_ = <-b.Out
		fmt.Printf("eaten %d\n", i)
		time.Sleep(10 * time.Millisecond)
	}
	<-stopSign
	l := b.GetLength()
	if l != 50 {
		t.Errorf("Expect buffer length 50, but got %d", l)
	}
}
