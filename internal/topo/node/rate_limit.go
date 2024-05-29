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

package node

import (
	"fmt"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

// RateLimitOp handle messages at a regular rate, ignoring messages that arrive too quickly, only keep the most recent message. (default strategy)
// If strategy is set, send through all messages as well as trigger signal and let strategy node handle the merge.
// Otherwise, send the most recent message at trigger time
// Input: Raw
// Output: Raw as it is
// Concurrency: false
type RateLimitOp struct {
	*defaultSinkNode
	// configs
	interval time.Duration
	// state
	latest any
}

func NewRateLimitOp(name string, rOpt *def.RuleOption, interval time.Duration) (*RateLimitOp, error) {
	if interval < 1*time.Millisecond {
		return nil, fmt.Errorf("interval should be larger than 1ms")
	}
	o := &RateLimitOp{
		defaultSinkNode: newDefaultSinkNode(name, rOpt),
		interval:        interval,
	}
	return o, nil
}

func (o *RateLimitOp) Exec(ctx api.StreamContext, errCh chan<- error) {
	o.prepareExec(ctx, errCh, "op")
	ticker := timex.GetTicker(o.interval)
	go func() {
		defer func() {
			ticker.Stop()
			o.Close()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case d := <-o.input:
				dd, processed := o.commonIngest(ctx, d)
				if processed {
					continue
				}
				o.statManager.IncTotalRecordsIn()
				o.statManager.ProcessTimeStart()
				o.latest = dd
				o.statManager.ProcessTimeEnd()
				o.statManager.IncTotalMessagesProcessed(1)
				o.statManager.SetBufferLength(int64(len(o.input)))
			case t := <-ticker.C:
				if o.latest != nil {
					o.Broadcast(o.latest)
					o.latest = nil
					o.statManager.IncTotalRecordsOut()
				} else {
					ctx.GetLogger().Debugf("ratelimit had nothing to sent at %d", t.UnixMilli())
				}
			}
		}
	}()
}
