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

	"github.com/benbjohnson/clock"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node/cache"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

// CacheOp receives tuples and decide to send through or save to disk. Run right before sink
// Immutable: true
// Input: any (mostly MessageTuple/MessageTupleList, may receive RawTuple after transformOp)
// Special validation: one output only
type CacheOp struct {
	*defaultSinkNode
	// configs
	cacheConf *conf.SinkConf
	// state
	cache    *cache.SyncCache
	currItem any
	hasCache bool
	// send timer, only enabled when there is cache. disable when all cache are sent
	resendTicker  *clock.Ticker
	resendTimerCh <-chan time.Time
}

func NewCacheOp(ctx api.StreamContext, name string, rOpt *def.RuleOption, sc *conf.SinkConf) (*CacheOp, error) {
	// use channel buffer as memory cache
	c, err := cache.NewSyncCache(ctx, sc)
	if err != nil {
		return nil, err
	}
	return &CacheOp{
		defaultSinkNode: newDefaultSinkNode(name, rOpt),
		cache:           c,
		cacheConf:       sc,
	}, nil
}

// Exec ingest data and send through.
// If channel full, save data to disk cache and start send timer
// Once all cache sent, stop send timer
func (s *CacheOp) Exec(ctx api.StreamContext, errCh chan<- error) {
	if len(s.outputs) > 1 {
		infra.DrainError(ctx, fmt.Errorf("cache op should have only 1 output but got %+v", s.outputs), errCh)
	}
	s.prepareExec(ctx, errCh, "op")
	go func() {
		for {
			select {
			case <-ctx.Done():
				s.cache.Flush(ctx)
				return
			case d := <-s.input:
				data, processed := s.commonIngest(ctx, d)
				if processed {
					break
				}
				// If already have the cache, append this to cache and send the currItem
				// Otherwise, send out the new data. If blocked, make it currItem
				s.statManager.IncTotalRecordsIn()
				s.statManager.ProcessTimeStart()

				if s.hasCache { // already have cache, add current data to cache and send out the cache
					err := s.cache.AddCache(ctx, data)
					ctx.GetLogger().Debugf("add data %v to cache", data)
					if err != nil {
						s.statManager.IncTotalExceptions(err.Error())
						s.Broadcast(err)
						s.statManager.ProcessTimeEnd()
						s.statManager.IncTotalMessagesProcessed(1)
						break
					}
				} else {
					s.currItem = data
				}
				s.send()

				s.statManager.ProcessTimeEnd()
				s.statManager.IncTotalMessagesProcessed(1)
				l := int64(len(s.input) + s.cache.CacheLength)
				if s.currItem != nil {
					l += 1
				}
				s.statManager.SetBufferLength(l)
			case <-s.resendTimerCh:
				ctx.GetLogger().Debugf("ticker is triggered")
				s.statManager.ProcessTimeStart()
				s.send()
				s.statManager.ProcessTimeEnd()
				l := int64(len(s.input) + s.cache.CacheLength)
				if s.currItem != nil {
					l += 1
				}
				s.statManager.SetBufferLength(l)
			}
		}
	}()
}

func (s *CacheOp) send() {
	if s.currItem == nil { // current item sent out finally
		if s.cache.CacheLength > 0 {
			// read
			var readOk bool
			s.currItem, readOk = s.cache.PopCache(s.ctx)
			if !readOk { // should never happen
				s.ctx.GetLogger().Errorf("fail to read from cache")
			} else {
				s.ctx.GetLogger().Debugf("read from cache %v", s.currItem)
			}
		} else {
			// cancel the timer since all cache are sent
			s.resendTicker.Stop()
			s.hasCache = false
			s.ctx.GetLogger().Debugf("cache all sent, stop ticker")
			return
		}
	}
	// Send by custom broadcast, if successful, reset currItem to nil
	s.BroadcastCustomized(s.currItem, s.doBroadcast)
}

func (s *CacheOp) doBroadcast(val interface{}) {
	var out chan<- any
	for _, output := range s.outputs {
		out = output
	}
	select {
	case out <- val:
		s.ctx.GetLogger().Debugf("send out data %v", val)
		// send through. The sink must retry until successful
		s.currItem = nil
		s.statManager.IncTotalRecordsOut()
	case <-s.ctx.Done():
		// rule stop so stop waiting
	default:
		if !s.hasCache {
			s.ctx.GetLogger().Debugf("memory buffer full, start to save cache")
			// Start the send interval
			d, _ := cast.ConvertDuration(s.cacheConf.ResendInterval)
			s.resendTicker = timex.Clock.Ticker(d)
			s.resendTimerCh = s.resendTicker.C
			s.hasCache = true
		}
	}
}
