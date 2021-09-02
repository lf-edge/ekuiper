// Copyright 2021 EMQ Technologies Co., Ltd.
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
	"encoding/gob"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/store"
	"github.com/lf-edge/ekuiper/internal/topo/checkpoint"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/kv"
	"path"
	"sort"
	"strconv"
)

type CacheTuple struct {
	index int
	data  interface{}
}

type LinkedQueue struct {
	Data map[int]interface{}
	Tail int
}

func (l *LinkedQueue) append(item interface{}) {
	l.Data[l.Tail] = item
	l.Tail++
}

func (l *LinkedQueue) delete(index int) {
	delete(l.Data, index)
}

func (l *LinkedQueue) reset() {
	l.Tail = 0
}

func (l *LinkedQueue) length() int {
	return len(l.Data)
}

func (l *LinkedQueue) clone() *LinkedQueue {
	result := &LinkedQueue{
		Data: make(map[int]interface{}),
		Tail: l.Tail,
	}
	for k, v := range l.Data {
		result.Data[k] = v
	}
	return result
}

func (l *LinkedQueue) String() string {
	return fmt.Sprintf("tail: %d, data: %v", l.Tail, l.Data)
}

type Cache struct {
	//Data and control channels
	in       <-chan interface{}
	Out      chan *CacheTuple
	Complete chan int
	errorCh  chan<- error
	//states
	pending *LinkedQueue
	changed bool
	//serialize
	key   string //the key for current cache
	store kv.KeyValue
}

func NewTimebasedCache(in <-chan interface{}, limit int, saveInterval int, errCh chan<- error, ctx api.StreamContext) *Cache {
	c := &Cache{
		in:       in,
		Out:      make(chan *CacheTuple, limit),
		Complete: make(chan int),
		errorCh:  errCh,
	}
	go c.timebasedRun(ctx, saveInterval)
	return c
}

func (c *Cache) initStore(ctx api.StreamContext) {
	logger := ctx.GetLogger()
	c.pending = &LinkedQueue{
		Data: make(map[int]interface{}),
		Tail: 0,
	}
	var err error
	err, c.store = store.GetKV(path.Join("sink", ctx.GetRuleId()))
	if err != nil {
		c.drainError(err)
	}
	c.key = ctx.GetOpId() + strconv.Itoa(ctx.GetInstanceId())
	logger.Debugf("cache saved to key %s", c.key)
	//load cache
	if err := c.loadCache(); err != nil {
		go c.drainError(err)
		return
	}
}

func (c *Cache) timebasedRun(ctx api.StreamContext, saveInterval int) {
	logger := ctx.GetLogger()
	c.initStore(ctx)
	ticker := conf.GetTicker(saveInterval)
	defer ticker.Stop()
	var tcount = 0
	for {
		select {
		case item := <-c.in:
			index := c.pending.Tail
			c.pending.append(item)
			//non blocking until limit exceeded
			c.Out <- &CacheTuple{
				index: index,
				data:  item,
			}
			c.changed = true
		case index := <-c.Complete:
			c.pending.delete(index)
			c.changed = true
		case <-ticker.C:
			tcount++
			l := c.pending.length()
			if l == 0 {
				c.pending.reset()
			}
			//If the data is still changing, only do a save when the cache has more than threshold to prevent too much file IO
			//If the data is not changing in the time slot and have not saved before, save it. This is to prevent the
			//data won't be saved as the cache never pass the threshold
			//logger.Infof("ticker %t, l=%d\n", c.changed, l)
			if (c.changed && l > conf.Config.Sink.CacheThreshold) || (tcount == conf.Config.Sink.CacheTriggerCount && c.changed) {
				logger.Infof("save cache for rule %s, %s", ctx.GetRuleId(), c.pending.String())
				clone := c.pending.clone()
				c.changed = false
				go func() {
					if err := c.saveCache(logger, clone); err != nil {
						logger.Debugf("%v", err)
						c.drainError(err)
					}
				}()
			}
			if tcount >= conf.Config.Sink.CacheThreshold {
				tcount = 0
			}
		case <-ctx.Done():
			err := c.saveCache(logger, c.pending)
			if err != nil {
				logger.Warnf("Error found during saving cache: %s \n ", err)
			}
			logger.Infof("sink node %s instance cache %d done", ctx.GetOpId(), ctx.GetInstanceId())
			return
		}
	}
}

func (c *Cache) loadCache() error {
	gob.Register(c.pending)
	mt := new(LinkedQueue)
	if f, err := c.store.Get(c.key, &mt); f {
		if nil != err {
			return fmt.Errorf("load malform cache, found %v(%v)", c.key, mt)
		}
		c.pending = mt
		c.changed = true
		// To store the keys in slice in sorted order
		var keys []int
		for k := range mt.Data {
			keys = append(keys, k)
		}
		sort.Ints(keys)
		for _, k := range keys {
			t := &CacheTuple{
				index: k,
				data:  mt.Data[k],
			}
			c.Out <- t
		}
		return nil
	}
	return nil
}

func (c *Cache) saveCache(logger api.Logger, p *LinkedQueue) error {
	logger.Infof("clean the cache and reopen")
	c.store.Clean()

	return c.store.Set(c.key, p)
}

func (c *Cache) drainError(err error) {
	c.errorCh <- err
}

func (c *Cache) Length() int {
	return c.pending.length()
}

func NewCheckpointbasedCache(in <-chan interface{}, limit int, tch <-chan struct{}, errCh chan<- error, ctx api.StreamContext) *Cache {
	c := &Cache{
		in:       in,
		Out:      make(chan *CacheTuple, limit),
		Complete: make(chan int),
		errorCh:  errCh,
	}
	go c.checkpointbasedRun(ctx, tch)
	return c
}

func (c *Cache) checkpointbasedRun(ctx api.StreamContext, tch <-chan struct{}) {
	logger := ctx.GetLogger()
	c.initStore(ctx)

	for {
		select {
		case item := <-c.in:
			// possibility of barrier, ignore if found
			if boe, ok := item.(*checkpoint.BufferOrEvent); ok {
				if _, ok := boe.Data.(*checkpoint.Barrier); ok {
					c.Out <- &CacheTuple{
						data: item,
					}
					logger.Debugf("sink cache send out barrier %v", boe.Data)
					break
				}
			}
			index := c.pending.Tail
			c.pending.append(item)
			//non blocking until limit exceeded
			c.Out <- &CacheTuple{
				index: index,
				data:  item,
			}
			logger.Debugf("sink cache send out tuple %v", item)
			c.changed = true
		case index := <-c.Complete:
			c.pending.delete(index)
			c.changed = true
		case <-tch:
			logger.Infof("save cache for rule %s, %s", ctx.GetRuleId(), c.pending.String())
			clone := c.pending.clone()
			if c.changed {
				go func() {
					if err := c.saveCache(logger, clone); err != nil {
						logger.Debugf("%v", err)
						c.drainError(err)
					}
				}()
			}
			c.changed = false
		case <-ctx.Done():
			logger.Infof("sink node %s instance cache %d done", ctx.GetOpId(), ctx.GetInstanceId())
			return
		}
	}
}
