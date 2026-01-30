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
	"context"
	"time"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type item struct {
	data       []map[string]any
	expiration time.Time
}

type Cache struct {
	expireTime      time.Duration
	cacheMissingKey bool
	cancel          context.CancelFunc
	items           map[string]*item
	syncx.RWMutex
}

func NewCache(expireTime time.Duration, cacheMissingKey bool) *Cache {
	c := &Cache{
		expireTime:      expireTime,
		cacheMissingKey: cacheMissingKey,
		items:           make(map[string]*item),
	}
	if expireTime > 0 {
		ctx, cancel := context.WithCancel(context.Background())
		c.cancel = cancel
		go c.run(ctx)
	}
	return c
}

func (c *Cache) run(ctx context.Context) {
	ticker := timex.GetTicker(c.expireTime * 2)
	for {
		select {
		case <-ticker.C:
			c.deleteExpired()
		case <-ctx.Done():
			conf.Log.Infof("Lookup cache is stopped")
			ticker.Stop()
			return
		}
	}
}

func (c *Cache) deleteExpired() {
	now := timex.GetNow()
	c.Lock()
	for k, v := range c.items {
		if !v.expiration.IsZero() && now.After(v.expiration) {
			delete(c.items, k)
		}
	}
	c.Unlock()
}

func (c *Cache) Set(key string, value []map[string]any) {
	if len(value) == 0 && !c.cacheMissingKey {
		return
	}
	c.Lock()
	defer c.Unlock()
	if c.expireTime > 0 {
		c.items[key] = &item{data: value, expiration: timex.GetNow().Add(c.expireTime)}
	} else {
		c.items[key] = &item{data: value}
	}
}

func (c *Cache) Get(key string) ([]map[string]any, bool) {
	c.RLock()
	defer c.RUnlock()
	if v, ok := c.items[key]; ok {
		if !v.expiration.IsZero() && timex.GetNow().After(v.expiration) {
			return nil, false
		}
		return v.data, true
	}
	return nil, false
}

func (c *Cache) Close() {
	if c.cancel != nil {
		c.cancel()
	}
	c.items = nil
}
