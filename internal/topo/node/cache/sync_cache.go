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
	"path"
	"strconv"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/pkg/kv"
)

// SyncCache is the struct to handle cache saving and read
// The data are sink tuples: MessageTuple, MessageTupleList or RawTuple

// page Rotates storage for in memory cache
// Not thread safe!
type page struct {
	Data []any
	H    int
	T    int
	L    int
	Size int
}

// newPage create a new cache page
func newPage(size int) *page {
	return &page{
		Data: make([]any, size),
		H:    0, // When deleting, head++, if tail == head, it is empty
		T:    0, // When append, tail++, if tail== head, it is full
		Size: size,
	}
}

// append item if list is not full and return true; otherwise return false
func (p *page) append(item any) bool {
	if p.L == p.Size { // full
		return false
	}
	p.Data[p.T] = item
	p.T++
	if p.T == p.Size {
		p.T = 0
	}
	p.L++
	return true
}

// peak get the first item in the cache
func (p *page) peak() (any, bool) {
	if p.L == 0 {
		return nil, false
	}
	return p.Data[p.H], true
}

func (p *page) delete() bool {
	if p.L == 0 {
		return false
	}
	p.H++
	if p.H == p.Size {
		p.H = 0
	}
	p.L--
	return true
}

func (p *page) isEmpty() bool {
	return p.L == 0
}

func (p *page) reset() {
	p.H = 0
	p.T = 0
	p.L = 0
}

type SyncCache struct {
	// cache config
	cacheConf   *conf.SinkConf
	maxDiskPage int
	maxMemPage  int
	// cache storage
	memCache       []*page
	diskBufferPage *page
	// status
	diskSize     int // the count of pages has been saved
	CacheLength  int // readonly, for metrics only to save calculation
	diskPageTail int // init from the database
	diskPageHead int
	// serialize
	store kv.KeyValue
}

func NewSyncCache(ctx api.StreamContext, cacheConf *conf.SinkConf) (*SyncCache, error) {
	ctx.GetLogger().Infof("create sync cache with conf %+v", cacheConf)
	c := &SyncCache{
		cacheConf: cacheConf,
		// Do not export this
		maxMemPage: 1,
		memCache:   make([]*page, 0),
		// add one more slot so that there will be at least one slot between head and tail to find out the head/tail id
		maxDiskPage: (cacheConf.MaxDiskCache / cacheConf.BufferPageSize) + 1,
	}
	err := c.initStore(ctx)
	return c, err
}

// AddCache not thread safe!
func (c *SyncCache) AddCache(ctx api.StreamContext, item any) error {
	// If having disk cache, append to disk by append to disk buffer. Otherwise, append to mem cache
	if c.diskBufferPage != nil {
		err := c.appendToDisk(ctx, item)
		if err != nil {
			return err
		}
	} else {
		isNotFull := c.appendMemCache(item)
		if !isNotFull {
			if c.diskBufferPage == nil {
				c.diskBufferPage = newPage(c.cacheConf.BufferPageSize)
			}
			err := c.appendToDisk(ctx, item)
			if err != nil {
				return err
			}
		} else {
			ctx.GetLogger().Debugf("added cache to mem cache %v", item)
		}
	}
	c.CacheLength++
	ctx.GetLogger().Debugf("added cache %d", c.CacheLength)
	return nil
}

func (c *SyncCache) appendToDisk(ctx api.StreamContext, item any) error {
	isBufferNotFull := c.diskBufferPage.append(item)
	if !isBufferNotFull { // cool page full, save to disk
		if c.diskSize == c.maxDiskPage {
			// disk full, read the oldest page to the hot page
			err := c.loadFromDisk(ctx)
			if err != nil {
				return err
			}
			ctx.GetLogger().Debug("disk full, remove the last page")
		}
		err := c.store.Set(strconv.Itoa(c.diskPageTail), c.diskBufferPage)
		if err != nil {
			return fmt.Errorf("fail to store disk cache %v", err)
		} else {
			ctx.GetLogger().Debug("add cache to disk. the new disk buffer page is %v", c.diskBufferPage)
			c.diskPageTail++
			c.diskSize++
			err := c.store.Set("size", c.diskSize)
			if err != nil {
				ctx.GetLogger().Warnf("fail to store disk cache size %v", err)
			}
			// rotate
			if c.diskPageTail == c.maxDiskPage {
				c.diskPageTail = 0
			}
		}
		c.diskBufferPage.reset()
		c.diskBufferPage.append(item)
	} else {
		ctx.GetLogger().Debugf("added cache to disk buffer page %v", c.diskBufferPage)
	}
	return nil
}

// PopCache not thread safe!
func (c *SyncCache) PopCache(ctx api.StreamContext) (any, bool) {
	ctx.GetLogger().Debugf("deleting cache. CacheLength: %d, diskSize: %d", c.CacheLength, c.diskSize)
	if len(c.memCache) == 0 {
		ctx.GetLogger().Debug("mem cache is empty")
		return nil, false
	}
	result, _ := c.memCache[0].peak()
	isNotEmpty := c.memCache[0].delete()
	if isNotEmpty {
		c.CacheLength--
		ctx.GetLogger().Debugf("deleted cache: %d", c.CacheLength)
	}
	if c.memCache[0].isEmpty() { // read from disk or cool list
		c.memCache = c.memCache[1:]
		if c.diskSize > 0 {
			err := c.loadFromDisk(ctx)
			if err != nil {
				ctx.GetLogger().Error(err)
			}
		} else if c.diskBufferPage != nil { // use cool page as the new page
			ctx.GetLogger().Debugf("reading from diskBufferPage: %d", c.CacheLength)
			c.memCache = append(c.memCache, c.diskBufferPage)
			c.diskBufferPage = nil
		}
	}
	ctx.GetLogger().Debugf("deleted cache. CacheLength: %d, diskSize: %d, memCache: %v", c.CacheLength, c.diskSize, c.memCache)
	return result, true
}

func (c *SyncCache) loadFromDisk(ctx api.StreamContext) error {
	// load page from the disk
	ctx.GetLogger().Debugf("loading from disk %d. CacheLength: %d, diskSize: %d", c.diskPageTail, c.CacheLength, c.diskSize)
	hotPage := newPage(c.cacheConf.BufferPageSize)
	ok, err := c.store.Get(strconv.Itoa(c.diskPageHead), hotPage)
	if err != nil {
		return fmt.Errorf("fail to load disk cache %v", err)
	} else if !ok {
		return fmt.Errorf("nothing in the disk, should not happen")
	} else {
		_ = c.store.Delete(strconv.Itoa(c.diskPageHead))
		if len(c.memCache) >= c.maxMemPage {
			ctx.GetLogger().Warnf("drop a page of %d items in memory", c.memCache[0].L)
			c.CacheLength -= c.memCache[0].L
			c.memCache = c.memCache[1:]
		}
		c.memCache = append(c.memCache, hotPage)
		c.diskPageHead++
		c.diskSize--
		err := c.store.Set("size", c.diskSize)
		if err != nil {
			ctx.GetLogger().Warnf("fail to store disk cache size %v", err)
		}
		if c.diskPageHead == c.maxDiskPage {
			c.diskPageHead = 0
		}
		err = c.store.Set("head", c.diskPageHead)
		if err != nil {
			ctx.GetLogger().Warnf("fail to store disk cache size %v", err)
		}
	}
	ctx.GetLogger().Debugf("loaded from disk %d. CacheLength: %d, diskSize: %d", c.diskPageTail, c.CacheLength, c.diskSize)
	return nil
}

func (c *SyncCache) appendMemCache(item any) bool {
	if len(c.memCache) > c.maxMemPage {
		return false
	}
	if len(c.memCache) == 0 {
		c.memCache = append(c.memCache, newPage(c.cacheConf.BufferPageSize))
	}
	isNotFull := c.memCache[len(c.memCache)-1].append(item)
	if !isNotFull {
		if len(c.memCache) == c.maxMemPage {
			return false
		}
		c.memCache = append(c.memCache, newPage(c.cacheConf.BufferPageSize))
		return c.memCache[len(c.memCache)-1].append(item)
	}
	return true
}

func (c *SyncCache) peakMemCache(_ api.StreamContext) (any, bool) {
	if len(c.memCache) == 0 {
		return nil, false
	}
	return c.memCache[0].peak()
}

func (c *SyncCache) initStore(ctx api.StreamContext) error {
	kvTable := path.Join("sink", ctx.GetRuleId()+ctx.GetOpId()+strconv.Itoa(ctx.GetInstanceId()))
	if c.cacheConf.CleanCacheAtStop {
		ctx.GetLogger().Infof("creating cache store %s", kvTable)
		_ = store.DropCacheKV(kvTable)
	}
	var err error
	c.store, err = store.GetCacheKV(kvTable)
	if err != nil {
		return err
	}
	// restore the sink cache from disk
	if !c.cacheConf.CleanCacheAtStop {
		// Save 0 when init and save 1 when close. Wait for close for newly started sink node
		var set int
		ok, _ := c.store.Get("storeSig", &set)
		if ok && set == 0 { // may be saving
			i := 0
			for ; i < 100; i++ {
				time.Sleep(time.Millisecond * 10)
				_, err := c.store.Get("storeSig", &set)
				if err == nil && set == 1 {
					ctx.GetLogger().Infof("waiting for previous cache for %d times", i)
					break
				}
			}
			if i == 100 {
				ctx.GetLogger().Errorf("waiting for previous cache for %d times, exit and drop", i)
			}
		}
		_ = c.store.Set("storeSig", 0)
		ctx.GetLogger().Infof("start to restore cache from disk")
		// restore the memCache
		_, err = c.store.Get("memcache", &c.memCache)
		if err != nil {
			ctx.GetLogger().Errorf("fail to restore mem cache %v", err)
		}
		for _, p := range c.memCache {
			c.CacheLength += p.L
		}
		err = c.store.Delete("memcache")
		if err != nil {
			ctx.GetLogger().Errorf("fail to delete mem cache %v", err)
		}
		ctx.GetLogger().Infof("restored mem cache %d", c.CacheLength)
		// restore the disk cache
		var size int
		ok, _ = c.store.Get("size", &size)
		if !ok || size == 0 { // no disk cache
			return nil
		}
		c.diskSize = size
		var head int
		ok, _ = c.store.Get("head", &head)
		if ok {
			c.diskPageHead = head
		}
		c.CacheLength += (c.diskSize - 1) * c.cacheConf.BufferPageSize
		c.diskPageTail = (c.diskPageHead + c.diskSize - 1) % c.maxDiskPage
		// load buffer page
		hotPage := newPage(c.cacheConf.BufferPageSize)
		ok, err = c.store.Get(strconv.Itoa(c.diskPageTail), hotPage)
		if err != nil {
			ctx.GetLogger().Errorf("fail to load disk cache to buffer %v", err)
		} else if !ok {
			ctx.GetLogger().Errorf("nothing in the disk, should not happen")
		} else {
			c.diskBufferPage = hotPage
			c.CacheLength += c.diskBufferPage.L
			c.diskSize--
		}
		ctx.GetLogger().Infof("restored all cache %d. diskSize %d", c.CacheLength, c.diskSize)
	}
	return nil
}

// Flush save memory states to disk.
func (c *SyncCache) Flush(ctx api.StreamContext) {
	ctx.GetLogger().Infof("sink node %s instance cache %d closing", ctx.GetOpId(), ctx.GetInstanceId())
	if c.cacheConf.CleanCacheAtStop {
		kvTable := path.Join("sink", ctx.GetRuleId()+ctx.GetOpId()+strconv.Itoa(ctx.GetInstanceId()))
		ctx.GetLogger().Infof("cleaning cache store %s", kvTable)
		_ = store.DropCacheKV(kvTable)
	} else {
		if c.diskBufferPage != nil {
			err := c.store.Set(strconv.Itoa(c.diskPageTail), c.diskBufferPage)
			if err != nil {
				ctx.GetLogger().Errorf("fail to store disk cache %v", err)
			}
			err = c.store.Set("size", c.diskSize+1)
			if err != nil {
				ctx.GetLogger().Errorf("fail to store disk size %v", err)
			}
			ctx.GetLogger().Debug("store disk cache")
		}
		// store the memory states
		if len(c.memCache) > 0 {
			err := c.store.Set("memcache", c.memCache)
			if err != nil {
				ctx.GetLogger().Errorf("fail to store memory cache to disk %v", err)
			}
			ctx.GetLogger().Debugf("store memory cache %d", len(c.memCache))
		}
		_ = c.store.Set("storeSig", 1)
	}
}
