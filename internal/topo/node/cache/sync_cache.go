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
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/store"
	"github.com/lf-edge/ekuiper/internal/topo/node/metric"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/infra"
	"github.com/lf-edge/ekuiper/pkg/kv"
	"path"
	"strconv"
	"time"
)

type AckResult bool

// page Rotate storage for in memory cache
// Not thread safe!
type page struct {
	Data [][]map[string]interface{}
	H    int
	T    int
	L    int
	Size int
}

// newPage create a new cache page
// TODO the page is created even not used, need dynamic?
func newPage(size int) *page {
	return &page{
		Data: make([][]map[string]interface{}, size),
		H:    0, // When deleting, head++, if tail == head, it is empty
		T:    0, // When append, tail++, if tail== head, it is full
		Size: size,
	}
}

// append item if list is not full and return true; otherwise return false
func (p *page) append(item []map[string]interface{}) bool {
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
func (p *page) peak() ([]map[string]interface{}, bool) {
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
	// The input data to the cache
	in        <-chan []map[string]interface{}
	Out       chan []map[string]interface{}
	Ack       chan bool
	cacheCtrl chan interface{} // CacheCtrl is the only place to control the cache; sync in and ack result
	errorCh   chan<- error
	stats     metric.StatManager
	// cache config
	cacheConf   *conf.SinkConf
	maxDiskPage int
	maxMemPage  int
	// cache storage
	memCache       []*page
	diskBufferPage *page
	// status
	diskSize     int // how many pages has been saved
	cacheLength  int //readonly, for metrics only to save calculation
	diskPageTail int // init from the database
	diskPageHead int
	sendStatus   int // 0: idle, 1: sending and waiting for ack, 2: stopped for error
	//serialize
	store kv.KeyValue
}

func NewSyncCache(ctx api.StreamContext, in <-chan []map[string]interface{}, errCh chan<- error, stats metric.StatManager, cacheConf *conf.SinkConf, bufferLength int) *SyncCache {
	c := &SyncCache{
		cacheConf:  cacheConf,
		in:         in,
		Out:        make(chan []map[string]interface{}, bufferLength),
		Ack:        make(chan bool, 10),
		cacheCtrl:  make(chan interface{}, 10),
		errorCh:    errCh,
		maxMemPage: cacheConf.MemoryCacheThreshold / cacheConf.BufferPageSize,
		memCache:   make([]*page, 0),
		// add one more slot so that there will be at least one slot between head and tail to find out the head/tail id
		maxDiskPage: (cacheConf.MaxDiskCache / cacheConf.BufferPageSize) + 1,
		stats:       stats,
	}
	go func() {
		err := infra.SafeRun(func() error {
			c.run(ctx)
			return nil
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
	return c
}

func (c *SyncCache) run(ctx api.StreamContext) {
	c.initStore(ctx)
	defer c.onClose(ctx)
	if c.cacheLength > 0 { // start to send the cache
		c.send(ctx)
	}
	for {
		select {
		case item := <-c.in:
			ctx.GetLogger().Debugf("send to cache")
			c.cacheCtrl <- item
		case isSuccess := <-c.Ack:
			// only send the next sink after receiving an ack
			ctx.GetLogger().Debugf("cache ack")
			c.cacheCtrl <- AckResult(isSuccess)
		case data := <-c.cacheCtrl: // The only place to manipulate cache
			switch r := data.(type) {
			case AckResult:
				if r {
					ctx.GetLogger().Debugf("deleting cache")
					c.deleteCache(ctx)
					c.sendStatus = 0
					ctx.GetLogger().Debug("send status to 0 after true ack")
				} else {
					c.sendStatus = 2
					ctx.GetLogger().Debug("send status to 2 after false ack")
				}
			case []map[string]interface{}:
				ctx.GetLogger().Debugf("adding cache %v", data)
				c.addCache(ctx, r)
				if c.sendStatus == 2 {
					c.sendStatus = 0
					ctx.GetLogger().Debug("send status to 0 after adding cache in error state")
				}
			default:
				ctx.GetLogger().Errorf("unknown cache control command %v", data)
			}
			c.stats.SetBufferLength(int64(len(c.in) + c.cacheLength))
			if c.sendStatus == 0 {
				c.send(ctx)
			}
		case <-ctx.Done():
			ctx.GetLogger().Infof("sink node %s instance cache %d done", ctx.GetOpId(), ctx.GetInstanceId())
			return
		}
	}
}

func (c *SyncCache) send(ctx api.StreamContext) {
	if c.cacheLength > 1 && c.cacheConf.ResendInterval > 0 {
		time.Sleep(time.Duration(c.cacheConf.ResendInterval) * time.Millisecond)
	}
	d, ok := c.peakMemCache(ctx)
	if ok {
		ctx.GetLogger().Debugf("sending cache item %v", d)
		c.sendStatus = 1
		ctx.GetLogger().Debug("send status to 0 after sending tuple")
		select {
		case c.Out <- d:
			ctx.GetLogger().Debugf("sink cache send out %v", d)
		case <-ctx.Done():
			ctx.GetLogger().Debugf("stop sink cache send")
		}
	} else {
		ctx.GetLogger().Debug("no cache to send")
	}
}

// addCache not thread safe!
func (c *SyncCache) addCache(ctx api.StreamContext, item []map[string]interface{}) {
	isNotFull := c.appendMemCache(item)
	if !isNotFull {
		if c.diskBufferPage == nil {
			c.diskBufferPage = newPage(c.cacheConf.BufferPageSize)
		}
		isBufferNotFull := c.diskBufferPage.append(item)
		if !isBufferNotFull { // cool page full, save to disk
			if c.diskSize == c.maxDiskPage {
				// disk full, read the oldest page to the hot page
				c.loadFromDisk(ctx)
				ctx.GetLogger().Debug("disk full, remove the last page")
			}
			err := c.store.Set(strconv.Itoa(c.diskPageTail), c.diskBufferPage)
			if err != nil {
				ctx.GetLogger().Errorf("fail to store disk cache %v", err)
				return
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
	} else {
		ctx.GetLogger().Debugf("added cache to mem cache %v", c.memCache)
	}
	c.cacheLength++
	ctx.GetLogger().Debugf("added cache %d", c.cacheLength)
}

// deleteCache not thread safe!
func (c *SyncCache) deleteCache(ctx api.StreamContext) {
	ctx.GetLogger().Debugf("deleting cache. cacheLength: %d, diskSize: %d", c.cacheLength, c.diskSize)
	if len(c.memCache) == 0 {
		ctx.GetLogger().Debug("mem cache is empty")
		return
	}
	isNotEmpty := c.memCache[0].delete()
	if isNotEmpty {
		c.cacheLength--
		ctx.GetLogger().Debugf("deleted cache: %d", c.cacheLength)
	}
	if c.memCache[0].isEmpty() { // read from disk or cool list
		c.memCache = c.memCache[1:]
		if c.diskSize > 0 {
			c.loadFromDisk(ctx)
		} else if c.diskBufferPage != nil { // use cool page as the new page
			ctx.GetLogger().Debugf("reading from diskBufferPage: %d", c.cacheLength)
			c.memCache = append(c.memCache, c.diskBufferPage)
			c.diskBufferPage = nil
		}
	}
	ctx.GetLogger().Debugf("deleted cache. cacheLength: %d, diskSize: %d, memCache: %v", c.cacheLength, c.diskSize, c.memCache)
}

func (c *SyncCache) loadFromDisk(ctx api.StreamContext) {
	// load page from the disk
	ctx.GetLogger().Debugf("loading from disk %d. cacheLength: %d, diskSize: %d", c.diskPageTail, c.cacheLength, c.diskSize)
	hotPage := newPage(c.cacheConf.BufferPageSize)
	ok, err := c.store.Get(strconv.Itoa(c.diskPageHead), hotPage)
	if err != nil {
		ctx.GetLogger().Errorf("fail to load disk cache %v", err)
	} else if !ok {
		ctx.GetLogger().Errorf("nothing in the disk, should not happen")
	} else {
		if len(c.memCache) >= c.maxMemPage {
			ctx.GetLogger().Warnf("drop a page of %d items in memory", c.memCache[0].L)
			c.cacheLength -= c.memCache[0].L
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
	ctx.GetLogger().Debugf("loaded from disk %d. cacheLength: %d, diskSize: %d", c.diskPageTail, c.cacheLength, c.diskSize)
}

func (c *SyncCache) appendMemCache(item []map[string]interface{}) bool {
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

func (c *SyncCache) peakMemCache(_ api.StreamContext) ([]map[string]interface{}, bool) {
	if len(c.memCache) == 0 {
		return nil, false
	}
	return c.memCache[0].peak()
}

func (c *SyncCache) initStore(ctx api.StreamContext) {
	kvTable := path.Join("sink", ctx.GetRuleId()+ctx.GetOpId()+strconv.Itoa(ctx.GetInstanceId()))
	if c.cacheConf.CleanCacheAtStop {
		ctx.GetLogger().Infof("creating cache store %s", kvTable)
		store.DropCacheKV(kvTable)
	}
	var err error
	err, c.store = store.GetCacheKV(kvTable)
	if err != nil {
		infra.DrainError(ctx, err, c.errorCh)
	}
	// restore the sink cache from disk
	if !c.cacheConf.CleanCacheAtStop {
		// Save 0 when init and save 1 when close. Wait for close for newly started sink node
		var set int
		ok, err := c.store.Get("storeSig", &set)
		if ok && set == 0 { // may be saving
			var i = 0
			for ; i < 100; i++ {
				time.Sleep(time.Millisecond * 10)
				_, err = c.store.Get("storeSig", &set)
				if set == 1 {
					ctx.GetLogger().Infof("waiting for previous cache for %d times", i)
					break
				}
			}
			if i == 100 {
				ctx.GetLogger().Errorf("waiting for previous cache for %d times, exit and drop", i)
			}
		}
		c.store.Set("storeSig", 0)
		ctx.GetLogger().Infof("start to restore cache from disk")
		// restore the memCache
		_, err = c.store.Get("memcache", &c.memCache)
		if err != nil {
			ctx.GetLogger().Errorf("fail to restore mem cache %v", err)
		}
		for _, p := range c.memCache {
			c.cacheLength += p.L
		}
		err = c.store.Delete("memcache")
		if err != nil {
			ctx.GetLogger().Errorf("fail to delete mem cache %v", err)
		}
		ctx.GetLogger().Infof("restored mem cache %d", c.cacheLength)
		// restore the disk cache
		var size int
		ok, _ = c.store.Get("size", &size)
		if !ok || size == 0 { // no disk cache
			return
		}
		c.diskSize = size
		var head int
		ok, _ = c.store.Get("head", &head)
		if ok {
			c.diskPageHead = head
		}
		c.cacheLength += (c.diskSize - 1) * c.cacheConf.BufferPageSize
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
			c.cacheLength += c.diskBufferPage.L
			c.diskSize--
		}
		ctx.GetLogger().Infof("restored all cache %d. diskSize %d", c.cacheLength, c.diskSize)
	}
}

// save memory states to disk
func (c *SyncCache) onClose(ctx api.StreamContext) {
	ctx.GetLogger().Infof("sink node %s instance cache %d closing", ctx.GetOpId(), ctx.GetInstanceId())
	if c.cacheConf.CleanCacheAtStop {
		kvTable := path.Join("sink", ctx.GetRuleId()+ctx.GetOpId()+strconv.Itoa(ctx.GetInstanceId()))
		ctx.GetLogger().Infof("cleaning cache store %s", kvTable)
		store.DropCacheKV(kvTable)
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
		c.store.Set("storeSig", 1)
	}
}
