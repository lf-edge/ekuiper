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

package node

import (
	"context"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/binder/io"
	"github.com/lf-edge/ekuiper/internal/conf"
	kctx "github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/infra"
	"sync"
)

//// Package vars and funcs

var (
	pool = &sourcePool{
		registry: make(map[string]*sourceSingleton),
	}
)

// node is readonly
func getSourceInstance(node *SourceNode, index int) (*sourceInstance, error) {
	var si *sourceInstance
	if node.options.SHARED {
		rkey := fmt.Sprintf("%s.%s", node.sourceType, node.name)
		s, ok := pool.load(rkey)
		if !ok {
			ns, err := io.Source(node.sourceType)
			if ns != nil {
				s, err = pool.addInstance(rkey, node, ns, index)
				if err != nil {
					return nil, err
				}
			} else {
				if err != nil {
					return nil, err
				} else {
					return nil, fmt.Errorf("source %s not found", node.sourceType)
				}
			}
		}
		// attach
		instanceKey := fmt.Sprintf("%s.%s.%d", rkey, node.ctx.GetRuleId(), index)
		err := s.attach(instanceKey, node.bufferLength)
		if err != nil {
			return nil, err
		}
		si = &sourceInstance{
			source:                 s.source,
			ctx:                    s.ctx,
			sourceInstanceChannels: s.outputs[instanceKey],
		}
	} else {
		ns, err := io.Source(node.sourceType)
		if ns != nil {
			si, err = start(nil, node, ns)
			if err != nil {
				return nil, err
			}
			go func() {
				err := infra.SafeRun(func() error {
					nctx := node.ctx.WithInstance(index)
					defer si.source.Close(nctx)
					si.source.Open(nctx, si.dataCh.In, si.errorCh)
					return nil
				})
				if err != nil {
					infra.DrainError(node.ctx, err, si.errorCh)
				}
			}()
		} else {
			if err != nil {
				return nil, err
			} else {
				return nil, fmt.Errorf("source %s not found", node.sourceType)
			}
		}
	}
	return si, nil
}

// removeSourceInstance remove an attach from the sourceSingleton
// If all attaches are removed, close the sourceSingleton and remove it from the pool registry
// ONLY apply to shared instance
func removeSourceInstance(node *SourceNode) {
	for i := 0; i < node.concurrency; i++ {
		rkey := fmt.Sprintf("%s.%s", node.sourceType, node.name)
		pool.deleteInstance(rkey, node, i)
	}
}

//// data types

/*
 *	Pool for all keyed source instance.
 *  Create an instance, and start the source go routine when the keyed was hit the first time.
 *  For later hit, create the new set of channels and attach to the instance
 *  When hit a delete (when close a rule), remove the attached channels. If all channels removed, remove the instance from the pool
 *  For performance reason, the pool only holds the shared instance. Rule specific instance are holden by rule source node itself
 */
type sourcePool struct {
	registry map[string]*sourceSingleton
	sync.RWMutex
}

func (p *sourcePool) load(k string) (*sourceSingleton, bool) {
	p.RLock()
	defer p.RUnlock()
	s, ok := p.registry[k]
	return s, ok
}

func (p *sourcePool) addInstance(k string, node *SourceNode, source api.Source, index int) (*sourceSingleton, error) {
	p.Lock()
	defer p.Unlock()
	s, ok := p.registry[k]
	if !ok {
		contextLogger := conf.Log.WithField("source_pool", k)
		ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
		ruleId := "$$source_pool_" + k
		opId := "source_pool_" + k
		store, err := state.CreateStore("source_pool_"+k, 0)
		if err != nil {
			ctx.GetLogger().Errorf("source pool %s create store error %v", k, err)
			return nil, err
		}
		sctx, cancel := ctx.WithMeta(ruleId, opId, store).WithCancel()
		si, err := start(sctx, node, source)
		if err != nil {
			return nil, err
		}
		newS := &sourceSingleton{
			sourceInstance: si,
			outputs:        make(map[string]*sourceInstanceChannels),
			cancel:         cancel,
		}
		p.registry[k] = newS
		go func() {
			err := infra.SafeRun(func() error {
				nctx := node.ctx.WithInstance(index)
				defer si.source.Close(nctx)
				si.source.Open(nctx, si.dataCh.In, si.errorCh)
				return nil
			})
			if err != nil {
				newS.broadcastError(err)
			}
		}()
		go func() {
			err := infra.SafeRun(func() error {
				newS.run(node.sourceType, node.name)
				return nil
			})
			if err != nil {
				newS.broadcastError(err)
			}
		}()
		s = newS
	}
	return s, nil
}

func (p *sourcePool) deleteInstance(k string, node *SourceNode, index int) {
	p.Lock()
	defer p.Unlock()
	s, ok := p.registry[k]
	if ok {
		instanceKey := fmt.Sprintf("%s.%s.%d", k, node.ctx.GetRuleId(), index)
		end := s.detach(instanceKey)
		if end {
			s.cancel()

			s.dataCh.Close()
			delete(p.registry, k)
		}
	}
}

type sourceInstance struct {
	source api.Source
	ctx    api.StreamContext
	*sourceInstanceChannels
}

// Hold the only instance for all shared source
// And hold the reference to all shared source input channels. Must be sync when dealing with outputs
type sourceSingleton struct {
	*sourceInstance                    // immutable
	cancel          context.CancelFunc // immutable

	outputs map[string]*sourceInstanceChannels // read-write lock
	sync.RWMutex
}

type sourceInstanceChannels struct {
	dataCh  *DynamicChannelBuffer
	errorCh chan error
}

func newSourceInstanceChannels(bl int) *sourceInstanceChannels {
	buffer := NewDynamicChannelBuffer()
	buffer.SetLimit(bl)
	errorOutput := make(chan error)
	return &sourceInstanceChannels{
		dataCh:  buffer,
		errorCh: errorOutput,
	}
}

func (ss *sourceSingleton) run(name, key string) {
	logger := ss.ctx.GetLogger()
	logger.Infof("Start source %s shared instance %s successfully", name, key)
	for {
		select {
		case <-ss.ctx.Done():
			logger.Infof("source %s shared instance %s done", name, key)
			return
		case err := <-ss.errorCh:
			ss.broadcastError(err)
			return
		case data := <-ss.dataCh.Out:
			logger.Debugf("broadcast data %v from source pool %s:%s", data, name, key)
			ss.broadcast(data)
		}
	}
}

func (ss *sourceSingleton) broadcast(val api.SourceTuple) {
	ss.RLock()
	for n, out := range ss.outputs {
		go func(name string, dataCh *DynamicChannelBuffer) {
			select {
			case dataCh.In <- val:
			case <-ss.ctx.Done():
			case <-dataCh.done:
				// detached
			}
		}(n, out.dataCh)
	}
	ss.RUnlock()
}

func (ss *sourceSingleton) broadcastError(err error) {
	logger := ss.ctx.GetLogger()
	var wg sync.WaitGroup
	ss.RLock()
	wg.Add(len(ss.outputs))
	for n, out := range ss.outputs {
		go func(name string, output chan<- error) {
			infra.DrainError(ss.ctx, err, output)
			wg.Done()
		}(n, out.errorCh)
	}
	ss.RUnlock()
	logger.Debugf("broadcasting from source pool")
	wg.Wait()
}

func (ss *sourceSingleton) attach(instanceKey string, bl int) error {
	ss.Lock()
	defer ss.Unlock()
	if _, ok := ss.outputs[instanceKey]; !ok {
		ss.outputs[instanceKey] = newSourceInstanceChannels(bl)
	} else {
		// should not happen
		return fmt.Errorf("fail to attach source instance, already has an output of the same key %s", instanceKey)
	}
	return nil
}

// detach Detach an instance and return if the singleton is ended
func (ss *sourceSingleton) detach(instanceKey string) bool {
	ss.Lock()
	defer ss.Unlock()
	if chs, ok := ss.outputs[instanceKey]; ok {
		chs.dataCh.Close()
	} else {
		// should not happen
		ss.ctx.GetLogger().Warnf("detach source instance %s, not found", instanceKey)
		return false
	}
	delete(ss.outputs, instanceKey)
	if len(ss.outputs) == 0 {
		ss.cancel()
		return true
	}
	return false
}

func start(poolCtx api.StreamContext, node *SourceNode, s api.Source) (*sourceInstance, error) {
	err := s.Configure(node.options.DATASOURCE, node.props)
	if err != nil {
		return nil, err
	}

	ctx := poolCtx
	if poolCtx == nil {
		ctx = node.ctx
		if rw, ok := s.(api.Rewindable); ok {
			if offset, err := ctx.GetState(OffsetKey); err != nil {
				return nil, err
			} else if offset != nil {
				ctx.GetLogger().Infof("Source rewind from %v", offset)
				err = rw.Rewind(offset)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	chs := newSourceInstanceChannels(node.bufferLength)
	return &sourceInstance{
		source:                 s,
		sourceInstanceChannels: chs,
		ctx:                    ctx,
	}, nil
}
