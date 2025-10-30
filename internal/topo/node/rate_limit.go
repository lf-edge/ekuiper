// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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
	"sort"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"golang.org/x/exp/maps"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/converter"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type limitConf struct {
	Interval        cast.DurationConf `json:"interval"`
	MergeField      string            `json:"mergeField"`
	Format          string            `json:"format"`
	Merger          string            `json:"merger"`
	PayloadSchemaId string            `json:"payloadSchemaId"`
	SchemaId        string            `json:"schemaId"`
}

// RateLimitOp handle messages at a regular rate, ignoring messages that arrive too quickly, only keep the most recent message. (default strategy)
// If strategy is set, send through all messages as well as trigger signal and let strategy node handle the merge.
// Otherwise, send the most recent message at trigger time
// Input: Raw
// Output: Raw as it is
// Concurrency: false
type RateLimitOp struct {
	*defaultSinkNode
	// configs
	c             *limitConf
	mergeStrategy int
	// state
	// keep last strategy
	latest any
	// merged items
	frameSet map[any]map[string]any
	// only when mergeField is set
	decoder message.PartialDecoder
	// only when using merger
	merger modules.Merger
}

func NewRateLimitOp(ctx api.StreamContext, name string, rOpt *def.RuleOption, schema map[string]*ast.JsonStreamField, props map[string]any) (*RateLimitOp, error) {
	c := &limitConf{}
	err := cast.MapToStruct(props, c)
	if err != nil {
		return nil, err
	}
	if time.Duration(c.Interval) < 1*time.Millisecond {
		return nil, fmt.Errorf("interval should be larger than 1ms")
	}
	o := &RateLimitOp{
		defaultSinkNode: newDefaultSinkNode(name, rOpt),
		c:               c,
	}
	if c.MergeField != "" {
		f := c.Format
		if f == "" {
			return nil, fmt.Errorf("rate limit merge must define format")
		}
		cv, err := converter.GetOrCreateConverter(ctx, c.Format, c.SchemaId, nil, props)
		if err != nil {
			return nil, err
		}
		if d, ok := cv.(message.PartialDecoder); ok {
			o.decoder = d
			o.frameSet = make(map[any]map[string]any)
		} else {
			return nil, fmt.Errorf("format %s does not support partial decode", c.Format)
		}
		o.mergeStrategy = 1
	} else if c.Merger != "" {
		cm, err := converter.GetMerger(ctx, c.Merger, c.PayloadSchemaId, schema, props)
		if err != nil {
			return nil, fmt.Errorf("fail to initiate merge %s: %v", c.Merger, err)
		}
		o.merger = cm
		o.mergeStrategy = 2
	}
	return o, nil
}

// Exec ratelimit op deal with 3 merge strategy
// - latest
// - merge by mergeField (when format and mergeField is set and no payload format)
// - merge by merger (when format, payloadFormat and merger is set)
func (o *RateLimitOp) Exec(ctx api.StreamContext, errCh chan<- error) {
	o.prepareExec(ctx, errCh, "op")
	ticker := timex.GetTicker(time.Duration(o.c.Interval))
	ctx.GetLogger().Infof("rate limit run with interval %v", o.c.Interval)
	go func() {
		defer func() {
			ticker.Stop()
			o.Close()
		}()
		switch o.mergeStrategy {
		case 0: // get the latest
			ctx.GetLogger().Infof("rate limit run with merge strategy 0: latest")
			for {
				select {
				case <-ctx.Done():
					return
				case d := <-o.input:
					if o.span != nil {
						o.span.End()
					}
					dd, processed := o.commonIngest(ctx, d)
					if processed {
						continue
					}
					o.onProcessStart(ctx, dd)
					o.latest = dd
					o.statManager.ProcessTimeEnd()
					o.statManager.IncTotalMessagesProcessed(1)
					o.statManager.SetBufferLength(int64(len(o.input)))
				case t := <-ticker.C:
					if o.latest != nil {
						if o.span != nil {
							o.span.End()
						}
						o.Broadcast(o.latest)
						o.onSend(ctx, o.latest)
						o.latest = nil
					} else {
						ctx.GetLogger().Debugf("ratelimit had nothing to sent at %d", t.UnixMilli())
					}
				}
			}
		case 1: // by mergeField
			ctx.GetLogger().Infof("rate limit run with merge strategy 1: by field")
			for {
				select {
				case <-ctx.Done():
					return
				case d := <-o.input:
					if o.span != nil {
						o.span.End()
					}
					dd, processed := o.commonIngest(ctx, d)
					if processed {
						continue
					}
					o.onProcessStart(ctx, dd)
					var (
						val any
						err error
					)
					switch dt := dd.(type) {
					case *xsql.RawTuple:
						val, err = o.decoder.DecodeField(ctx, dt.Raw(), o.c.MergeField)
						if err != nil {
							break
						}
						o.frameSet[val] = map[string]any{
							"data": dt.Raw(),
						}
						o.latest = dd
					default:
						err = fmt.Errorf("rate limit merge only supports raw but got %v", d)
					}
					if err != nil {
						o.onError(ctx, err)
					}
					o.statManager.ProcessTimeEnd()
					o.statManager.IncTotalMessagesProcessed(1)
					o.statManager.SetBufferLength(int64(len(o.input)))
				case t := <-ticker.C:
					if len(o.frameSet) > 0 {
						rt := o.latest.(*xsql.RawTuple)
						frames := make([]any, 0, len(o.frameSet))
						if conf.IsTesting { // sort it
							keys := make([]int, 0, len(o.frameSet))
							for k := range o.frameSet {
								keys = append(keys, k.(int))
							}
							sort.Ints(keys)
							for _, k := range keys {
								frames = append(frames, o.frameSet[k])
							}
						} else {
							for _, f := range o.frameSet {
								frames = append(frames, f)
							}
						}
						val := &xsql.Tuple{
							Ctx:       rt.Ctx,
							Emitter:   rt.Emitter,
							Timestamp: t,
							Metadata:  rt.Metadata,
							Message: map[string]any{
								"frames": frames,
							},
						}
						if o.span != nil {
							o.span.End()
						}
						o.Broadcast(val)
						o.onSend(ctx, val)
						o.latest = nil
						maps.Clear(o.frameSet)
					} else {
						ctx.GetLogger().Debugf("ratelimit had nothing to sent at %d", t.UnixMilli())
					}
				}
			}
		case 2:
			ctx.GetLogger().Infof("rate limit run with merge strategy 2: by merger")
			for {
				select {
				case <-ctx.Done():
					return
				case d := <-o.input:
					// end the last span after receiving a new item
					if o.span != nil {
						o.span.End()
					}
					dd, processed := o.commonIngest(ctx, d)
					if processed {
						continue
					}
					o.onProcessStart(ctx, dd)
					var err error
					switch dt := dd.(type) {
					case *xsql.RawTuple:
						bbs := o.merger.Split(ctx, dt.Raw())
						for _, bb := range bbs {
							err = o.merger.Merging(ctx, bb)
							if err == nil {
								o.latest = dt
							}
						}
					default:
						err = fmt.Errorf("rate limit merge only supports raw but got %v", d)
					}
					if err != nil {
						o.onError(ctx, err)
					}
					// do not use onProcessEnd because span still need to exist
					o.statManager.ProcessTimeEnd()
					o.statManager.IncTotalMessagesProcessed(1)
					o.statManager.SetBufferLength(int64(len(o.input)))
				case t := <-ticker.C:
					frames, ok := o.merger.Trigger(ctx)
					if ok && o.latest != nil {
						rt := o.latest.(*xsql.RawTuple)
						val := &xsql.Tuple{
							Ctx:       rt.Ctx,
							Emitter:   rt.Emitter,
							Timestamp: t,
							Metadata:  rt.Metadata,
							Message: map[string]any{
								"frames": frames,
							},
						}
						if o.span != nil {
							o.span.End()
						}
						o.Broadcast(val)
						o.onSend(ctx, val)
						o.latest = nil
					} else {
						ctx.GetLogger().Debugf("ratelimit had nothing to sent at %d", t.UnixMilli())
					}
				}
			}
		}
	}()
}

func (o *RateLimitOp) ResetSchema(ctx api.StreamContext, schema map[string]*ast.JsonStreamField) {
	if o.mergeStrategy != 2 {
		return
	}
	if fastDecoder, ok := o.merger.(message.SchemaResetAbleConverter); ok {
		ctx.GetLogger().Infof("reset schema to shared stream")
		fastDecoder.ResetSchema(schema)
	}
}
