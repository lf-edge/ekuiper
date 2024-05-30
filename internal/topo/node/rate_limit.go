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
	"sort"
	"time"

	"golang.org/x/exp/maps"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/converter"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type limitConf struct {
	Interval   time.Duration `json:"interval"`
	MergeField string        `json:"mergeField"`
	Format     string        `json:"format"`
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
	c *limitConf
	// state
	// keep last strategy
	latest any
	// merged items
	frameSet map[any]map[string]any
	// only when merge is set
	decoder message.PartialDecoder
}

func NewRateLimitOp(ctx api.StreamContext, name string, rOpt *def.RuleOption, props map[string]any) (*RateLimitOp, error) {
	c := &limitConf{}
	err := cast.MapToStruct(props, c)
	if err != nil {
		return nil, err
	}
	if c.Interval < 1*time.Millisecond {
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
		cv, err := converter.GetOrCreateConverter(ctx, &ast.Options{
			FORMAT:       c.Format,
			Schema:       nil,
			IsSchemaLess: true,
			IsWildCard:   true,
		})
		if err != nil {
			return nil, err
		}
		if d, ok := cv.(message.PartialDecoder); ok {
			o.decoder = d
			o.frameSet = make(map[any]map[string]any)
		} else {
			return nil, fmt.Errorf("format %s does not support partial decode", c.Format)
		}
	}
	return o, nil
}

func (o *RateLimitOp) Exec(ctx api.StreamContext, errCh chan<- error) {
	o.prepareExec(ctx, errCh, "op")
	ticker := timex.GetTicker(o.c.Interval)
	go func() {
		defer func() {
			ticker.Stop()
			o.Close()
		}()
		if o.c.MergeField != "" { // do merge
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
						o.statManager.IncTotalExceptions(err.Error())
						o.Broadcast(err)
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

						o.Broadcast(&xsql.Tuple{
							Emitter:   rt.Emitter,
							Timestamp: rt.Timestamp,
							Metadata:  rt.Metadata,
							Message: map[string]any{
								"frames": frames,
							},
						})
						o.latest = nil
						maps.Clear(o.frameSet)
						o.statManager.IncTotalRecordsOut()
					} else {
						ctx.GetLogger().Debugf("ratelimit had nothing to sent at %d", t.UnixMilli())
					}
				}
			}
		} else {
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
		}
	}()
}
