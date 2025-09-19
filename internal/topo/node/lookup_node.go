// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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
	"errors"
	"fmt"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/converter"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/lookup"
	"github.com/lf-edge/ekuiper/v2/internal/topo/lookup/cache"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type LookupConf struct {
	Cache           bool              `json:"cache"`
	CacheTTL        cast.DurationConf `json:"cacheTtl"`
	CacheMissingKey bool              `json:"cacheMissingKey"`
}

type srcConf struct {
	PayloadField     string `json:"payloadField"`
	PayloadFormat    string `json:"payloadFormat"`
	PayloadSchemaId  string `json:"payloadSchemaId"`
	PayloadDelimiter string `json:"payloadDelimiter"`
}

// LookupNode will look up the data from the external source when receiving an event
type LookupNode struct {
	*defaultSinkNode

	conf *LookupConf
	c    *srcConf

	joinType ast.JoinType
	vals     []ast.Expr
	fields   []string
	keys     []string
	// If lookupByteSource, the decoders are needed
	isBytesLookup  bool
	formatDecoder  message.Converter
	payloadDecoder message.Converter
}

func NewLookupNode(ctx api.StreamContext, name string, isBytesLookup bool, fields []string, keys []string, joinType ast.JoinType, vals []ast.Expr, srcOptions *ast.Options, options *def.RuleOption, props map[string]any) (*LookupNode, error) {
	lookupConf := &LookupConf{}
	if lc, ok := props["lookup"].(map[string]interface{}); ok {
		err := cast.MapToStruct(lc, lookupConf)
		if err != nil {
			return nil, err
		}
	}
	n := &LookupNode{
		fields:        fields,
		keys:          keys,
		conf:          lookupConf,
		joinType:      joinType,
		vals:          vals,
		isBytesLookup: isBytesLookup,
	}
	n.defaultSinkNode = newDefaultSinkNode(name, options)
	if isBytesLookup {
		sc := &srcConf{}
		e := cast.MapToStruct(props, sc)
		if e != nil {
			return nil, e
		}
		if (sc.PayloadFormat == "" && sc.PayloadField != "") || (sc.PayloadFormat != "" && sc.PayloadField == "") {
			return nil, fmt.Errorf("payloadFormat and payloadField must set together")
		}
		var sch map[string]*ast.JsonStreamField
		if len(fields) > 0 {
			sch = make(map[string]*ast.JsonStreamField, len(fields))
			for _, field := range fields {
				sch[field] = nil
			}
			if sc.PayloadField != "" {
				sch[sc.PayloadField] = nil
			}
		}

		decoder, err := converter.GetOrCreateConverter(ctx, srcOptions.FORMAT, srcOptions.SCHEMAID, sch, props)
		if err != nil {
			msg := fmt.Sprintf("cannot get converter from format %s, schemaId %s: %v", srcOptions.FORMAT, srcOptions.SCHEMAID, err)
			return nil, errors.New(msg)
		}
		n.formatDecoder = decoder

		if sc.PayloadField != "" {
			props["delimiter"] = sc.PayloadDelimiter
			payloadDecoder, err := converter.GetOrCreateConverter(ctx, sc.PayloadFormat, sc.PayloadSchemaId, sch, props)
			if err != nil {
				return nil, fmt.Errorf("cannot get payload converter from payloadFormat %s, schemaId %s: %v", sc.PayloadFormat, sc.PayloadSchemaId, err)
			}
			n.payloadDecoder = payloadDecoder
		}

		n.c = sc
	}
	return n, nil
}

func (n *LookupNode) Exec(ctx api.StreamContext, errCh chan<- error) {
	log := ctx.GetLogger()
	n.prepareExec(ctx, errCh, "op")
	go func() {
		defer func() {
			n.Close()
		}()
		err := infra.SafeRun(func() error {
			ns, err := lookup.Attach(n.name)
			if err != nil {
				return err
			}
			defer lookup.Detach(n.name)
			fv, _ := xsql.NewFunctionValuersForOp(ctx)
			var c *cache.Cache
			if n.conf.Cache {
				c = cache.NewCache(time.Duration(n.conf.CacheTTL), n.conf.CacheMissingKey)
				defer c.Close()
			}
			// Start the lookup source loop
			for {
				log.Debugf("LookupNode %s is looping", n.name)
				select {
				// process incoming item from both streams(transformed) and tables
				case item := <-n.input:
					data, processed := n.commonIngest(ctx, item)
					if processed {
						break
					}
					n.onProcessStart(ctx, data)
					switch d := data.(type) {
					case *xsql.JoinTuples:
						sets := &xsql.JoinTuples{Content: make([]*xsql.JoinTuple, 0), WindowRange: item.(*xsql.JoinTuples).GetWindowRange()}
						err := d.Range(func(i int, r xsql.ReadonlyRow) (bool, error) {
							tr, ok := r.(xsql.Row)
							if !ok {
								return false, fmt.Errorf("Invalid window element, must be a tuple row but got %v", r)
							}
							err := n.lookup(ctx, tr, fv, ns, sets, c)
							if err != nil {
								return false, err
							}
							return true, nil
						})
						if err != nil {
							n.onError(ctx, err)
						} else if sets.Len() > 0 {
							n.Broadcast(sets)
							n.statManager.IncTotalRecordsOut()
						} else {
							ctx.GetLogger().Debugf("lookup return nil")
						}
					case xsql.Row:
						log.Debugf("Lookup Node receive tuple input %s", d)
						sets := &xsql.JoinTuples{Content: make([]*xsql.JoinTuple, 0)}
						err := n.lookup(ctx, d, fv, ns, sets, c)
						if err != nil {
							n.onError(ctx, err)
						} else if sets.Len() > 0 {
							n.Broadcast(sets)
							n.onSend(ctx, sets)
						} else {
							ctx.GetLogger().Debugf("lookup return nil")
						}
					case *xsql.WindowTuples:
						log.Debugf("Lookup Node receive window input %v", d)
						sets := &xsql.JoinTuples{Content: make([]*xsql.JoinTuple, 0), WindowRange: item.(*xsql.WindowTuples).GetWindowRange()}
						err := d.Range(func(i int, r xsql.ReadonlyRow) (bool, error) {
							tr, ok := r.(xsql.Row)
							if !ok {
								return false, fmt.Errorf("Invalid window element, must be a tuple row but got %v", r)
							}
							err := n.lookup(ctx, tr, fv, ns, sets, c)
							if err != nil {
								return false, err
							}
							return true, nil
						})
						if err != nil {
							n.onError(ctx, err)
						} else if sets.Len() > 0 {
							n.Broadcast(sets)
							n.statManager.IncTotalRecordsOut()
						} else {
							ctx.GetLogger().Debugf("lookup return nil")
						}
					default:
						n.onError(ctx, fmt.Errorf("run lookup node error: invalid input type but got %[1]T(%[1]v)", d))
					}
					n.onProcessEnd(ctx)
					n.statManager.SetBufferLength(int64(len(n.input)))
				case <-ctx.Done():
					log.Info("Cancelling lookup node....")
					return nil
				}
			}
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
}

// lookup will lookup the cache firstly, if expires, read the external source
func (n *LookupNode) lookup(ctx api.StreamContext, d xsql.Row, fv *xsql.FunctionValuer, ns api.Source, tuples *xsql.JoinTuples, c *cache.Cache) error {
	ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(d, fv)}
	cvs := make([]interface{}, len(n.vals))
	hasNil := false
	for i, val := range n.vals {
		cvs[i] = ve.Eval(val)
		if cvs[i] == nil {
			hasNil = true
		}
	}
	var (
		r  []map[string]any
		e  error
		ok bool
	)
	if !hasNil { // if any of the value is nil, the lookup will always return empty result
		if c != nil {
			k := fmt.Sprintf("%v", cvs)
			r, ok = c.Get(k)
			if !ok {
				r, e = n.doLookup(ctx, ns, cvs)
				if e != nil {
					return e
				}
				c.Set(k, r)
			}
		} else {
			r, e = n.doLookup(ctx, ns, cvs)
		}
	}
	if e != nil {
		return e
	} else {
		if len(r) == 0 {
			if n.joinType == ast.LEFT_JOIN {
				merged := &xsql.JoinTuple{}
				merged.AddTuple(d)
				tuples.Content = append(tuples.Content, merged)
			} else {
				ctx.GetLogger().Debugf("Lookup Node %s no result found for tuple %s", n.name, d)
				return nil
			}
		}
		if r != nil {
			for _, mm := range r {
				merged := &xsql.JoinTuple{}
				merged.AddTuple(d)
				t := &xsql.Tuple{
					Emitter:   n.name,
					Message:   mm,
					Timestamp: timex.GetNow(),
				}
				merged.AddTuple(t)
				tuples.Content = append(tuples.Content, merged)
			}
		}
		return nil
	}
}

func (n *LookupNode) doLookup(ctx api.StreamContext, ns api.Source, cvs []any) ([]map[string]any, error) {
	if n.isBytesLookup {
		rawRows, err := ns.(api.LookupBytesSource).Lookup(ctx, n.fields, n.keys, cvs)
		if err != nil {
			return nil, err
		}
		result := make([]map[string]any, 0, len(rawRows))
		for _, row := range rawRows {
			r, e := n.decode(ctx, row)
			if e != nil {
				ctx.GetLogger().Errorf("decode row %v error: %v", row, e)
			} else {
				switch rt := r.(type) {
				case []map[string]any:
					result = append(result, rt...)
				case map[string]any:
					result = append(result, rt)
				default:
					ctx.GetLogger().Errorf("decode row %v got unknow result: %v", row, rt)
				}
			}
		}
		return result, nil
	} else {
		return ns.(api.LookupSource).Lookup(ctx, n.fields, n.keys, cvs)
	}
}

// Only called when isBytesLookup is true
// Must guarantee decoders are set
func (n *LookupNode) decode(ctx api.StreamContext, row []byte) (any, error) {
	r, e := n.formatDecoder.Decode(ctx, row)
	if e == nil && n.payloadDecoder != nil {
		switch rt := r.(type) {
		case map[string]any:
			return decodePayload(ctx, n.payloadDecoder, rt, n.c.PayloadField)
		case []map[string]any:
			result := make([]map[string]any, 0, len(rt))
			for _, mm := range rt {
				rr, e := decodePayload(ctx, n.payloadDecoder, mm, n.c.PayloadField)
				if e != nil {
					ctx.GetLogger().Warnf("decode payload of %v got error %v", mm, e)
				} else {
					switch rrt := rr.(type) {
					case map[string]any:
						result = append(result, rrt)
					case []map[string]any:
						result = append(result, rrt...)
					default:
						return nil, fmt.Errorf("payload decoder return non map or map slice")
					}
				}
			}
			return result, nil
		default:
			return nil, fmt.Errorf("decoder return non map or map slice")
		}
	}
	return r, e
}

func decodePayload(ctx api.StreamContext, decoder message.Converter, rt map[string]any, field string) (any, error) {
	payload, ok := rt[field]
	if !ok {
		ctx.GetLogger().Warnf("cannot find payload field %s", field)
		return nil, nil
	}
	raw, err := cast.ToByteA(payload, cast.CONVERT_SAMEKIND)
	if err != nil {
		return nil, fmt.Errorf("payload is not bytes: %v", err)
	}
	return decoder.Decode(ctx, raw)
}
