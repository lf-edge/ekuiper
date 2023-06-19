// Copyright 2023 EMQ Technologies Co., Ltd.
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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
)

func TestSingleStreamWatermark(t *testing.T) {
	tests := []struct {
		name    string
		latetol int64
		inputs  []any // a tuple or a window
		outputs []any
	}{
		{
			name: "ordered tuple",
			inputs: []any{
				&xsql.Tuple{
					Emitter: "demo",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 10,
				},
				&xsql.Tuple{
					Emitter: "demo",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 20,
				},
				&xsql.Tuple{
					Emitter: "demo",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 30,
				},
			},
			outputs: []any{
				&xsql.Tuple{
					Emitter: "demo",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 10,
				},
				&xsql.Tuple{
					Emitter: "demo",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 20,
				},
				&xsql.Tuple{
					Emitter: "demo",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 30,
				},
			},
		}, {
			name:    "disordered tuple",
			latetol: 5,
			inputs: []any{
				&xsql.Tuple{
					Emitter: "demo",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 20,
				},
				&xsql.Tuple{
					Emitter: "demo",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 10,
				},
				&xsql.Tuple{
					Emitter: "demo",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 30,
				},
				&xsql.Tuple{
					Emitter: "demo",
					Message: map[string]interface{}{
						"a": 5,
						"b": "aaaa",
					},
					Timestamp: 32,
				},
				&xsql.Tuple{
					Emitter: "demo",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 32,
				},
				&xsql.Tuple{
					Emitter: "demo",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 28,
				},
				&xsql.Tuple{
					Emitter: "demo",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 40,
				},
			},
			outputs: []any{
				&xsql.Tuple{
					Emitter: "demo",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 20,
				},
				&xsql.Tuple{
					Emitter: "demo",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 28,
				},
				&xsql.Tuple{
					Emitter: "demo",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 30,
				},
				&xsql.Tuple{
					Emitter: "demo",
					Message: map[string]interface{}{
						"a": 5,
						"b": "aaaa",
					},
					Timestamp: 32,
				},
				&xsql.Tuple{
					Emitter: "demo",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 32,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			contextLogger := conf.Log.WithField("rule", "TestWatermark")
			ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
			tempStore, _ := state.CreateStore("TestWatermark", api.AtMostOnce)
			nctx := ctx.WithMeta("TestWatermark", "test", tempStore)
			w := NewWatermarkOp("mock", false, 0, []string{"demo"}, &api.RuleOption{
				IsEventTime:        true,
				LateTol:            tt.latetol,
				Concurrency:        0,
				BufferLength:       0,
				SendMetaToSink:     false,
				SendError:          false,
				Qos:                0,
				CheckpointInterval: 0,
			})
			errCh := make(chan error)
			outputCh := make(chan interface{}, 50)
			w.outputs["mock"] = outputCh
			w.Exec(nctx, errCh)

			in := 0
			out := 0
			result := make([]interface{}, len(tt.outputs))
			for in < len(tt.inputs) || out < len(tt.outputs) {
				// may send more than once
				if in < len(tt.inputs) {
					select {
					case err := <-errCh:
						t.Error(err)
						return
					case w.input <- tt.inputs[in]:
						in++
					case outval := <-outputCh:
						result[out] = outval
						out++
					case <-time.After(5 * time.Second):
						t.Error("send message timeout")
						return
					}
				} else {
					select {
					case err := <-errCh:
						t.Error(err)
						return
					case outval := <-outputCh:
						result[out] = outval
						out++
					case <-time.After(5 * time.Second):
						t.Error("send message timeout")
						return
					}
				}
			}
			assert.Equal(t, tt.outputs, result)
		})
	}
}

func TestMultiStreamWatermark(t *testing.T) {
	tests := []struct {
		name    string
		latetol int64
		inputs  []any // a tuple or a window
		outputs []any
	}{
		{
			name: "ordered tuple",
			inputs: []any{
				&xsql.Tuple{
					Emitter: "demo1",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 10,
				},
				&xsql.Tuple{
					Emitter: "demo2",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 20,
				},
				&xsql.Tuple{
					Emitter: "demo1",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 30,
				},
			},
			outputs: []any{
				&xsql.Tuple{
					Emitter: "demo1",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 10,
				},
				&xsql.WatermarkTuple{
					Timestamp: 10,
				},
				&xsql.Tuple{
					Emitter: "demo2",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 20,
				},
				&xsql.WatermarkTuple{
					Timestamp: 20,
				},
			},
		}, {
			name:    "disordered tuple",
			latetol: 5,
			inputs: []any{
				&xsql.Tuple{
					Emitter: "demo1",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 20,
				},
				&xsql.Tuple{
					Emitter: "demo2",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 10,
				},
				&xsql.Tuple{
					Emitter: "demo1",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 30,
				},
				&xsql.Tuple{
					Emitter: "demo2",
					Message: map[string]interface{}{
						"a": 5,
						"b": "aaaa",
					},
					Timestamp: 32,
				},
				&xsql.Tuple{
					Emitter: "demo1",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 32,
				},
				&xsql.Tuple{
					Emitter: "demo2",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 28,
				},
				&xsql.Tuple{
					Emitter: "demo1",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 40,
				},
				&xsql.Tuple{
					Emitter: "demo2",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 45,
				},
			},
			outputs: []any{
				&xsql.WatermarkTuple{
					Timestamp: 5,
				},
				&xsql.Tuple{
					Emitter: "demo2",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 10,
				},
				&xsql.Tuple{
					Emitter: "demo1",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 20,
				},
				&xsql.WatermarkTuple{
					Timestamp: 25,
				},
				&xsql.WatermarkTuple{
					Timestamp: 27,
				},
				&xsql.Tuple{
					Emitter: "demo2",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 28,
				},
				&xsql.Tuple{
					Emitter: "demo1",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 30,
				},
				&xsql.Tuple{
					Emitter: "demo2",
					Message: map[string]interface{}{
						"a": 5,
						"b": "aaaa",
					},
					Timestamp: 32,
				},
				&xsql.Tuple{
					Emitter: "demo1",
					Message: map[string]interface{}{
						"a": 6,
						"b": "aaaa",
					},
					Timestamp: 32,
				},
				&xsql.WatermarkTuple{
					Timestamp: 35,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			contextLogger := conf.Log.WithField("rule", "TestWatermark")
			ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
			tempStore, _ := state.CreateStore("TestWatermark", api.AtMostOnce)
			nctx := ctx.WithMeta("TestWatermark", "test", tempStore)
			w := NewWatermarkOp("mock", true, 0, []string{"demo1", "demo2"}, &api.RuleOption{
				IsEventTime:        true,
				LateTol:            tt.latetol,
				Concurrency:        0,
				BufferLength:       0,
				SendMetaToSink:     false,
				SendError:          false,
				Qos:                0,
				CheckpointInterval: 0,
			})
			errCh := make(chan error)
			outputCh := make(chan interface{}, 50)
			w.outputs["mock"] = outputCh
			w.Exec(nctx, errCh)

			in := 0
			out := 0
			result := make([]interface{}, len(tt.outputs))
			for in < len(tt.inputs) || out < len(tt.outputs) {
				// may send more than once
				if in < len(tt.inputs) {
					select {
					case err := <-errCh:
						t.Error(err)
						return
					case w.input <- tt.inputs[in]:
						in++
					case outval := <-outputCh:
						// fmt.Printf("%v\n", outval)
						result[out] = outval
						out++
					case <-time.After(5 * time.Second):
						t.Error("send message timeout")
						return
					}
				} else {
					select {
					case err := <-errCh:
						t.Error(err)
						return
					case outval := <-outputCh:
						// fmt.Printf("%v\n", outval)
						result[out] = outval
						out++
					case <-time.After(5 * time.Second):
						t.Error("send message timeout")
						return
					}
				}
			}
			assert.Equal(t, tt.outputs, result)
		})
	}
}
