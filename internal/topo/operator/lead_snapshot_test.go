// Copyright 2022-2026 EMQ Technologies Co., Ltd.
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

package operator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

func TestLeadSnapshotIsolation(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		name := "tuple"
		if indexed {
			name = "slice"
		}
		t.Run(name, func(t *testing.T) {
			store, _ := state.CreateStore(t.Name(), def.AtMostOnce)
			ctx := context.WithValue(context.Background(), context.LoggerKey, conf.Log).WithMeta(t.Name(), "analytic", store)
			fv, afv := xsql.NewFunctionValuersForOp(ctx)
			field := &ast.FieldRef{Name: "v"}
			index := -1
			if indexed {
				field.HasIndex = true
				field.SourceIndex = 0
				index = 0
			}
			newOp := func() *AnalyticFuncsOp {
				return &AnalyticFuncsOp{Funcs: []*ast.Call{{Name: "lead", FuncId: 0, CacheIndex: index, CachedField: "next", Args: []ast.Expr{field}}}}
			}
			row := func(v int) xsql.Row {
				if indexed {
					return &xsql.SliceTuple{SourceContent: model.SliceVal{v}, Timestamp: time.Unix(int64(v), 0)}
				}
				return &xsql.Tuple{Message: xsql.Message{"v": v}, Timestamp: time.Unix(int64(v), 0)}
			}
			value := func(r xsql.Row) any {
				if indexed {
					return r.(model.IndexValuer).TempByIndex(0)
				}
				v, _ := r.Value("next", "")
				return v
			}
			op := newOp()
			require.Nil(t, op.Apply(ctx, row(1), fv, afv))
			_, err := op.Watermark(ctx, &xsql.WatermarkTuple{Timestamp: time.Unix(2, 0)})
			require.NoError(t, err)
			stored, err := ctx.GetState(leadOperatorStateKey)
			require.NoError(t, err)
			require.Nil(t, stored, "rows and watermarks must not build snapshots")
			require.NoError(t, op.Snapshot(ctx))
			stored, err = ctx.GetState(leadOperatorStateKey)
			require.NoError(t, err)
			snapshot := stored.(leadSnapshot)
			require.Len(t, snapshot.Pending, 1)
			ready := op.Apply(ctx, row(2), fv, afv).([]xsql.Row)
			require.Equal(t, 2, value(ready[0]))
			require.Nil(t, value(snapshot.Pending[0].Row), "live completion must not mutate the saved row")
			require.Equal(t, 1, snapshot.Pending[0].Unresolved)
			restored := newOp()
			ready = restored.Apply(ctx, row(3), fv, afv).([]xsql.Row)
			require.Equal(t, 3, value(ready[0]))
			require.Nil(t, value(snapshot.Pending[0].Row), "restored execution must not mutate its source snapshot")
			require.NoError(t, restored.Snapshot(ctx))
			latest, err := ctx.GetState(leadOperatorStateKey)
			require.NoError(t, err)
			require.Equal(t, time.Unix(3, 0), latest.(leadSnapshot).Pending[0].Row.(xsql.Event).GetTimestamp())
		})
	}
}

func TestLeadSnapshotAtAlignedBarrier(t *testing.T) {
	for _, qos := range []def.Qos{def.AtLeastOnce, def.ExactlyOnce} {
		t.Run(string(rune('0'+qos)), func(t *testing.T) {
			store, _ := state.CreateStore(t.Name(), def.AtMostOnce)
			ctx := context.WithValue(context.Background(), context.LoggerKey, conf.Log).WithMeta(t.Name(), "analytic", store)
			runCtx, cancel := ctx.WithCancel()
			defer cancel()
			op := node.New("lead", &def.RuleOption{BufferLength: 16, SendError: true})
			op.SetOperation(&AnalyticFuncsOp{Funcs: []*ast.Call{{Name: "lead", FuncId: 0, CacheIndex: -1, CachedField: "next", Args: []ast.Expr{&ast.FieldRef{Name: "v"}}, WhenExpr: &ast.BooleanLiteral{Val: false}}}})
			op.SetQos(qos)
			signals := make(chan *checkpoint.Signal, 1)
			responder := checkpoint.NewResponderExecutor(signals, op)
			if qos == def.ExactlyOnce {
				op.SetBarrierHandler(checkpoint.NewBarrierAligner(responder, 2))
			} else {
				op.SetBarrierHandler(checkpoint.NewBarrierTracker(responder, 2))
			}
			out, errCh := make(chan any, 16), make(chan error, 1)
			require.NoError(t, op.AddOutput(out, "test"))
			op.Exec(runCtx, errCh)
			input, _ := op.GetInput()
			send := func(channel string, data any) { input <- &checkpoint.BufferOrEvent{Channel: channel, Data: data} }
			receive := func() any {
				select {
				case r := <-out:
					return r.(*checkpoint.BufferOrEvent).Data
				case err := <-errCh:
					t.Fatalf("operator error: %v", err)
				case <-time.After(5 * time.Second):
					t.Fatal("operator output timed out")
				}
				return nil
			}
			send("a", &xsql.Tuple{Timestamp: time.Unix(1, 0), Message: xsql.Message{"v": 1}})
			send("a", &checkpoint.Barrier{CheckpointId: 1, OpId: "a"})
			send("b", &xsql.Tuple{Timestamp: time.Unix(2, 0), Message: xsql.Message{"v": 2}})
			send("b", &xsql.WatermarkTuple{Timestamp: time.Unix(3, 0)})
			require.IsType(t, &xsql.WatermarkTuple{}, receive())
			stored, err := ctx.GetState(leadOperatorStateKey)
			require.NoError(t, err)
			require.Nil(t, stored, "the first input barrier must not snapshot before alignment")
			send("b", &checkpoint.Barrier{CheckpointId: 1, OpId: "b"})
			require.IsType(t, &checkpoint.Barrier{}, receive())
			select {
			case signal := <-signals:
				require.Equal(t, checkpoint.ACK, signal.Message)
			case <-time.After(5 * time.Second):
				t.Fatal("checkpoint ACK timed out")
			}
			stored, err = ctx.GetState(leadOperatorStateKey)
			require.NoError(t, err)
			snapshot := stored.(leadSnapshot)
			require.Len(t, snapshot.Pending, 2)
			send("b", &xsql.Tuple{Timestamp: time.Unix(4, 0), Message: xsql.Message{"v": 4}})
			send("b", xsql.EOFTuple("done"))
			for i := 0; i < 3; i++ {
				require.IsType(t, &xsql.Tuple{}, receive())
			}
			require.Equal(t, xsql.EOFTuple("done"), receive())
			require.Len(t, snapshot.Pending, 2, "post-barrier rows must not enter the snapshot")
			for _, pending := range snapshot.Pending {
				_, cached := pending.Row.Value("next", "")
				require.False(t, cached, "EOF finalization must not modify checkpoint rows")
			}
		})
	}
}
