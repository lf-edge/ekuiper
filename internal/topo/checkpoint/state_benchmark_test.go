// Copyright 2026 EMQ Technologies Co., Ltd.
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

package checkpoint_test

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

func init() {
	gob.Register([]*xsql.Tuple{})
}

func BenchmarkCheckpointState(b *testing.B) {
	for _, tupleCount := range []int{0, 1, 10, 100, 1000, 10000} {
		state := benchmarkState(tupleCount)
		frozen, err := checkpoint.EncodeState(state)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("Freeze/Tuples_%d", tupleCount), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(frozen)))
			for b.Loop() {
				if _, err := checkpoint.EncodeState(state); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("Envelope/Tuples_%d", tupleCount), func(b *testing.B) {
			envelope := struct {
				Format    string
				Version   uint16
				Operators map[string][]byte
			}{
				Format:    "ekuiper-checkpoint",
				Version:   2,
				Operators: map[string][]byte{"window": frozen},
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(frozen)))
			for b.Loop() {
				var buf bytes.Buffer
				if err := gob.NewEncoder(&buf).Encode(envelope); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSourceOffsetClone(b *testing.B) {
	offsets := map[string]map[string]interface{}{
		"Scalar": {"offset": int64(42)},
		"Map": {
			"offset": map[string]interface{}{
				"partition-0": int64(42),
				"partition-1": int64(84),
			},
		},
	}
	for name, state := range offsets {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				frozen, err := checkpoint.EncodeState(state)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := checkpoint.DecodeState(frozen); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkAggregateAliasCache(b *testing.B) {
	expressions := map[string]ast.Expr{
		"DirectField": &ast.FieldRef{Name: "id", StreamName: ast.DefaultStream},
		"Alias": &ast.FieldRef{
			Name:       "derived",
			StreamName: ast.AliasStream,
			AliasRef: &ast.AliasRef{
				Expression: &ast.FieldRef{Name: "id", StreamName: ast.DefaultStream},
			},
		},
	}
	for name, expression := range expressions {
		b.Run(name, func(b *testing.B) {
			for _, tupleCount := range []int{1, 10, 100, 1000, 10000} {
				tuples := benchmarkTuples(tupleCount)
				content := make([]xsql.Row, len(tuples))
				for i, tuple := range tuples {
					content[i] = tuple
				}
				window := &xsql.WindowTuples{Content: content}
				b.Run(fmt.Sprintf("Tuples_%d", tupleCount), func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						_ = window.AggregateEval(expression, nil)
					}
				})
			}
		})
	}
}

func benchmarkState(tupleCount int) map[string]interface{} {
	return map[string]interface{}{
		"watermark": time.UnixMilli(1000),
		"buffer":    benchmarkTuples(tupleCount),
	}
}

func benchmarkTuples(tupleCount int) []*xsql.Tuple {
	tuples := make([]*xsql.Tuple, tupleCount)
	for i := range tuples {
		tuples[i] = &xsql.Tuple{
			Emitter:   "demo",
			Timestamp: time.UnixMilli(int64(i)),
			Message: xsql.Message{
				"id":    int64(i),
				"value": float64(i) * 1.5,
				"name":  "checkpoint-benchmark",
				"ok":    true,
				"tag":   "sensor",
			},
			Metadata: xsql.Metadata{"topic": "demo"},
			Props:    map[string]string{"source": "benchmark"},
			AffiliateRow: xsql.AffiliateRow{
				CalCols:  map[string]interface{}{"calculated": i},
				AliasMap: map[string]interface{}{"alias": i},
			},
		}
	}
	return tuples
}
