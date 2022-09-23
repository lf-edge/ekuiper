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

package node

import (
	"fmt"
	"github.com/benbjohnson/clock"
	"github.com/lf-edge/ekuiper/internal/binder"
	"github.com/lf-edge/ekuiper/internal/binder/io"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/lookup"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"reflect"
	"testing"
	"time"
)

type mockLookupSrc struct {
	data []api.SourceTuple // new mock data
}

func (m *mockLookupSrc) Open(_ api.StreamContext) error {
	return nil
}

func (m *mockLookupSrc) Configure(_ string, _ map[string]interface{}) error {
	return nil
}

// Lookup accept int value as the first array value
func (m *mockLookupSrc) Lookup(_ api.StreamContext, fields []string, _ []string, values []interface{}) ([]api.SourceTuple, error) {
	if len(fields) > 0 { // if fields is not empty, the value will be kept
		if m.data != nil {
			return m.data, nil
		} else {
			m.data = []api.SourceTuple{api.NewDefaultSourceTuple(map[string]interface{}{
				"newA": 1000,
				"newB": 1000,
			}, nil)}
		}
	}
	a1, ok := values[0].(int)
	if ok {
		var result []api.SourceTuple
		c := a1 % 2
		if c != 0 {
			result = append(result, api.NewDefaultSourceTuple(map[string]interface{}{
				"newA": c,
				"newB": c * 2,
			}, nil))
		}
		c = a1 % 3
		if c != 0 {
			result = append(result, api.NewDefaultSourceTuple(map[string]interface{}{
				"newA": c,
				"newB": c * 2,
			}, nil))
		}
		c = a1 % 5
		if c != 0 {
			result = append(result, api.NewDefaultSourceTuple(map[string]interface{}{
				"newA": c,
				"newB": c * 2,
			}, nil))
		}
		c = a1 % 7
		if c != 0 {
			result = append(result, api.NewDefaultSourceTuple(map[string]interface{}{
				"newA": c,
				"newB": c * 2,
			}, nil))
		}
		return result, nil
	} else {
		return []api.SourceTuple{
			api.NewDefaultSourceTuple(map[string]interface{}{
				"newA": 0,
				"newB": 0,
			}, nil),
		}, nil
	}
}

func (m *mockLookupSrc) Close(_ api.StreamContext) error {
	// do nothing
	return nil
}

type mockFac struct{}

func (m *mockFac) Source(_ string) (api.Source, error) {
	return nil, nil
}

func (m *mockFac) LookupSource(name string) (api.LookupSource, error) {
	if name == "mock" {
		return &mockLookupSrc{}, nil
	}
	return nil, nil
}

// init mock lookup source factory
func init() {
	io.Initialize([]binder.FactoryEntry{
		{Name: "native plugin", Factory: &mockFac{}, Weight: 10},
	})
}

func TestLookup(t *testing.T) {
	var tests = []struct {
		input  interface{} // a tuple or a window
		output *xsql.JoinTuples
	}{
		{
			input: &xsql.Tuple{
				Emitter: "demo",
				Message: map[string]interface{}{
					"a": 6,
					"b": "aaaa",
				},
			},
			output: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{
								Emitter: "demo",
								Message: map[string]interface{}{
									"a": 6,
									"b": "aaaa",
								},
							},
							&xsql.Tuple{
								Emitter: "mock",
								Message: map[string]interface{}{
									"newA": 1,
									"newB": 2,
								},
							},
						},
					}, {
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{
								Emitter: "demo",
								Message: map[string]interface{}{
									"a": 6,
									"b": "aaaa",
								},
							},
							&xsql.Tuple{
								Emitter: "mock",
								Message: map[string]interface{}{
									"newA": 6,
									"newB": 12,
								},
							},
						},
					},
				},
			},
		},
		{
			input: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "demo",
						Message: map[string]interface{}{
							"a": 9,
							"b": "aaaa",
						},
					},
					&xsql.Tuple{
						Emitter: "demo",
						Message: map[string]interface{}{
							"a": 4,
							"b": "bbaa",
						},
					},
				},
			},
			output: &xsql.JoinTuples{
				Content: []*xsql.JoinTuple{
					{
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{
								Emitter: "demo",
								Message: map[string]interface{}{
									"a": 9,
									"b": "aaaa",
								},
							},
							&xsql.Tuple{
								Emitter: "mock",
								Message: map[string]interface{}{
									"newA": 1,
									"newB": 2,
								},
							},
						},
					}, {
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{
								Emitter: "demo",
								Message: map[string]interface{}{
									"a": 9,
									"b": "aaaa",
								},
							},
							&xsql.Tuple{
								Emitter: "mock",
								Message: map[string]interface{}{
									"newA": 4,
									"newB": 8,
								},
							},
						},
					}, {
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{
								Emitter: "demo",
								Message: map[string]interface{}{
									"a": 9,
									"b": "aaaa",
								},
							},
							&xsql.Tuple{
								Emitter: "mock",
								Message: map[string]interface{}{
									"newA": 2,
									"newB": 4,
								},
							},
						},
					}, {
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{
								Emitter: "demo",
								Message: map[string]interface{}{
									"a": 4,
									"b": "bbaa",
								},
							},
							&xsql.Tuple{
								Emitter: "mock",
								Message: map[string]interface{}{
									"newA": 1,
									"newB": 2,
								},
							},
						},
					}, {
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{
								Emitter: "demo",
								Message: map[string]interface{}{
									"a": 4,
									"b": "bbaa",
								},
							},
							&xsql.Tuple{
								Emitter: "mock",
								Message: map[string]interface{}{
									"newA": 4,
									"newB": 8,
								},
							},
						},
					}, {
						Tuples: []xsql.TupleRow{
							&xsql.Tuple{
								Emitter: "demo",
								Message: map[string]interface{}{
									"a": 4,
									"b": "bbaa",
								},
							},
							&xsql.Tuple{
								Emitter: "mock",
								Message: map[string]interface{}{
									"newA": 4,
									"newB": 8,
								},
							},
						},
					},
				},
			},
		},
	}
	options := &ast.Options{
		DATASOURCE:        "mock",
		TYPE:              "mock",
		STRICT_VALIDATION: true,
		KIND:              "lookup",
	}
	lookup.CreateInstance("mock", "mock", options)
	contextLogger := conf.Log.WithField("rule", "TestLookup")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	l, _ := NewLookupNode("mock", []string{}, []string{"a"}, ast.INNER_JOIN, []ast.Expr{&ast.FieldRef{
		StreamName: "",
		Name:       "a",
	}}, options, &api.RuleOption{
		IsEventTime:        false,
		LateTol:            0,
		Concurrency:        0,
		BufferLength:       0,
		SendMetaToSink:     false,
		SendError:          false,
		Qos:                0,
		CheckpointInterval: 0,
	})
	errCh := make(chan error)
	outputCh := make(chan interface{}, 1)
	l.outputs["mock"] = outputCh
	l.Exec(ctx, errCh)
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		select {
		case err := <-errCh:
			t.Error(err)
			return
		case l.input <- tt.input:
		case <-time.After(1 * time.Second):
			t.Error("send message timeout")
			return
		}
		select {
		case err := <-errCh:
			t.Error(err)
			return
		case output := <-outputCh:
			if !reflect.DeepEqual(tt.output, output) {
				t.Errorf("\ncase %d: expect %v but got %v", i, tt.output, output)
			}
		case <-time.After(1 * time.Second):
			t.Error("send message timeout")
			return
		}
	}
}

func TestCachedLookup(t *testing.T) {
	options := &ast.Options{
		DATASOURCE:        "mock",
		TYPE:              "mock",
		STRICT_VALIDATION: true,
		KIND:              "lookup",
	}
	lookup.CreateInstance("mock", "mock", options)
	contextLogger := conf.Log.WithField("rule", "TestLookup")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	l, _ := NewLookupNode("mock", []string{"fixed"}, []string{"a"}, ast.INNER_JOIN, []ast.Expr{&ast.FieldRef{
		StreamName: "",
		Name:       "a",
	}}, options, &api.RuleOption{
		IsEventTime:        false,
		LateTol:            0,
		Concurrency:        0,
		BufferLength:       0,
		SendMetaToSink:     false,
		SendError:          false,
		Qos:                0,
		CheckpointInterval: 0,
	})
	l.conf = &LookupConf{
		Cache:           true,
		CacheTTL:        20,
		CacheMissingKey: false,
	}
	errCh := make(chan error)
	outputCh := make(chan interface{}, 1)
	l.outputs["mock"] = outputCh
	l.Exec(ctx, errCh)
	input := &xsql.Tuple{
		Emitter: "demo",
		Message: map[string]interface{}{
			"a": 6,
			"b": "aaaa",
		},
	}
	outputBefore := &xsql.JoinTuples{
		Content: []*xsql.JoinTuple{
			{
				Tuples: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "demo",
						Message: map[string]interface{}{
							"a": 6,
							"b": "aaaa",
						},
					},
					&xsql.Tuple{
						Emitter: "mock",
						Message: map[string]interface{}{
							"newA": 1,
							"newB": 2,
						},
						Timestamp: 11000,
					},
				},
			}, {
				Tuples: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "demo",
						Message: map[string]interface{}{
							"a": 6,
							"b": "aaaa",
						},
					},
					&xsql.Tuple{
						Emitter: "mock",
						Message: map[string]interface{}{
							"newA": 6,
							"newB": 12,
						},
						Timestamp: 11000,
					},
				},
			},
		},
	}
	outputAfter := &xsql.JoinTuples{
		Content: []*xsql.JoinTuple{
			{
				Tuples: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "demo",
						Message: map[string]interface{}{
							"a": 6,
							"b": "aaaa",
						},
					},
					&xsql.Tuple{
						Emitter: "mock",
						Message: map[string]interface{}{
							"newA": 1000,
							"newB": 1000,
						},
						Timestamp: 22000,
					},
				},
			},
		},
	}
	// First run and the set mock result
	clock := conf.Clock.(*clock.Mock)
	select {
	case err := <-errCh:
		t.Error(err)
		return
	case l.input <- input:
	case <-time.After(1 * time.Second):
		t.Error("send message timeout")
		return
	}
	select {
	case err := <-errCh:
		t.Error(err)
		return
	case <-outputCh:
		//if !reflect.DeepEqual(output, outputBefore) {
		//	t.Errorf("\nfirst case: expect %v but got %v", output, outputBefore)
		//}
	case <-time.After(1 * time.Second):
		t.Error("send message timeout")
		return
	}
	// Get cache
	clock.Add(11 * time.Second)
	select {
	case err := <-errCh:
		t.Error(err)
		return
	case l.input <- input:
	case <-time.After(1 * time.Second):
		t.Error("send message timeout")
		return
	}
	select {
	case err := <-errCh:
		t.Error(err)
		return
	case output := <-outputCh:
		if !reflect.DeepEqual(output, outputBefore) {
			t.Errorf("\ncached case: expect %v but got %v", output, outputBefore)
		}
	case <-time.After(1 * time.Second):
		t.Error("send message timeout")
		return
	}
	// Cache expired
	clock.Add(11 * time.Second)
	select {
	case err := <-errCh:
		t.Error(err)
		return
	case l.input <- input:
	case <-time.After(1 * time.Second):
		t.Error("send message timeout")
		return
	}
	select {
	case err := <-errCh:
		t.Error(err)
		return
	case output := <-outputCh:
		if !reflect.DeepEqual(output, outputAfter) {
			t.Errorf("\nexpired case: expect %v but got %v", output, outputAfter)
		}
	case <-time.After(1 * time.Second):
		t.Error("send message timeout")
		return
	}
}
