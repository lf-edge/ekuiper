// Copyright 2022-2025 EMQ Technologies Co., Ltd.
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

package neuron

import (
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/mock"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestSink(t *testing.T) {
	server, ch := mockNeuron(false, true, DefaultNeuronUrl)
	defer server.Close()

	s := GetSink().(api.TupleCollector)
	data := []any{
		&xsql.Tuple{
			Message: map[string]any{
				"temperature": 22,
				"humidity":    50,
				"status":      "green",
			},
			Props: map[string]string{
				"test1": "dynamic",
				"grp":   "dynamic",
			},
		},
		&xsql.WindowTuples{
			Content: []xsql.Row{
				&xsql.Tuple{
					Message: map[string]any{
						"temperature": 25,
						"humidity":    82,
						"status":      "wet",
					},
				},
				&xsql.Tuple{
					Message: map[string]any{
						"temperature": 33,
						"humidity":    60,
						"status":      "hot",
					},
				},
			},
		},
	}
	err := mock.RunTupleSinkCollect(s, data, map[string]any{
		"url":       DefaultNeuronUrl,
		"nodeName":  "test1",
		"groupName": "grp",
		"tags":      []string{"temperature", "status"},
		"raw":       false,
	})
	assert.NoError(t, err)

	exp := []string{
		`{"group_name":"dynamic","node_name":"dynamic","tags":[{"tag_name":"temperature","value":22},{"tag_name":"status","value":"green"}]}`,
		`{"group_name":"grp","node_name":"test1","tags":[{"tag_name":"temperature","value":25},{"tag_name":"status","value":"wet"}]}`,
		`{"group_name":"grp","node_name":"test1","tags":[{"tag_name":"temperature","value":33},{"tag_name":"status","value":"hot"}]}`,
	}
	var actual []string
	ticker := time.After(5 * time.Second)
	for i := 0; i < len(exp); i++ {
		select {
		case <-ticker:
			t.Errorf("timeout")
			return
		case d := <-ch:
			actual = append(actual, string(d))
		}
	}

	assert.Equal(t, exp, actual)
}

func TestSinkNoTags(t *testing.T) {
	server, ch := mockNeuron(false, true, DefaultNeuronUrl)
	defer server.Close()

	s := GetSink().(api.TupleCollector)
	data := []any{
		&xsql.Tuple{
			Message: map[string]any{
				"temperature": 22,
				"humidity":    50,
				"status":      "green",
			},
			Props: map[string]string{
				"test1": "dynamic",
				"grp":   "dynamic",
			},
		},
	}
	err := mock.RunTupleSinkCollect(s, data, map[string]any{
		"url":       DefaultNeuronUrl,
		"nodeName":  "test1",
		"groupName": "grp",
		"raw":       false,
	})
	assert.NoError(t, err)

	exp := []string{
		`{"group_name":"dynamic","node_name":"dynamic","tags":[{"tag_name":"humidity","value":50},{"tag_name":"status","value":"green"},{"tag_name":"temperature","value":22}]}`,
	}
	var actual []string
	ticker := time.After(5 * time.Second)
	for i := 0; i < len(exp); i++ {
		select {
		case <-ticker:
			t.Errorf("timeout")
			return
		case d := <-ch:
			actual = append(actual, string(d))
		}
	}

	assert.Equal(t, exp, actual)
}

func TestSinkRaw(t *testing.T) {
	server, ch := mockNeuron(false, true, DefaultNeuronUrl)
	defer server.Close()

	s := GetSink().(api.TupleCollector)
	data := []any{
		&xsql.Tuple{
			Message: map[string]any{
				"temperature": 22,
				"humidity":    50,
				"status":      "green",
			},
		},
		&xsql.Tuple{
			Message: map[string]any{
				"temperature": 25,
				"humidity":    82,
				"status":      "wet",
			},
		},
		&xsql.Tuple{
			Message: map[string]any{
				"temperature": 33,
				"humidity":    60,
				"status":      "hot",
			},
		},
	}
	err := mock.RunTupleSinkCollect(s, data, map[string]any{
		"url": DefaultNeuronUrl,
		"raw": true,
	})
	assert.NoError(t, err)

	exp := []string{
		`{"humidity":50,"status":"green","temperature":22}`,
		`{"humidity":82,"status":"wet","temperature":25}`,
		`{"humidity":60,"status":"hot","temperature":33}`,
	}
	var actual []string
	ticker := time.After(5 * time.Second)
	for i := 0; i < len(exp); i++ {
		select {
		case <-ticker:
			t.Errorf("timeout")
			return
		case d := <-ch:
			actual = append(actual, string(d))
		}
	}

	assert.Equal(t, exp, actual)
	time.Sleep(100 * time.Millisecond)
}

func TestSinkProvision(t *testing.T) {
	ctx := mockContext.NewMockContext("t", "tt")
	s := &sink{}
	err := s.Provision(ctx, map[string]any{
		"url": "3434",
	})
	assert.Error(t, err)
	assert.EqualError(t, err, "only tcp and ipc scheme are supported")

	err = s.Provision(ctx, map[string]any{
		"url":  "tcp://127.0.0.1:8000",
		"tags": "tag2",
	})
	assert.Error(t, err)
	assert.EqualError(t, err, "1 error(s) decoding:\n\n* 'tags': source data must be an array or slice, got string")

	err = s.Provision(ctx, map[string]any{
		"url": "tcp://127.0.0.1:8000",
	})
	assert.Error(t, err)
	assert.EqualError(t, err, "node name is required if raw is not set")

	err = s.Provision(ctx, map[string]any{
		"url":      "tcp://127.0.0.1:8000",
		"nodeName": "test",
	})
	assert.Error(t, err)
	assert.EqualError(t, err, "group name is required if raw is not set")

	err = s.Provision(ctx, map[string]any{
		"url": "tcp://127.0.0.1:8000",
		"raw": true,
	})
	assert.NoError(t, err)

	err = s.Close(ctx)
	assert.NoError(t, err)
}

func TestExtractSinkTraceData(t *testing.T) {
	data := &mockData{
		traceID: "1234567890abcdef",
		spanID:  "12345678",
	}
	rawData := []byte(`{"a":1}`)
	ctx := mockContext.NewMockContext("1", "2")
	ctx.EnableTracer(true)
	got := extractSpanContextIntoData(ctx, data, rawData)
	require.Equal(t, NeuronTraceHeader, got[:len(NeuronTraceHeader)])
	require.Equal(t, []byte(data.traceID), got[NeuronTraceIDStartIndex:NeuronTraceIDEndIndex])
	require.Equal(t, rawData, got[NeuronTraceHeaderLen:])
}

type mockData struct {
	traceID string
	spanID  string
}

func (m *mockData) GetTracerCtx() api.StreamContext {
	var traceID [16]byte
	for i := 0; i < 16; i++ {
		traceID[i] = m.traceID[i]
	}
	var spanID [8]byte
	for i := 0; i < 8; i++ {
		spanID[i] = m.spanID[i]
	}
	ctx := trace.ContextWithSpanContext(topoContext.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID,
		SpanID:  spanID,
	}))
	return topoContext.WithContext(ctx)
}

func (m *mockData) SetTracerCtx(ctx api.StreamContext) {}
