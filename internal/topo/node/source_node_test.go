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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/topotest/mockclock"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func TestSCNLC(t *testing.T) {
	mc := mockclock.GetMockClock()
	expects := []any{
		&xsql.RawTuple{
			Rawdata:   []byte("hello"),
			Metadata:  map[string]any{"topic": "demo"},
			Timestamp: mc.Now(),
			Emitter:   "mock_connector",
		},
		&xsql.RawTuple{
			Emitter:   "mock_connector",
			Metadata:  map[string]any{"topic": "demo"},
			Timestamp: mc.Now(),
		},
		&xsql.RawTuple{
			Rawdata:   []byte("world"),
			Metadata:  map[string]any{"topic": "demo"},
			Timestamp: mc.Now(),
			Emitter:   "mock_connector",
		},
	}
	var sc api.BytesSource = &MockSourceConnector{
		data: [][]byte{
			[]byte("hello"),
			nil,
			[]byte("world"),
		},
	}
	ctx := mockContext.NewMockContext("rule1", "src1")
	errCh := make(chan error)
	scn, err := NewSourceNode(ctx, "mock_connector", sc, map[string]any{"datasource": "demo"}, &def.RuleOption{
		BufferLength: 1024,
		SendError:    true,
	})
	assert.NoError(t, err)
	result := make(chan any, 10)
	err = scn.AddOutput(result, "testResult")
	assert.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	limit := len(expects)
	actual := make([]any, 0, limit)
	go func() {
		defer wg.Done()
		ticker := time.After(2000 * time.Second)
		for {
			select {
			case sg := <-errCh:
				switch et := sg.(type) {
				case error:
					assert.Fail(t, et.Error())
					return
				default:
					fmt.Println("ctrlCh", et)
				}
			case tuple := <-result:
				actual = append(actual, tuple)
				limit--
				if limit <= 0 {
					return
				}
			case <-ticker:
				assert.Fail(t, "timeout")
				return
			}
		}
	}()
	scn.Open(ctx, errCh)
	wg.Wait()
	for i := 0; i < len(expects); i++ {
		exp := expects[i].(*xsql.RawTuple)
		got := actual[i].(*xsql.RawTuple)
		require.Equal(t, exp.Rawdata, got.Rawdata)
		require.Equal(t, exp.Metadata, got.Metadata)
		require.Equal(t, exp.Emitter, got.Emitter)
	}
}

func TestNewError(t *testing.T) {
	var sc api.BytesSource = &MockSourceConnector{
		data: [][]byte{
			[]byte("hello"),
			[]byte("world"),
		},
	}
	ctx := mockContext.NewMockContext("rule1", "src1")
	_, err := NewSourceNode(ctx, "mock_connector", sc, map[string]any{}, &def.RuleOption{
		BufferLength: 1024,
		SendError:    true,
	})
	assert.Error(t, err)
	assert.Equal(t, "datasource name cannot be empty", err.Error())
	_, err = NewSourceNode(ctx, "mock_connector", sc, map[string]any{"interval": "invalid", "datasource": "demo"}, &def.RuleOption{
		BufferLength: 1024,
		SendError:    true,
	})
	assert.Error(t, err)
	assert.Equal(t, "1 error(s) decoding:\n\n* error decoding 'interval': time: invalid duration \"invalid\"", err.Error())

	var pc api.PullTupleSource = &MockPullSource{}
	_, err = NewSourceNode(ctx, "mock_connector", pc, map[string]any{"datasource": "demo", "interval": "1s"}, &def.RuleOption{
		BufferLength: 1024,
		SendError:    true,
	})
	assert.NoError(t, err)
	assert.True(t, pc.(*MockPullSource).set)
}

func TestConnError(t *testing.T) {
	var sc api.BytesSource = &MockSourceConnector{
		data: nil, // nil data to produce mock connect error
	}
	ctx := mockContext.NewMockContext("rule1", "src1")
	scn, err := NewSourceNode(ctx, "mock_connector", sc, map[string]any{"datasource": "demo2"}, &def.RuleOption{
		BufferLength: 1024,
		SendError:    true,
	})
	assert.NoError(t, err)

	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(1)
	var errResult error
	go func() {
		defer wg.Done()
		ticker := time.After(2 * time.Second)
		for {
			select {
			case sg := <-errCh:
				switch et := sg.(type) {
				case error:
					errResult = et
					return
				default:
					fmt.Println("ctrlCh", et)
				}
			case <-ticker:
				return
			}
		}
	}()
	scn.Open(ctx, errCh)
	wg.Wait()
	assert.Error(t, errResult)
	assert.Equal(t, "data is nil", errResult.Error())
}

func TestPull(t *testing.T) {
	mc := mockclock.GetMockClock()
	expects := []any{
		&xsql.Tuple{
			Metadata:  map[string]any{"topic": "demo"},
			Timestamp: mc.Now(),
			Emitter:   "mock_connector",
			Message:   map[string]any{"index": 1},
		},
		&xsql.RawTuple{
			Emitter:   "mock_connector",
			Metadata:  map[string]any{"topic": "demo"},
			Timestamp: mc.Now().Add(time.Second),
			Rawdata:   []byte{2},
		},
		&xsql.Tuple{
			Timestamp: mc.Now().Add(2 * time.Second),
			Emitter:   "mock_connector",
			Message:   map[string]any{"index": 3},
		},
		&xsql.Tuple{
			Timestamp: mc.Now().Add(3 * time.Second),
			Emitter:   "mock_connector",
			Message:   map[string]any{"index": 4},
		},
		&xsql.Tuple{
			Timestamp: mc.Now().Add(4 * time.Second),
			Emitter:   "mock_connector",
			Message:   map[string]any{"index": 5},
			Metadata:  map[string]any{"topic": "demo"},
		},
	}
	var sc api.PullTupleSource = &MockPullSource{}
	ctx := mockContext.NewMockContext("rule1", "src1")
	errCh := make(chan error)
	scn, err := NewSourceNode(ctx, "mock_connector", sc, map[string]any{"datasource": "demo", "interval": "1s"}, &def.RuleOption{
		BufferLength: 1024,
		SendError:    true,
	})
	assert.NoError(t, err)
	result := make(chan any, 10)
	err = scn.AddOutput(result, "testResult")
	assert.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	limit := len(expects)
	actual := make([]any, 0, limit)
	go func() {
		defer wg.Done()
		ticker := time.After(2000 * time.Second)
		for {
			select {
			case sg := <-errCh:
				switch et := sg.(type) {
				case error:
					assert.Fail(t, et.Error())
					return
				default:
					fmt.Println("ctrlCh", et)
				}
			case tuple := <-result:
				actual = append(actual, tuple)
				limit--
				if limit <= 0 {
					return
				}
			case <-ticker:
				assert.Fail(t, "timeout")
				return
			}
		}
	}()
	scn.Open(ctx, errCh)
	time.Sleep(10 * time.Millisecond)
	timex.Add(10 * time.Second)
	wg.Wait()
	assert.Equal(t, expects, actual)
}

type MockSourceConnector struct {
	data       [][]byte
	topic      string
	subscribed atomic.Bool
}

func (m *MockSourceConnector) Provision(ctx api.StreamContext, configs map[string]any) error {
	datasource, ok := configs["datasource"]
	if !ok {
		return fmt.Errorf("datasource name cannot be empty")
	}
	m.topic = datasource.(string)
	return nil
}

func (m *MockSourceConnector) Connect(ctx api.StreamContext, _ api.StatusChangeHandler) error {
	if m.data == nil {
		return fmt.Errorf("data is nil")
	}
	return nil
}

func (m *MockSourceConnector) Close(ctx api.StreamContext) error {
	if m.subscribed.Load() {
		m.subscribed.Store(false)
		return nil
	} else {
		return fmt.Errorf("not subscribed")
	}
}

func (m *MockSourceConnector) Subscribe(ctx api.StreamContext, ingest api.BytesIngest, ingestError api.ErrorIngest) error {
	if m.subscribed.Load() {
		return fmt.Errorf("already subscribed")
	}
	m.subscribed.Store(true)
	go func() {
		if !m.subscribed.Load() {
			time.Sleep(100 * time.Millisecond)
		}
		for _, d := range m.data {
			ingest(ctx, d, map[string]any{"topic": "demo"}, timex.GetNow())
		}
		<-ctx.Done()
		fmt.Println("MockSourceConnector closed")
	}()
	return nil
}

type MockPullSource struct {
	set       bool
	pullTimes int
}

func (m *MockPullSource) Provision(ctx api.StreamContext, configs map[string]any) error {
	return nil
}

func (m *MockPullSource) Close(ctx api.StreamContext) error {
	return nil
}

func (m *MockPullSource) Connect(ctx api.StreamContext, _ api.StatusChangeHandler) error {
	return nil
}

func (m *MockPullSource) Pull(ctx api.StreamContext, trigger time.Time, ingest api.TupleIngest, ingestError api.ErrorIngest) {
	m.pullTimes++
	var mess any
	switch m.pullTimes % 5 {
	case 0:
		mess = map[string]any{
			"index": m.pullTimes,
		}
	case 1:
		mess = []map[string]any{
			{
				"index": m.pullTimes,
			},
		}
	case 2:
		mess = []byte{byte(m.pullTimes)}
	case 3:
		mess = &xsql.Tuple{
			Message: map[string]any{
				"index": m.pullTimes,
			},
		}
	case 4:
		mess = []*xsql.Tuple{
			{
				Message: map[string]any{
					"index": m.pullTimes,
				},
			},
		}
	}
	ingest(ctx, mess, map[string]any{"topic": "demo"}, trigger)
}

func (m *MockPullSource) SetEofIngest(eof api.EOFIngest) {
	m.set = true
}

type MockRewindSource struct {
	notify chan struct{}
	state  int
}

func (m *MockRewindSource) GetOffset() (any, error) {
	return m.state, nil
}

func (m *MockRewindSource) Rewind(offset any) error {
	m.state = offset.(int)
	return nil
}

func (m *MockRewindSource) ResetOffset(input map[string]any) error {
	return nil
}

func (m *MockRewindSource) Provision(ctx api.StreamContext, configs map[string]any) error {
	return nil
}

func (m *MockRewindSource) Close(ctx api.StreamContext) error {
	return nil
}

func (m *MockRewindSource) Connect(ctx api.StreamContext, _ api.StatusChangeHandler) error {
	return nil
}

func (m *MockRewindSource) Subscribe(ctx api.StreamContext, ingest api.TupleIngest, ingestError api.ErrorIngest) error {
	go func() {
		for range m.notify {
			ingest(ctx, map[string]any{
				"key": m.state,
			}, nil, time.Now())
			m.state++
		}
	}()
	return nil
}

func TestMockRewind(t *testing.T) {
	notify := make(chan struct{})
	m := &MockRewindSource{
		notify: notify,
	}
	var sc api.TupleSource = m
	ctx := mockContext.NewMockContext("rule1", "src1")
	// set rewind value
	ctx.PutState(OffsetKey, 10)
	errCh := make(chan error)
	scn, err := NewSourceNode(ctx, "mock_connector", sc, map[string]any{"datasource": "demo"}, &def.RuleOption{
		BufferLength: 1024,
		SendError:    true,
	})
	assert.NoError(t, err)
	result := make(chan any, 10)
	err = scn.AddOutput(result, "testResult")
	assert.NoError(t, err)
	scn.Open(ctx, errCh)
	notify <- struct{}{}
	data := <-result
	require.Equal(t, map[string]interface{}{"key": 10}, map[string]interface{}(data.(*xsql.Tuple).Message))
	notify <- struct{}{}
	data = <-result
	require.Equal(t, map[string]interface{}{"key": 11}, map[string]interface{}(data.(*xsql.Tuple).Message))
	v, _ := ctx.GetState(OffsetKey)
	require.Equal(t, 11, v)
}
