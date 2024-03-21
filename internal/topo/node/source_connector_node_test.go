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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	mockContext "github.com/lf-edge/ekuiper/pkg/mock/context"
)

func TestSCNLC(t *testing.T) {
	mc := conf.Clock.(*clock.Mock)
	expects := []any{
		&xsql.Tuple{
			Raw:       []byte("hello"),
			Metadata:  map[string]any{"topic": "demo"},
			Timestamp: mc.Now().UnixMilli(),
			Emitter:   "mock_connector",
		},
		&xsql.ErrorSourceTuple{
			Error: errors.New("expect api.RawTuple but got *api.DefaultSourceTuple"),
		},
		&xsql.Tuple{
			Raw:       []byte("world"),
			Metadata:  map[string]any{"topic": "demo"},
			Timestamp: mc.Now().UnixMilli(),
			Emitter:   "mock_connector",
		},
	}
	var sc api.SourceConnector = &MockSourceConnector{
		data: [][]byte{
			[]byte("hello"),
			nil,
			[]byte("world"),
		},
	}
	scn, err := NewSourceConnectorNode("mock_connector", sc, "demo", map[string]any{}, &api.RuleOption{
		BufferLength: 1024,
		SendError:    true,
	})
	assert.NoError(t, err)
	result := make(chan any, 10)
	err = scn.AddOutput(result, "testResult")
	assert.NoError(t, err)

	ctx := mockContext.NewMockContext("rule1", "src1")
	errCh := make(chan error)

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
	assert.Equal(t, expects, actual)
}

func TestNewError(t *testing.T) {
	var sc api.SourceConnector = &MockSourceConnector{
		data: [][]byte{
			[]byte("hello"),
			[]byte("world"),
		},
	}
	_, err := NewSourceConnectorNode("mock_connector", sc, "", map[string]any{}, &api.RuleOption{
		BufferLength: 1024,
		SendError:    true,
	})
	assert.Error(t, err)
	assert.Equal(t, "datasource name cannot be empty", err.Error())
}

func TestConnError(t *testing.T) {
	var sc api.SourceConnector = &MockSourceConnector{
		data: nil, // nil data to produce mock connect error
	}
	scn, err := NewSourceConnectorNode("mock_connector", sc, "demo2", map[string]any{}, &api.RuleOption{
		BufferLength: 1024,
		SendError:    true,
	})
	assert.NoError(t, err)

	ctx := mockContext.NewMockContext("rule1", "src1")
	assert.NoError(t, err)
	errCh := make(chan error)
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

func TestSubError(t *testing.T) {
	var sc api.SourceConnector = &MockSourceConnector{
		data: [][]byte{
			[]byte("hello"),
			[]byte("world"),
		},
	}
	scn, err := NewSourceConnectorNode("mock_connector", sc, "demo2", map[string]any{}, &api.RuleOption{
		BufferLength: 1024,
		SendError:    true,
	})
	assert.NoError(t, err)

	ctx := mockContext.NewMockContext("rule1", "src1")
	// subscribe once to produce error
	err = sc.Subscribe(ctx)
	assert.NoError(t, err)
	err = sc.Subscribe(ctx)
	assert.Error(t, err)
	errCh := make(chan error)
	var wg sync.WaitGroup
	wg.Add(1)
	var errResult error
	go func() {
		defer wg.Done()
		ticker := time.After(5 * time.Second)
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
	assert.Equal(t, "already subscribed", errResult.Error())
}

type MockSourceConnector struct {
	data       [][]byte
	topic      string
	subscribed atomic.Bool
}

func (m *MockSourceConnector) Connect(ctx api.StreamContext) error {
	if m.data == nil {
		return fmt.Errorf("data is nil")
	}
	return nil
}

func (m *MockSourceConnector) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	if !m.subscribed.Load() {
		time.Sleep(100 * time.Millisecond)
	}
	for _, d := range m.data {
		if d != nil {
			consumer <- api.NewDefaultRawTuple(d, map[string]any{
				"topic": m.topic,
			}, conf.GetNow())
		} else {
			consumer <- api.NewDefaultSourceTuple(nil, nil)
		}
	}
	<-ctx.Done()
	fmt.Println("MockSourceConnector closed")
}

func (m *MockSourceConnector) Configure(datasource string, _ map[string]interface{}) error {
	if datasource == "" {
		return fmt.Errorf("datasource name cannot be empty")
	}
	m.topic = datasource
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

func (m *MockSourceConnector) Subscribe(ctx api.StreamContext) error {
	if m.subscribed.Load() {
		return fmt.Errorf("already subscribed")
	}
	m.subscribed.Store(true)
	return nil
}
