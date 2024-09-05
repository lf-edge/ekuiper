// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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

package memory

import (
	gocontext "context"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func produceUpdatable(ctx api.StreamContext, topic string, data pubsub.MemTuple, rowkind string, keyval any) {
	pubsub.Produce(ctx, topic, &pubsub.UpdatableTuple{
		MemTuple: data,
		Rowkind:  rowkind,
		Keyval:   keyval,
	})
}

func TestLookupProvision(t *testing.T) {
	ctx := mockContext.NewMockContext("test", "Test")
	tests := []struct {
		name  string
		props map[string]any
		err   string
	}{
		{
			name: "invalid prop type",
			props: map[string]any{
				"key": 1,
			},
			err: "read properties map[key:1] fail with error: 1 error(s) decoding:\n\n* 'key' expected type 'string', got unconvertible type 'int', value: '1'",
		},
		{
			name: "missing topic",
			props: map[string]any{
				"key": "test",
			},
			err: "datasource(topic) is required",
		},
		{
			name: "wrong topic regex",
			props: map[string]any{
				"key":        "test",
				"datasource": "test/#/abc",
			},
			err: "invalid topic test/#/abc: # must at the last level",
		},
		{
			name: "missing key",
			props: map[string]any{
				"datasource": "test/#",
			},
			err: "key is required for lookup source",
		},
	}
	ls := &lookupsource{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ls.Provision(ctx, tt.props)
			assert.Error(t, err)
			assert.EqualError(t, err, tt.err)
		})
	}
}

func TestUpdateLookup(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "test")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	ls := GetLookupSource().(api.LookupSource)
	err := ls.Provision(ctx, map[string]interface{}{"datasource": "test", "key": "ff"})
	assert.NoError(t, err)
	err = ls.Connect(ctx, func(status string, message string) {
		// do nothing
	})
	assert.NoError(t, err)
	// wait for the source to be ready
	time.Sleep(100 * time.Millisecond)
	pubsub.Produce(ctx, "test", &xsql.Tuple{Message: map[string]any{"ff": "value1", "gg": "value2"}})
	produceUpdatable(ctx, "test", &xsql.Tuple{Message: map[string]any{"ff": "value1", "gg": "value2"}}, "delete", "value1")
	produceUpdatable(ctx, "test", &xsql.Tuple{Message: map[string]any{"ff": "value2", "gg": "value2"}}, "insert", "value2")
	pubsub.Produce(ctx, "test", &xsql.Tuple{Message: map[string]any{"ff": "value1", "gg": "value4"}})
	produceUpdatable(ctx, "test", &xsql.Tuple{Message: map[string]any{"ff": "value2", "gg": "value2"}}, "delete", "value2")
	pubsub.Produce(ctx, "test", &xsql.Tuple{Message: map[string]any{"ff": "value1", "gg": "value2"}})
	pubsub.Produce(ctx, "test", &xsql.Tuple{Message: map[string]any{"ff": "value2", "gg": "value2"}})
	// wait for table accumulation
	time.Sleep(100 * time.Millisecond)
	canctx, cancel := gocontext.WithCancel(gocontext.Background())
	defer cancel()
	go func() {
		for {
			select {
			case <-canctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
				pubsub.Produce(ctx, "test", &xsql.Tuple{Message: map[string]any{"ff": "value4", "gg": "value2"}})
			}
		}
	}()
	expected := []map[string]any{
		{"ff": "value1", "gg": "value2"},
	}
	result, err := ls.Lookup(ctx, []string{}, []string{"ff"}, []interface{}{"value1"})
	assert.NoError(t, err)
	assert.Equal(t, expected, result)
	err = ls.Close(ctx)
	if err != nil {
		t.Error(err)
		return
	}
}

func TestLookup(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "test2")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	ls := GetLookupSource().(api.LookupSource)
	err := ls.Provision(ctx, map[string]any{"datasource": "test2", "key": "gg"})
	assert.NoError(t, err)
	err = ls.Connect(ctx, func(status string, message string) {
		// do nothing
	})
	assert.NoError(t, err)
	// wait for the source to be ready
	time.Sleep(100 * time.Millisecond)
	pubsub.Produce(ctx, "test2", &xsql.Tuple{Message: map[string]any{"ff": "value1", "gg": "value2"}})
	pubsub.Produce(ctx, "test2", &xsql.Tuple{Message: map[string]any{"ff": "value2", "gg": "value3"}})
	pubsub.Produce(ctx, "test2", &xsql.Tuple{Message: map[string]any{"ff": "value1", "gg": "value4"}})
	// wait for table accumulation
	time.Sleep(100 * time.Millisecond)
	canctx, cancel := gocontext.WithCancel(gocontext.Background())
	defer cancel()
	go func() {
		for {
			select {
			case <-canctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
				pubsub.Produce(ctx, "test", &xsql.Tuple{Message: map[string]any{"ff": "value4", "gg": "value5"}})
			}
		}
	}()
	result, _ := ls.Lookup(ctx, []string{}, []string{"ff"}, []any{"value1"})
	expected := []map[string]any{
		{"ff": "value1", "gg": "value2"},
		{"ff": "value1", "gg": "value4"},
	}
	if len(result) != 2 {
		t.Errorf("expect %v but got %v", expected, result)
	} else {
		if result[0]["gg"] != "value2" {
			result[0], result[1] = result[1], result[0]
		}
	}
	assert.Equal(t, expected, result)
	err = ls.Close(ctx)
	assert.NoError(t, err)
}
