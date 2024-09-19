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

package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestSink(t *testing.T) {
	s := &RedisSink{}
	ctx := mockContext.NewMockContext("testSink", "op")
	err := s.Provision(ctx, map[string]any{
		"addr": addr,
		"key":  "test",
	})
	if err != nil {
		t.Error(err)
		return
	}
	err = s.Connect(ctx, func(status string, message string) {
		// do nothing
	})
	assert.NoError(t, err)
	tests := []struct {
		n string
		c map[string]any
		d any
		k string
		v any
	}{
		{
			n: "case1",
			c: map[string]any{"key": "1"},
			d: map[string]any{"id": 1, "name": "John", "address": 34, "mobile": "334433"},
			k: "1",
			v: `{"address":34,"id":1,"mobile":"334433","name":"John"}`,
		},
		{
			n: "case2",
			c: map[string]any{"field": "id"},
			d: map[string]any{"id": 2, "name": "Susan", "address": 34, "mobile": "334433"},
			k: "2",
			v: `{"address":34,"id":2,"mobile":"334433","name":"Susan"}`,
		},
		{
			n: "case3",
			c: map[string]any{"field": "name", "datatype": "list"},
			d: map[string]any{"id": 3, "name": "Susan"},
			k: "Susan",
			v: `{"id":3,"name":"Susan"}`,
		},
		{
			n: "case4",
			c: map[string]any{"field": "id", "datatype": "list"},
			d: []map[string]any{
				{"id": 4, "name": "Susan"},
				{"id": 4, "name": "Bob"},
				{"id": 4, "name": "John"},
			},
			k: "4",
			v: `{"id":4,"name":"John"}`,
		},
		{
			n: "case5",
			c: map[string]any{"field": "id", "datatype": "string"},
			d: []map[string]any{
				{"id": 25, "name": "Susan"},
				{"id": 25, "name": "Bob"},
				{"id": 25, "name": "John"},
			},
			k: "25",
			v: `{"id":25,"name":"John"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.n, func(t *testing.T) {
			err = cast.MapToStruct(tt.c, s.c)
			assert.NoError(t, err)
			switch dd := tt.d.(type) {
			case map[string]any:
				err = s.Collect(ctx, &xsql.Tuple{
					Message: dd,
				})
			case []map[string]any:
				result := &xsql.WindowTuples{
					Content: make([]xsql.Row, 0, len(dd)),
				}
				for _, m := range dd {
					result.Content = append(result.Content, &xsql.Tuple{
						Message: m,
					})
				}
				err = s.CollectList(ctx, result)
			}
			assert.NoError(t, err)
			var (
				r   string
				err error
			)
			switch tt.c["datatype"] {
			case "list":
				r, err = mr.Lpop(tt.k)
			default:
				r, err = mr.Get(tt.k)
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.v, r)
		})
	}
}

func TestSinkMultipleFields(t *testing.T) {
	s := &RedisSink{}
	ctx := mockContext.NewMockContext("testSink", "op")
	err := s.Provision(ctx, map[string]any{
		"addr": addr,
		"key":  "test",
	})
	assert.NoError(t, err)
	err = s.Connect(ctx, func(status string, message string) {
		// do nothing
	})
	assert.NoError(t, err)
	tests := []struct {
		n      string
		c      map[string]any
		d      any
		kvPair map[string]any
	}{
		{
			n:      "case1",
			c:      map[string]any{"keyType": "multiple"},
			d:      map[string]any{"id": 1, "name": "John", "address": 34, "mobile": "334433"},
			kvPair: map[string]any{"id": "1", "name": "John", "address": "34", "mobile": "334433"},
		},
		{
			n: "case2",
			c: map[string]any{"keyType": "multiple", "datatype": "string"},
			d: []map[string]any{
				{"id": 24, "name": "Susan"},
				{"id": 25, "name": "Bob"},
				{"id": 26, "name": "John"},
			},
			kvPair: map[string]any{"id": "26", "name": "John"},
		},
		{
			n: "case3",
			c: map[string]any{"datatype": "list", "keyType": "multiple"},
			d: map[string]any{
				"listId": 4, "listName": "Susan",
			},
			kvPair: map[string]any{"listId": "4", "listName": "Susan"},
		},
		{
			n: "case4",
			c: map[string]any{"datatype": "list", "keyType": "multiple"},
			d: []map[string]any{
				{"listId": 4, "listName": "Susan"},
				{"listId": 5, "listName": "Bob"},
				{"listId": 6, "listName": "John"},
			},
			kvPair: map[string]any{"listId": "6", "listName": "John"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.n, func(t *testing.T) {
			err = cast.MapToStruct(tt.c, s.c)
			assert.NoError(t, err)
			switch dd := tt.d.(type) {
			case map[string]any:
				err = s.Collect(ctx, &xsql.Tuple{
					Message: dd,
				})
			case []map[string]any:
				result := &xsql.WindowTuples{
					Content: make([]xsql.Row, 0, len(dd)),
				}
				for _, m := range dd {
					result.Content = append(result.Content, &xsql.Tuple{
						Message: m,
					})
				}
				err = s.CollectList(ctx, result)
			}
			assert.NoError(t, err)
			var (
				r   string
				err error
			)
			for k, v := range tt.kvPair {
				switch tt.c["datatype"] {
				case "list":
					r, err = mr.Lpop(k)
				default:
					r, err = mr.Get(k)
				}
				assert.NoError(t, err)
				assert.Equal(t, v, r)
			}
		})
	}
}

func TestUpdateString(t *testing.T) {
	s := &RedisSink{}
	ctx := mockContext.NewMockContext("testSink", "op")
	err := s.Provision(ctx, map[string]any{
		"addr":         addr,
		"field":        "id",
		"rowkindField": "action",
	})
	assert.NoError(t, err)
	err = s.Connect(ctx, func(status string, message string) {
		// do nothing
	})
	assert.NoError(t, err)
	tests := []struct {
		n string
		d any
		k string
		v any
	}{
		{
			n: "case1",
			d: map[string]any{ // add without action
				"id": "testUpdate1", "name": "Susan",
			},
			k: "testUpdate1",
			v: `{"id":"testUpdate1","name":"Susan"}`,
		},
		{
			n: "case2",
			d: map[string]any{ // update with action
				"action": "update", "id": "testUpdate1", "name": "John",
			},
			k: "testUpdate1",
			v: `{"action":"update","id":"testUpdate1","name":"John"}`,
		},
		{
			n: "case3",
			d: map[string]any{ // delete
				"action": "delete", "id": "testUpdate1",
			},
			k: "testUpdate1",
			v: ``,
		},
		{
			n: "case4",
			d: []map[string]any{ // multiple actions
				{"action": "delete", "id": "testUpdate1"},
				{"action": "insert", "id": "testUpdate1", "name": "Susan"},
			},
			k: "testUpdate1",
			v: `{"action":"insert","id":"testUpdate1","name":"Susan"}`,
		},
	}
	for _, tt := range tests {
		switch dd := tt.d.(type) {
		case map[string]any:
			err = s.Collect(ctx, &xsql.Tuple{
				Message: dd,
			})
		case []map[string]any:
			result := &xsql.WindowTuples{
				Content: make([]xsql.Row, 0, len(dd)),
			}
			for _, m := range dd {
				result.Content = append(result.Content, &xsql.Tuple{
					Message: m,
				})
			}
			err = s.CollectList(ctx, result)
		}
		assert.NoError(t, err)
		r, err := mr.Get(tt.k)
		if tt.v == "" {
			assert.EqualError(t, err, "ERR no such key")
		} else {
			assert.NoError(t, err)
			assert.Equal(t, tt.v, r)
		}
	}
}

func TestUpdateList(t *testing.T) {
	s := &RedisSink{}
	ctx := mockContext.NewMockContext("testSink", "op")
	err := s.Provision(ctx, map[string]any{
		"addr":         addr,
		"field":        "id",
		"datatype":     "list",
		"rowkindField": "action",
	})
	assert.NoError(t, err)
	err = s.Connect(ctx, func(status string, message string) {
		// do nothing
	})
	assert.NoError(t, err)
	tests := []struct {
		n string
		d any
		k string
		v []string
	}{
		{
			n: "case1",
			d: map[string]any{ // add without action
				"id": "testUpdateList", "name": "Susan",
			},
			k: "testUpdateList",
			v: []string{`{"id":"testUpdateList","name":"Susan"}`},
		},
		{
			n: "case2",
			d: map[string]any{ // update with action
				"action": "update", "id": "testUpdateList", "name": "John",
			},
			k: "testUpdateList",
			v: []string{`{"action":"update","id":"testUpdateList","name":"John"}`, `{"id":"testUpdateList","name":"Susan"}`},
		},
		{
			n: "case3",
			d: map[string]any{ // delete
				"action": "delete", "id": "testUpdateList",
			},
			k: "testUpdateList",
			v: []string{`{"id":"testUpdateList","name":"Susan"}`},
		},
		{
			n: "case4",
			d: []map[string]any{ // multiple actions
				{"action": "delete", "id": "testUpdateList"},
				{"action": "insert", "id": "testUpdateList", "name": "Susan"},
			},
			k: "testUpdateList",
			v: []string{`{"action":"insert","id":"testUpdateList","name":"Susan"}`},
		},
		{
			n: "case5",
			d: map[string]any{ // delete
				"action": "delete", "id": "testUpdateList",
			},
			k: "testUpdateList",
			v: nil,
		},
	}
	for _, tt := range tests {
		switch dd := tt.d.(type) {
		case map[string]any:
			err = s.Collect(ctx, &xsql.Tuple{
				Message: dd,
			})
		case []map[string]any:
			result := &xsql.WindowTuples{
				Content: make([]xsql.Row, 0, len(dd)),
			}
			for _, m := range dd {
				result.Content = append(result.Content, &xsql.Tuple{
					Message: m,
				})
			}
			err = s.CollectList(ctx, result)
		}
		assert.NoError(t, err)
		r, err := mr.List(tt.k)
		if tt.v == nil {
			assert.EqualError(t, err, "ERR no such key")
		} else {
			assert.NoError(t, err)
			assert.Equal(t, tt.v, r)
		}
	}
}

func TestRedisSink_Configure(t *testing.T) {
	type args struct {
		props map[string]any
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "missing key and field and default keyType is single",
			args: args{map[string]any{
				"addr":     addr,
				"datatype": "list",
			}},
			wantErr: true,
		},
		{
			name: "missing key and field and keyType is multiple",
			args: args{map[string]any{
				"addr":     addr,
				"datatype": "list",
				"keyType":  "multiple",
			}},
			wantErr: false,
		},
		{
			name: "key type do not support",
			args: args{map[string]any{
				"addr":     addr,
				"datatype": "list",
				"keyType":  "ttt",
			}},
			wantErr: true,
		},
		{
			name: "data type do not support",
			args: args{map[string]any{
				"addr":     addr,
				"datatype": "stream",
				"keyType":  "multiple",
			}},
			wantErr: true,
		},
	}
	ctx := mockContext.NewMockContext("TestConfigure", "op")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RedisSink{
				c: nil,
			}
			if err := r.Provision(ctx, tt.args.props); (err != nil) != tt.wantErr {
				t.Errorf("Configure() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRedisSink(t *testing.T) {
	s := &RedisSink{}
	err := s.Validate(map[string]any{"db": 199})
	require.Error(t, err)
	require.Equal(t, "redisSink db should be in range 0-15", err.Error())
}
