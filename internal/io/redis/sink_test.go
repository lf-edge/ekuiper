// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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
	"reflect"
	"testing"

	econf "github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

func TestSink(t *testing.T) {
	s := &RedisSink{}
	err := s.Configure(map[string]interface{}{
		"addr": addr,
		"key":  "test",
	})
	if err != nil {
		t.Error(err)
		return
	}
	contextLogger := econf.Log.WithField("rule", "test")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	err = s.Open(ctx)
	if err != nil {
		t.Error(err)
		return
	}
	tests := []struct {
		c map[string]interface{}
		d interface{}
		k string
		v interface{}
	}{
		{
			c: map[string]interface{}{"key": "1"},
			d: map[string]interface{}{"id": 1, "name": "John", "address": 34, "mobile": "334433"},
			k: "1",
			v: `{"address":34,"id":1,"mobile":"334433","name":"John"}`,
		},
		{
			c: map[string]interface{}{"field": "id"},
			d: map[string]interface{}{"id": 2, "name": "Susan", "address": 34, "mobile": "334433"},
			k: "2",
			v: `{"address":34,"id":2,"mobile":"334433","name":"Susan"}`,
		},
		{
			c: map[string]interface{}{"field": "name", "datatype": "list"},
			d: map[string]interface{}{"id": 3, "name": "Susan"},
			k: "Susan",
			v: `{"id":3,"name":"Susan"}`,
		},
		{
			c: map[string]interface{}{"field": "id", "datatype": "list"},
			d: []map[string]interface{}{
				{"id": 4, "name": "Susan"},
				{"id": 4, "name": "Bob"},
				{"id": 4, "name": "John"},
			},
			k: "4",
			v: `{"id":4,"name":"John"}`,
		},
		{
			c: map[string]interface{}{"field": "id", "datatype": "string"},
			d: []map[string]interface{}{
				{"id": 25, "name": "Susan"},
				{"id": 25, "name": "Bob"},
				{"id": 25, "name": "John"},
			},
			k: "25",
			v: `{"id":25,"name":"John"}`,
		},
	}
	for i, tt := range tests {
		cast.MapToStruct(tt.c, s.c)
		err = s.Collect(ctx, tt.d)
		if err != nil {
			t.Error(err)
			return
		}
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
		if err != nil {
			t.Errorf("case %d err %v", i, err)
			return
		}
		if !reflect.DeepEqual(r, tt.v) {
			t.Errorf("case %d expect %v, but got %v", i, tt.v, r)
		}
	}
}

func TestSinkMultipleFields(t *testing.T) {
	s := &RedisSink{}
	err := s.Configure(map[string]interface{}{
		"addr": addr,
		"key":  "test",
	})
	if err != nil {
		t.Error(err)
		return
	}
	contextLogger := econf.Log.WithField("rule", "test")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	err = s.Open(ctx)
	if err != nil {
		t.Error(err)
		return
	}
	tests := []struct {
		c      map[string]interface{}
		d      interface{}
		kvPair map[string]interface{}
	}{
		{
			c:      map[string]interface{}{"keyType": "multiple"},
			d:      map[string]interface{}{"id": 1, "name": "John", "address": 34, "mobile": "334433"},
			kvPair: map[string]interface{}{"id": "1", "name": "John", "address": "34", "mobile": "334433"},
		},
		{
			c: map[string]interface{}{"keyType": "multiple", "datatype": "string"},
			d: []map[string]interface{}{
				{"id": 24, "name": "Susan"},
				{"id": 25, "name": "Bob"},
				{"id": 26, "name": "John"},
			},
			kvPair: map[string]interface{}{"id": "26", "name": "John"},
		},
		{
			c: map[string]interface{}{"datatype": "list", "keyType": "multiple"},
			d: map[string]interface{}{
				"listId": 4, "listName": "Susan",
			},
			kvPair: map[string]interface{}{"listId": "4", "listName": "Susan"},
		},
		{
			c: map[string]interface{}{"datatype": "list", "keyType": "multiple"},
			d: []map[string]interface{}{
				{"listId": 4, "listName": "Susan"},
				{"listId": 5, "listName": "Bob"},
				{"listId": 6, "listName": "John"},
			},
			kvPair: map[string]interface{}{"listId": "6", "listName": "John"},
		},
	}
	for i, tt := range tests {
		cast.MapToStruct(tt.c, s.c)
		err = s.Collect(ctx, tt.d)
		if err != nil {
			t.Error(err)
			return
		}
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
			if err != nil {
				t.Errorf("case %d err %v", i, err)
				return
			}
			if !reflect.DeepEqual(r, v) {
				t.Errorf("case %d expect %v, but got %v", i, v, r)
			}
		}
	}
}

func TestUpdateString(t *testing.T) {
	s := &RedisSink{}
	err := s.Configure(map[string]interface{}{
		"addr":         addr,
		"field":        "id",
		"rowkindField": "action",
	})
	if err != nil {
		t.Error(err)
		return
	}
	contextLogger := econf.Log.WithField("rule", "test")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	err = s.Open(ctx)
	if err != nil {
		t.Error(err)
		return
	}
	tests := []struct {
		d interface{}
		k string
		v interface{}
	}{
		{
			d: map[string]interface{}{ // add without action
				"id": "testUpdate1", "name": "Susan",
			},
			k: "testUpdate1",
			v: `{"id":"testUpdate1","name":"Susan"}`,
		},
		{
			d: map[string]interface{}{ // update with action
				"action": "update", "id": "testUpdate1", "name": "John",
			},
			k: "testUpdate1",
			v: `{"action":"update","id":"testUpdate1","name":"John"}`,
		},
		{
			d: map[string]interface{}{ // delete
				"action": "delete", "id": "testUpdate1",
			},
			k: "testUpdate1",
			v: ``,
		},
		{
			d: []map[string]interface{}{ // multiple actions
				{"action": "delete", "id": "testUpdate1"},
				{"action": "insert", "id": "testUpdate1", "name": "Susan"},
			},
			k: "testUpdate1",
			v: `{"action":"insert","id":"testUpdate1","name":"Susan"}`,
		},
	}
	for i, tt := range tests {
		err = s.Collect(ctx, tt.d)
		if err != nil {
			t.Error(err)
			return
		}
		r, err := mr.Get(tt.k)
		if tt.v == "" {
			if err == nil || err.Error() != "ERR no such key" {
				t.Errorf("case %d err %v", i, err)
				return
			}
		} else {
			if err != nil {
				t.Errorf("case %d err %v", i, err)
				return
			}
			if !reflect.DeepEqual(r, tt.v) {
				t.Errorf("case %d expect %v, but got %v", i, tt.v, r)
			}
		}
	}
}

func TestUpdateList(t *testing.T) {
	s := &RedisSink{}
	err := s.Configure(map[string]interface{}{
		"addr":         addr,
		"field":        "id",
		"datatype":     "list",
		"rowkindField": "action",
	})
	if err != nil {
		t.Error(err)
		return
	}
	contextLogger := econf.Log.WithField("rule", "test")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	err = s.Open(ctx)
	if err != nil {
		t.Error(err)
		return
	}
	tests := []struct {
		d interface{}
		k string
		v []string
	}{
		{
			d: map[string]interface{}{ // add without action
				"id": "testUpdateList", "name": "Susan",
			},
			k: "testUpdateList",
			v: []string{`{"id":"testUpdateList","name":"Susan"}`},
		},
		{
			d: map[string]interface{}{ // update with action
				"action": "update", "id": "testUpdateList", "name": "John",
			},
			k: "testUpdateList",
			v: []string{`{"action":"update","id":"testUpdateList","name":"John"}`, `{"id":"testUpdateList","name":"Susan"}`},
		},
		{
			d: map[string]interface{}{ // delete
				"action": "delete", "id": "testUpdateList",
			},
			k: "testUpdateList",
			v: []string{`{"id":"testUpdateList","name":"Susan"}`},
		},
		{
			d: []map[string]interface{}{ // multiple actions
				{"action": "delete", "id": "testUpdateList"},
				{"action": "insert", "id": "testUpdateList", "name": "Susan"},
			},
			k: "testUpdateList",
			v: []string{`{"action":"insert","id":"testUpdateList","name":"Susan"}`},
		},
		{
			d: map[string]interface{}{ // delete
				"action": "delete", "id": "testUpdateList",
			},
			k: "testUpdateList",
			v: nil,
		},
	}
	for i, tt := range tests {
		err = s.Collect(ctx, tt.d)
		if err != nil {
			t.Error(err)
			return
		}
		r, err := mr.List(tt.k)
		if tt.v == nil {
			if err == nil || err.Error() != "ERR no such key" {
				t.Errorf("case %d err %v", i, err)
				return
			}
		} else {
			if err != nil {
				t.Errorf("case %d err %v", i, err)
				return
			}
			if !reflect.DeepEqual(r, tt.v) {
				t.Errorf("case %d expect %v, but got %v", i, tt.v, r)
			}
		}
	}
}

func TestRedisSink_Configure(t *testing.T) {
	type args struct {
		props map[string]interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "missing key and field and default keyType is single",
			args: args{map[string]interface{}{
				"addr":     addr,
				"datatype": "list",
			}},
			wantErr: true,
		},
		{
			name: "missing key and field and keyType is multiple",
			args: args{map[string]interface{}{
				"addr":     addr,
				"datatype": "list",
				"keyType":  "multiple",
			}},
			wantErr: false,
		},
		{
			name: "key type do not support",
			args: args{map[string]interface{}{
				"addr":     addr,
				"datatype": "list",
				"keyType":  "ttt",
			}},
			wantErr: true,
		},
		{
			name: "data type do not support",
			args: args{map[string]interface{}{
				"addr":     addr,
				"datatype": "stream",
				"keyType":  "multiple",
			}},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RedisSink{
				c: nil,
			}
			if err := r.Configure(tt.args.props); (err != nil) != tt.wantErr {
				t.Errorf("Configure() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
