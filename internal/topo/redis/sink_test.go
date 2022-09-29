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

package redis

import (
	econf "github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"reflect"
	"testing"
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
	var tests = []struct {
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
