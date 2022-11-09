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

package rule

import (
	"errors"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/processor"
	"github.com/lf-edge/ekuiper/internal/testx"
	"github.com/lf-edge/ekuiper/pkg/api"
	"reflect"
	"testing"
	"time"
)

var defaultOption = &api.RuleOption{
	IsEventTime:        false,
	LateTol:            1000,
	Concurrency:        1,
	BufferLength:       1024,
	SendMetaToSink:     false,
	SendError:          true,
	Qos:                api.AtMostOnce,
	CheckpointInterval: 300000,
	Restart: &api.RestartStrategy{
		Attempts:     0,
		Delay:        1000,
		Multiplier:   2,
		MaxDelay:     30000,
		JitterFactor: 0.1,
	},
}

func init() {
	testx.InitEnv()
}

func TestCreate(t *testing.T) {
	sp := processor.NewStreamProcessor()
	sp.ExecStmt(`CREATE STREAM demo () WITH (DATASOURCE="users", FORMAT="JSON")`)
	defer sp.ExecStmt(`DROP STREAM demo`)
	var tests = []struct {
		r *api.Rule
		e error
	}{
		{
			r: &api.Rule{
				Triggered: false,
				Id:        "test",
				Sql:       "SELECT ts FROM demo",
				Actions: []map[string]interface{}{
					{
						"log": map[string]interface{}{},
					},
				},
				Options: defaultOption,
			},
			e: nil,
		}, {
			r: &api.Rule{
				Triggered: false,
				Id:        "test",
				Sql:       "SELECT timestamp FROM demo",
				Actions: []map[string]interface{}{
					{
						"log": map[string]interface{}{},
					},
				},
				Options: defaultOption,
			},
			e: errors.New("Parse SQL SELECT timestamp FROM demo error: found \"TIMESTAMP\", expected expression.."),
		},
		{
			r: &api.Rule{
				Triggered: false,
				Id:        "test",
				Sql:       "SELECT * FROM demo1",
				Actions: []map[string]interface{}{
					{
						"log": map[string]interface{}{},
					},
				},
				Options: defaultOption,
			},
			e: errors.New("fail to get stream demo1, please check if stream is created"),
		},
	}
	for i, tt := range tests {
		_, err := NewRuleState(tt.r)
		if !reflect.DeepEqual(err, tt.e) {
			t.Errorf("%d.\n\nerror mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.e, err)
		}
	}
}

func TestUpdate(t *testing.T) {
	sp := processor.NewStreamProcessor()
	sp.ExecStmt(`CREATE STREAM demo () WITH (DATASOURCE="users", FORMAT="JSON")`)
	defer sp.ExecStmt(`DROP STREAM demo`)
	rs, err := NewRuleState(&api.Rule{
		Triggered: false,
		Id:        "test",
		Sql:       "SELECT ts FROM demo",
		Actions: []map[string]interface{}{
			{
				"log": map[string]interface{}{},
			},
		},
		Options: defaultOption,
	})
	if err != nil {
		t.Error(err)
		return
	}
	defer rs.Close()
	err = rs.Start()
	if err != nil {
		t.Error(err)
		return
	}
	var tests = []struct {
		r *api.Rule
		e error
	}{
		{
			r: &api.Rule{
				Triggered: false,
				Id:        "test",
				Sql:       "SELECT timestamp FROM demo",
				Actions: []map[string]interface{}{
					{
						"log": map[string]interface{}{},
					},
				},
				Options: defaultOption,
			},
			e: errors.New("Parse SQL SELECT timestamp FROM demo error: found \"TIMESTAMP\", expected expression.."),
		},
		{
			r: &api.Rule{
				Triggered: false,
				Id:        "test",
				Sql:       "SELECT * FROM demo1",
				Actions: []map[string]interface{}{
					{
						"log": map[string]interface{}{},
					},
				},
				Options: defaultOption,
			},
			e: errors.New("fail to get stream demo1, please check if stream is created"),
		},
		{
			r: &api.Rule{
				Triggered: false,
				Id:        "test",
				Sql:       "SELECT * FROM demo",
				Actions: []map[string]interface{}{
					{
						"log": map[string]interface{}{},
					},
				},
				Options: defaultOption,
			},
			e: nil,
		},
	}
	for i, tt := range tests {
		err = rs.UpdateTopo(tt.r)
		if !reflect.DeepEqual(err, tt.e) {
			t.Errorf("%d.\n\nerror mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.e, err)
		}
	}
}

func TestMultipleAccess(t *testing.T) {
	sp := processor.NewStreamProcessor()
	sp.ExecStmt(`CREATE STREAM demo () WITH (DATASOURCE="users", FORMAT="JSON")`)
	defer sp.ExecStmt(`DROP STREAM demo`)
	rs, err := NewRuleState(&api.Rule{
		Triggered: false,
		Id:        "test",
		Sql:       "SELECT ts FROM demo",
		Actions: []map[string]interface{}{
			{
				"log": map[string]interface{}{},
			},
		},
		Options: defaultOption,
	})
	if err != nil {
		t.Error(err)
		return
	}
	defer rs.Close()
	err = rs.Start()
	if err != nil {
		t.Error(err)
		return
	}
	for i := 0; i < 10; i++ {
		if i%3 == 0 {
			go func(i int) {
				rs.Stop()
				fmt.Printf("%d:%d\n", i, rs.triggered)
			}(i)
		} else {
			go func(i int) {
				rs.Start()
				fmt.Printf("%d:%d\n", i, rs.triggered)
			}(i)
		}
	}
	time.Sleep(1 * time.Millisecond)
	rs.Start()
	time.Sleep(10 * time.Millisecond)
	if rs.triggered != 1 {
		t.Errorf("triggered mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", 1, rs.triggered)
	}
}
