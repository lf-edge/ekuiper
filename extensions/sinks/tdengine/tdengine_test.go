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

package main

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/testx"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/state"
)

func TestConfig(t *testing.T) {
	var tests = []struct {
		conf     map[string]interface{}
		expected *taosConfig
		error    string
	}{
		{ //0
			conf: map[string]interface{}{
				"host":        "e0d9d8089bef",
				"port":        6030,
				"user":        "root",
				"password":    "taosdata",
				"database":    "db",
				"table":       "t",
				"tsfieldname": "ts",
			},
			expected: &taosConfig{
				ProvideTs:   false,
				Host:        "e0d9d8089bef",
				Port:        6030,
				User:        "root",
				Password:    "taosdata",
				Database:    "db",
				Table:       "t",
				TsFieldName: "ts",
				Fields:      nil,
			},
		},
		{ //1
			conf: map[string]interface{}{
				"ip":          "e0d9d8089bef",
				"port":        6030,
				"user":        "root1",
				"password":    "taosdata1",
				"database":    "db",
				"table":       "t",
				"provideTs":   true,
				"tsfieldname": "ts",
			},
			expected: &taosConfig{
				ProvideTs:   true,
				Ip:          "e0d9d8089bef",
				Host:        "e0d9d8089bef",
				Port:        6030,
				User:        "root1",
				Password:    "taosdata1",
				Database:    "db",
				Table:       "t",
				TsFieldName: "ts",
				Fields:      nil,
			},
		},
		{ //2
			conf: map[string]interface{}{
				"port":        6030,
				"database":    "dab",
				"table":       "tt",
				"tsfieldname": "tst",
				"fields":      []string{"f1", "f2"},
				"sTable":      "s",
				"tagFields":   []string{"a", "b"},
			},
			expected: &taosConfig{
				ProvideTs:   false,
				Ip:          "",
				Host:        "localhost",
				Port:        6030,
				User:        "root",
				Password:    "taosdata",
				Database:    "dab",
				Table:       "tt",
				TsFieldName: "tst",
				Fields:      []string{"f1", "f2"},
				STable:      "s",
				TagFields:   []string{"a", "b"},
			},
		},
		{ //3
			conf: map[string]interface{}{
				"port":     6030,
				"database": "dab",
				"table":    "t",
				"fields":   []string{"f1", "f2"},
			},
			error: "property TsFieldName is required",
		},
		{ //4
			conf: map[string]interface{}{
				"port":        6030,
				"database":    "dab",
				"table":       "tt",
				"tsfieldname": "tst",
				"fields":      []string{"f1", "f2"},
				"sTable":      "s",
			},
			error: "property tagFields is required when sTable is set",
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, test := range tests {
		tdsink := &taosSink{}
		err := tdsink.Configure(test.conf)
		if !reflect.DeepEqual(test.error, testx.Errstring(err)) {
			t.Errorf("%d: error mismatch:\n  exp=%s\n  got=%s\n\n", i, test.error, err)
		} else if test.error == "" && !reflect.DeepEqual(test.expected, tdsink.conf) {
			t.Errorf("%d\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, test.expected, tdsink.conf)
		}
	}
}

func TestBuildSql(t *testing.T) {
	var tests = []struct {
		conf     *taosConfig
		data     map[string]interface{}
		expected string
		error    string
	}{
		{
			conf: &taosConfig{
				ProvideTs:   false,
				Host:        "e0d9d8089bef",
				Port:        6030,
				User:        "root",
				Password:    "taosdata",
				Database:    "db",
				Table:       "t",
				TsFieldName: "ts",
				Fields:      nil,
			},
			data: map[string]interface{}{
				"f1": "v1",
			},
			expected: `t (ts,f1) values (now,"v1")`,
		},
		{
			conf: &taosConfig{
				ProvideTs:   true,
				Ip:          "e0d9d8089bef",
				Host:        "e0d9d8089bef",
				Port:        6030,
				User:        "root1",
				Password:    "taosdata1",
				Database:    "db",
				Table:       "t",
				TsFieldName: "ts",
				Fields:      nil,
			},
			data: map[string]interface{}{
				"ts": 1.2345678e+06,
				"f2": 65,
			},
			expected: `t (ts,f2) values (12345678,65)`,
		},
		{
			conf: &taosConfig{
				ProvideTs:   true,
				Ip:          "e0d9d8089bef",
				Host:        "e0d9d8089bef",
				Port:        6030,
				User:        "root1",
				Password:    "taosdata1",
				Database:    "db",
				Table:       "t",
				TsFieldName: "ts",
				Fields:      nil,
			},
			data: map[string]interface{}{
				"ts": 12345678,
				"f2": 65,
			},
			expected: `t (ts,f2) values (12345678,65)`,
		},
		{
			conf: &taosConfig{
				ProvideTs:   false,
				Ip:          "",
				Host:        "localhost",
				Port:        6030,
				User:        "root",
				Password:    "taosdata",
				Database:    "dab",
				Table:       "{{.table}}",
				TsFieldName: "tst",
				Fields:      []string{"f1", "f2"},
				STable:      "s",
				TagFields:   []string{"a", "b"},
			},
			data: map[string]interface{}{
				"table": "t1",
				"ts":    12345678,
				"f2":    65,
				"f1":    12.3,
				"a":     "a1",
				"b":     2,
			},
			expected: `t1 (tst,f1,f2) using s tags ("a1",2) values (now,12.3,65)`,
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "mockRule0")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger).WithMeta("testTD", "op1", &state.MemoryStore{})
	for i, test := range tests {
		sql, err := test.conf.buildSql(ctx, test.data)
		if !reflect.DeepEqual(test.error, testx.Errstring(err)) {
			t.Errorf("%d: error mismatch:\n  exp=%s\n  got=%s\n\n", i, test.error, err)
		} else if test.error == "" && !reflect.DeepEqual(test.expected, sql) {
			t.Errorf("%d\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, test.expected, sql)
		}
	}
}
