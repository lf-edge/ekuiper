// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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

package tdengine3

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/testx"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestConfig(t *testing.T) {
	tests := []struct {
		name     string
		conf     map[string]interface{}
		expected TaosConfig
		error    string
	}{
		{
			name: "default props test",
			conf: map[string]interface{}{
				"database":    "power",
				"table":       "table",
				"tsFieldName": "ts",
			},
			expected: TaosConfig{
				Host:        "localhost",
				Port:        6041,
				User:        "root",
				Password:    "taosdata",
				Database:    "power",
				Table:       "table",
				TsFieldName: "ts",
			},
		},
		{
			name: "fixed table test",
			conf: map[string]interface{}{
				"host":        "192.168.1.1",
				"port":        6042,
				"user":        "test",
				"password":    "pass",
				"database":    "power",
				"table":       "table",
				"tsFieldName": "ts",
			},
			expected: TaosConfig{
				Host:        "192.168.1.1",
				Port:        6042,
				User:        "test",
				Password:    "pass",
				Database:    "power",
				Table:       "table",
				TsFieldName: "ts",
			},
		},
		{
			name: "no database error",
			conf: map[string]interface{}{
				"table":  "t",
				"fields": []string{"f1", "f2"},
			},
			error: "property database is required",
		},
		{
			name: "no table error",
			conf: map[string]interface{}{
				"database": "power",
				"fields":   []string{"f1", "f2"},
			},
			error: "property table is required",
		},
		{
			name: "no TsFieldName error",
			conf: map[string]interface{}{
				"host":     "localhost",
				"port":     6041,
				"database": "db",
				"table":    "t",
				"fields":   []string{"f1", "f2"},
			},
			error: "property TsFieldName is required",
		},
		{
			name: "no tagFields error",
			conf: map[string]interface{}{
				"host":        "localhost",
				"port":        6041,
				"database":    "db",
				"table":       "t",
				"tsFieldName": "ts",
				"fields":      []string{"f1", "f2"},
				"sTable":      "s",
			},
			error: "property tagFields is required when sTable is set",
		},
	}
	ctx := mockContext.NewMockContext("testconfig", "op")
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ifsink := tdengineSink3{}
			err := ifsink.Provision(ctx, test.conf)
			if test.error == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Equal(t, test.error, err.Error())
				return
			}
			assert.Equal(t, test.expected, *ifsink.cfg)
		})
	}
}

func TestBuildSql(t *testing.T) {
	tests := []struct {
		conf     *TaosConfig
		data     testx.MockTuple
		expected string
		error    string
	}{
		{
			conf: &TaosConfig{
				ProvideTs:   false,
				Database:    "db",
				Table:       "t",
				TsFieldName: "ts",
			},
			data:  testx.MockTuple{},
			error: "data is empty",
		},
		{
			conf: &TaosConfig{
				ProvideTs:   false,
				Database:    "db",
				Table:       "t",
				TsFieldName: "ts",
			},
			data: testx.MockTuple{
				Map: map[string]any{
					"f1": "v1",
				},
			},
			expected: "INSERT INTO t (ts,f1) values (now,\"v1\")",
		},
		{
			conf: &TaosConfig{
				ProvideTs:   true,
				Database:    "db",
				Table:       "t",
				STable:      "st",
				TsFieldName: "ts",
				TagFields:   []string{"tag1"},
			},
			data: testx.MockTuple{
				Map: map[string]any{
					"k1":   "v1",
					"tag1": "t1",
				},
			},
			error: "timestamp field not found : ts",
		},
		{
			conf: &TaosConfig{
				Database:    "db",
				Table:       "t",
				STable:      "st",
				TsFieldName: "ts",
				Fields:      []string{"ts", "k1", "tag1"},
				TagFields:   []string{"tag1"},
			},
			data: testx.MockTuple{
				Map: map[string]any{
					"tag1": "t1",
				},
			},
			error: "field not found : k1",
		},
		{
			conf: &TaosConfig{
				ProvideTs:   true,
				Database:    "db",
				Table:       "t",
				STable:      "st",
				TsFieldName: "ts",
				TagFields:   []string{"tag1"},
			},
			data: testx.MockTuple{
				Map: map[string]any{
					"ts":   1737628594255,
					"k1":   "v1",
					"tag1": "t1",
				},
			},
			expected: "INSERT INTO t (ts,k1) USING st TAGS(\"t1\") values (1737628594255,\"v1\")",
		},
		{
			conf: &TaosConfig{
				ProvideTs:   true,
				Database:    "db",
				Table:       "t",
				STable:      "st",
				Fields:      []string{"ts", "k1", "k2", "tag1", "tag2"},
				TsFieldName: "ts",
				TagFields:   []string{"tag1", "tag2"},
			},
			data: testx.MockTuple{
				Map: map[string]any{
					"ts":   1737628594255,
					"k1":   "v1",
					"k2":   2,
					"k3":   "v3",
					"tag1": "t1",
					"tag2": 2,
				},
			},
			expected: "INSERT INTO t (ts,k1,k2) USING st TAGS(\"t1\",2) values (1737628594255,\"v1\",2)",
		},
		{
			conf: &TaosConfig{
				ProvideTs:   true,
				Database:    "db",
				Table:       "{{.name}}",
				STable:      "{{.stName}}",
				TsFieldName: "ts",
				TagFields:   []string{"tag1"},
			},
			data: testx.MockTuple{
				Map: map[string]any{
					"ts":   1737628594255,
					"k1":   123,
					"tag1": "t1",
				},
				Template: map[string]string{"{{.name}}": "t", "{{.stName}}": "st"},
			},
			expected: "INSERT INTO t (ts,k1) USING st TAGS(\"t1\") values (1737628594255,123)",
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, test := range tests {
		sql, err := test.conf.buildSql(test.data)
		if !reflect.DeepEqual(test.error, testx.Errstring(err)) {
			t.Errorf("%d: error mismatch:\n  exp=%s\n  got=%s\n\n", i, test.error, err)
		} else if test.error == "" && !reflect.DeepEqual(test.expected, sql) {
			t.Errorf("%d\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, test.expected, sql)
		}
	}
}
