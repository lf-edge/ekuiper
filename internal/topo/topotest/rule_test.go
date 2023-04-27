// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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

package topotest

import (
	"encoding/json"
	"testing"

	"github.com/lf-edge/ekuiper/internal/topo/topotest/mocknode"
	"github.com/lf-edge/ekuiper/pkg/api"
)

func TestSRFSQL(t *testing.T) {
	//Reset
	streamList := []string{"demo", "demoArr"}
	HandleStream(false, streamList, t)
	var tests = []RuleTest{
		{
			Name: "TestSingleSQLRule25",
			Sql:  "SELECT unnest(a) from demoArr group by SESSIONWINDOW(ss, 2, 1);",
			R: [][]map[string]interface{}{
				{
					{
						"error": "the argument for the unnest function should be array",
					},
				},
			},
		},
		{
			Name: "TestSingleSQLRule24",
			Sql:  "Select unnest(a) from demoArr;",
			R: [][]map[string]interface{}{
				{
					{
						"error": "the argument for the unnest function should be array",
					},
				},
			},
		},
		{
			Name: "TestSingleSQLRule21",
			Sql:  `SELECT unnest(demoArr.arr3) as col, demo.size FROM demo inner join demoArr on demo.size = demoArr.x group by SESSIONWINDOW(ss, 2, 1);`,
			R: [][]map[string]interface{}{
				{
					{
						"col":  float64(1),
						"size": float64(1),
					},
					{
						"col":  float64(2),
						"size": float64(1),
					},
					{
						"col":  float64(3),
						"size": float64(1),
					},
				},
			},
		},
		{
			Name: "TestSingleSQLRule22",
			Sql:  `SELECT unnest(arr3) as col,y From demoArr group by y, SESSIONWINDOW(ss, 2, 1);`,
			R: [][]map[string]interface{}{
				{
					{
						"col": float64(1),
						"y":   float64(2),
					},
					{
						"col": float64(2),
						"y":   float64(2),
					},
					{
						"col": float64(3),
						"y":   float64(2),
					},
				},
			},
		},
		{
			Name: "TestSingleSQLRule23",
			Sql:  "SELECT unnest(arr3) as col,a from demoArr group by SESSIONWINDOW(ss, 2, 1);",
			R: [][]map[string]interface{}{
				{
					{
						"col": float64(1),
						"a":   float64(6),
					},
					{
						"col": float64(2),
						"a":   float64(6),
					},
					{
						"col": float64(3),
						"a":   float64(6),
					},
				},
			},
		},
		{
			Name: `TestSingleSQLRule18`,
			Sql:  `SELECT unnest(arr2) FROM demoArr where x=1`,
			R: [][]map[string]interface{}{
				{
					{
						"a": float64(1),
						"b": float64(2),
					},
				},
				{
					{
						"a": float64(3),
						"b": float64(4),
					},
				},
			},
		},
		// The mapping schema created by unnest function will cover the original column if they have the same column name
		{
			Name: `TestSingleSQLRule19`,
			Sql:  `SELECT unnest(arr2),a FROM demoArr where x=1`,
			R: [][]map[string]interface{}{
				{
					{
						"a": float64(1),
						"b": float64(2),
					},
				},
				{
					{
						"a": float64(3),
						"b": float64(4),
					},
				},
			},
		},
		{
			Name: `TestSingleSQLRule20`,
			Sql:  `SELECT unnest(arr3) as col FROM demoArr where x=1`,
			R: [][]map[string]interface{}{
				{
					{
						"col": float64(1),
					},
				},
				{
					{
						"col": float64(2),
					},
				},
				{
					{
						"col": float64(3),
					},
				},
			},
		},
		{
			Name: `TestSingleSQLRule21`,
			Sql:  `SELECT unnest(arr2),x FROM demoArr where x=1`,
			R: [][]map[string]interface{}{
				{
					{
						"a": float64(1),
						"b": float64(2),
						"x": float64(1),
					},
				},
				{
					{
						"a": float64(3),
						"b": float64(4),
						"x": float64(1),
					},
				},
			},
		},
	}
	//Data setup
	HandleStream(true, streamList, t)
	options := []*api.RuleOption{
		{
			BufferLength: 100,
			SendError:    true,
		}, {
			BufferLength:       100,
			SendError:          true,
			Qos:                api.AtLeastOnce,
			CheckpointInterval: 5000,
		}, {
			BufferLength:       100,
			SendError:          true,
			Qos:                api.ExactlyOnce,
			CheckpointInterval: 5000,
		},
	}
	for j, opt := range options {
		DoRuleTest(t, tests, j, opt, 0)
	}
}

func TestSingleSQL(t *testing.T) {
	//Reset
	streamList := []string{"demo", "demoError", "demo1", "table1", "demoTable", "demoArr"}
	HandleStream(false, streamList, t)
	//Data setup
	var tests = []RuleTest{
		{
			Name: `TestSingleSQLRule0`,
			Sql:  `SELECT arr[x:y+1] as col1 FROM demoArr where x=1`,
			R: [][]map[string]interface{}{
				{{
					"col1": []interface{}{
						float64(2), float64(3),
					},
				}},
			},
		},
		{
			Name: `TestSingleSQLRule1`,
			Sql:  `SELECT *, upper(color) FROM demo`,
			R: [][]map[string]interface{}{
				{{
					"color": "red",
					"size":  float64(3),
					"ts":    float64(1541152486013),
					"upper": "RED",
				}},
				{{
					"color": "blue",
					"size":  float64(6),
					"ts":    float64(1541152486822),
					"upper": "BLUE",
				}},
				{{
					"color": "blue",
					"size":  float64(2),
					"ts":    float64(1541152487632),
					"upper": "BLUE",
				}},
				{{
					"color": "yellow",
					"size":  float64(4),
					"ts":    float64(1541152488442),
					"upper": "YELLOW",
				}},
				{{
					"color": "red",
					"size":  float64(1),
					"ts":    float64(1541152489252),
					"upper": "RED",
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(5),
				"op_2_project_0_records_out_total":  int64(5),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(5),
				"sink_mockSink_0_records_out_total": int64(5),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),
			},
			T: &api.PrintableTopo{
				Sources: []string{"source_demo"},
				Edges: map[string][]interface{}{
					"source_demo":  {"op_2_project"},
					"op_2_project": {"sink_mockSink"},
				},
			},
		},
		{
			Name: `TestSingleSQLRule2`,
			Sql:  `SELECT color, ts FROM demo where size > 3`,
			R: [][]map[string]interface{}{
				{{
					"color": "blue",
					"ts":    float64(1541152486822),
				}},
				{{
					"color": "yellow",
					"ts":    float64(1541152488442),
				}},
			},
			M: map[string]interface{}{
				"op_3_project_0_exceptions_total":   int64(0),
				"op_3_project_0_process_latency_us": int64(0),
				"op_3_project_0_records_in_total":   int64(2),
				"op_3_project_0_records_out_total":  int64(2),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(2),
				"sink_mockSink_0_records_out_total": int64(2),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"op_2_filter_0_exceptions_total":   int64(0),
				"op_2_filter_0_process_latency_us": int64(0),
				"op_2_filter_0_records_in_total":   int64(5),
				"op_2_filter_0_records_out_total":  int64(2),
			},
		}, {
			Name: `TestSingleSQLRule3`,
			Sql:  `SELECT size as Int8, ts FROM demo where size > 3`,
			R: [][]map[string]interface{}{
				{{
					"Int8": float64(6),
					"ts":   float64(1541152486822),
				}},
				{{
					"Int8": float64(4),
					"ts":   float64(1541152488442),
				}},
			},
			M: map[string]interface{}{
				"op_3_project_0_exceptions_total":   int64(0),
				"op_3_project_0_process_latency_us": int64(0),
				"op_3_project_0_records_in_total":   int64(2),
				"op_3_project_0_records_out_total":  int64(2),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(2),
				"sink_mockSink_0_records_out_total": int64(2),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"op_2_filter_0_exceptions_total":   int64(0),
				"op_2_filter_0_process_latency_us": int64(0),
				"op_2_filter_0_records_in_total":   int64(5),
				"op_2_filter_0_records_out_total":  int64(2),
			},
		}, {
			Name: `TestSingleSQLRule4`,
			Sql:  `SELECT size as Int8, ts FROM demoError where size > 3`,
			R: [][]map[string]interface{}{
				{{
					"error": "error in preprocessor: field size type mismatch: cannot convert string(red) to int64",
				}},
				{{
					"Int8": float64(6),
					"ts":   float64(1541152486822),
				}},
				{{
					"Int8": float64(4),
					"ts":   float64(1541152488442),
				}},
				{{
					"error": "error in preprocessor: field size type mismatch: cannot convert string(blue) to int64",
				}},
			},
			M: map[string]interface{}{
				"op_3_project_0_exceptions_total":   int64(2),
				"op_3_project_0_process_latency_us": int64(0),
				"op_3_project_0_records_in_total":   int64(4),
				"op_3_project_0_records_out_total":  int64(2),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(4),
				"sink_mockSink_0_records_out_total": int64(4),

				"source_demoError_0_exceptions_total":  int64(2),
				"source_demoError_0_records_in_total":  int64(5),
				"source_demoError_0_records_out_total": int64(5),

				"op_2_filter_0_exceptions_total":   int64(2),
				"op_2_filter_0_process_latency_us": int64(0),
				"op_2_filter_0_records_in_total":   int64(5),
				"op_2_filter_0_records_out_total":  int64(2),
			},
		}, {
			Name: `TestSingleSQLRule5`,
			Sql:  `SELECT meta(topic) as m, ts FROM demo`,
			R: [][]map[string]interface{}{
				{{
					"m":  "mock",
					"ts": float64(1541152486013),
				}},
				{{
					"m":  "mock",
					"ts": float64(1541152486822),
				}},
				{{
					"m":  "mock",
					"ts": float64(1541152487632),
				}},
				{{
					"m":  "mock",
					"ts": float64(1541152488442),
				}},
				{{
					"m":  "mock",
					"ts": float64(1541152489252),
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(5),
				"op_2_project_0_records_out_total":  int64(5),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(5),
				"sink_mockSink_0_records_out_total": int64(5),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),
			},
		}, {
			Name: `TestSingleSQLRule6`,
			Sql:  `SELECT color, ts FROM demo where size > 3 and meta(topic)="mock"`,
			R: [][]map[string]interface{}{
				{{
					"color": "blue",
					"ts":    float64(1541152486822),
				}},
				{{
					"color": "yellow",
					"ts":    float64(1541152488442),
				}},
			},
			M: map[string]interface{}{
				"op_3_project_0_exceptions_total":   int64(0),
				"op_3_project_0_process_latency_us": int64(0),
				"op_3_project_0_records_in_total":   int64(2),
				"op_3_project_0_records_out_total":  int64(2),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(2),
				"sink_mockSink_0_records_out_total": int64(2),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"op_2_filter_0_exceptions_total":   int64(0),
				"op_2_filter_0_process_latency_us": int64(0),
				"op_2_filter_0_records_in_total":   int64(5),
				"op_2_filter_0_records_out_total":  int64(2),
			},
		}, {
			Name: `TestSingleSQLRule7`,
			Sql:  "SELECT `from` FROM demo1",
			R: [][]map[string]interface{}{
				{{
					"from": "device1",
				}},
				{{
					"from": "device2",
				}},
				{{
					"from": "device3",
				}},
				{{
					"from": "device1",
				}},
				{{
					"from": "device3",
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(5),
				"op_2_project_0_records_out_total":  int64(5),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(5),
				"sink_mockSink_0_records_out_total": int64(5),

				"source_demo1_0_exceptions_total":  int64(0),
				"source_demo1_0_records_in_total":  int64(5),
				"source_demo1_0_records_out_total": int64(5),
			},
		}, {
			Name: `TestSingleSQLRule8`,
			Sql:  "SELECT * FROM demo1 where `from`=\"device1\"",
			R: [][]map[string]interface{}{
				{{
					"temp": float64(25.5),
					"hum":  float64(65),
					"from": "device1",
					"ts":   float64(1541152486013),
				}},
				{{
					"temp": float64(27.4),
					"hum":  float64(80),
					"from": "device1",
					"ts":   float64(1541152488442),
				}},
			},
			M: map[string]interface{}{
				"op_3_project_0_exceptions_total":   int64(0),
				"op_3_project_0_process_latency_us": int64(0),
				"op_3_project_0_records_in_total":   int64(2),
				"op_3_project_0_records_out_total":  int64(2),

				"op_2_filter_0_exceptions_total":   int64(0),
				"op_2_filter_0_process_latency_us": int64(0),
				"op_2_filter_0_records_in_total":   int64(5),
				"op_2_filter_0_records_out_total":  int64(2),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(2),
				"sink_mockSink_0_records_out_total": int64(2),

				"source_demo1_0_exceptions_total":  int64(0),
				"source_demo1_0_records_in_total":  int64(5),
				"source_demo1_0_records_out_total": int64(5),
			},
		}, {
			Name: `TestSingleSQLRule9`,
			Sql:  `SELECT color, CASE WHEN size < 2 THEN "S" WHEN size < 4 THEN "M" ELSE "L" END as s, ts FROM demo`,
			R: [][]map[string]interface{}{
				{{
					"color": "red",
					"s":     "M",
					"ts":    float64(1541152486013),
				}},
				{{
					"color": "blue",
					"s":     "L",
					"ts":    float64(1541152486822),
				}},
				{{
					"color": "blue",
					"s":     "M",
					"ts":    float64(1541152487632),
				}},
				{{
					"color": "yellow",
					"s":     "L",
					"ts":    float64(1541152488442),
				}},
				{{
					"color": "red",
					"s":     "S",
					"ts":    float64(1541152489252),
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(5),
				"op_2_project_0_records_out_total":  int64(5),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(5),
				"sink_mockSink_0_records_out_total": int64(5),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),
			},
			T: &api.PrintableTopo{
				Sources: []string{"source_demo"},
				Edges: map[string][]interface{}{
					"source_demo":  {"op_2_project"},
					"op_2_project": {"sink_mockSink"},
				},
			},
		}, {
			Name: `TestSingleSQLRule10`,
			Sql:  "SELECT * FROM demo INNER JOIN table1 on demo.ts = table1.id",
			R: [][]map[string]interface{}{
				{{
					"id":    float64(1541152486013),
					"name":  "name1",
					"color": "red",
					"size":  float64(3),
					"ts":    float64(1541152486013),
				}},
				{{
					"id":    float64(1541152487632),
					"name":  "name2",
					"color": "blue",
					"size":  float64(2),
					"ts":    float64(1541152487632),
				}},
				{{
					"id":    float64(1541152489252),
					"name":  "name3",
					"color": "red",
					"size":  float64(1),
					"ts":    float64(1541152489252),
				}},
			},
			W: 15,
			M: map[string]interface{}{
				"op_3_join_aligner_0_records_in_total":  int64(6),
				"op_3_join_aligner_0_records_out_total": int64(5),

				"op_4_join_0_exceptions_total":  int64(0),
				"op_4_join_0_records_in_total":  int64(5),
				"op_4_join_0_records_out_total": int64(3),

				"op_5_project_0_exceptions_total":  int64(0),
				"op_5_project_0_records_in_total":  int64(3),
				"op_5_project_0_records_out_total": int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"source_table1_0_exceptions_total":  int64(0),
				"source_table1_0_records_in_total":  int64(4),
				"source_table1_0_records_out_total": int64(1),
			},
		}, {
			Name: `TestSingleSQLRule11`,
			Sql:  "SELECT device FROM demo INNER JOIN demoTable on demo.ts = demoTable.ts",
			R: [][]map[string]interface{}{
				{{
					"device": "device2",
				}},
				{{
					"device": "device4",
				}},
				{{
					"device": "device5",
				}},
			},
			M: map[string]interface{}{
				"op_3_join_aligner_0_records_in_total":  int64(10),
				"op_3_join_aligner_0_records_out_total": int64(5),

				"op_4_join_0_exceptions_total":  int64(0),
				"op_4_join_0_records_in_total":  int64(5),
				"op_4_join_0_records_out_total": int64(3),

				"op_5_project_0_exceptions_total":  int64(0),
				"op_5_project_0_records_in_total":  int64(3),
				"op_5_project_0_records_out_total": int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"source_demoTable_0_exceptions_total":  int64(0),
				"source_demoTable_0_records_in_total":  int64(5),
				"source_demoTable_0_records_out_total": int64(5),
			},
		}, {
			Name: `TestSingleSQLRule12`,
			Sql:  "SELECT demo.ts as demoTs, table1.id as table1Id FROM demo INNER JOIN table1 on demoTs = table1Id",
			R: [][]map[string]interface{}{
				{{
					"table1Id": float64(1541152486013),
					"demoTs":   float64(1541152486013),
				}},
				{{
					"table1Id": float64(1541152487632),
					"demoTs":   float64(1541152487632),
				}},
				{{
					"table1Id": float64(1541152489252),
					"demoTs":   float64(1541152489252),
				}},
			},
			W: 15,
			M: map[string]interface{}{
				"op_3_join_aligner_0_records_in_total":  int64(6),
				"op_3_join_aligner_0_records_out_total": int64(5),

				"op_4_join_0_exceptions_total":  int64(0),
				"op_4_join_0_records_in_total":  int64(5),
				"op_4_join_0_records_out_total": int64(3),

				"op_5_project_0_exceptions_total":  int64(0),
				"op_5_project_0_records_in_total":  int64(3),
				"op_5_project_0_records_out_total": int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"source_table1_0_exceptions_total":  int64(0),
				"source_table1_0_records_in_total":  int64(4),
				"source_table1_0_records_out_total": int64(1),
			},
		}, {
			Name: `TestChanged13`,
			Sql:  "SELECT changed_cols(\"tt_\", true, color, size) FROM demo",
			R: [][]map[string]interface{}{
				{{
					"tt_color": "red",
					"tt_size":  float64(3),
				}},
				{{
					"tt_color": "blue",
					"tt_size":  float64(6),
				}},
				{{
					"tt_size": float64(2),
				}},
				{{
					"tt_color": "yellow",
					"tt_size":  float64(4),
				}},
				{{
					"tt_color": "red",
					"tt_size":  float64(1),
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(5),
				"op_2_project_0_records_out_total":  int64(5),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(5),
				"sink_mockSink_0_records_out_total": int64(5),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),
			},
		}, {
			Name: `TestAliasOrderBy14`,
			Sql:  "SELECT color, count(*) as c FROM demo where color != \"red\" GROUP BY COUNTWINDOW(5), color Order by c DESC",
			R: [][]map[string]interface{}{
				{{
					"color": "blue",
					"c":     float64(2),
				},
					{
						"color": "yellow",
						"c":     float64(1),
					},
				},
			},
			M: map[string]interface{}{
				"op_6_project_0_exceptions_total":   int64(0),
				"op_6_project_0_process_latency_us": int64(0),
				"op_6_project_0_records_in_total":   int64(1),
				"op_6_project_0_records_out_total":  int64(1),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(1),
				"sink_mockSink_0_records_out_total": int64(1),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),
			},
		},
		{
			Name: `TestSingleSQLRule17`,
			Sql:  `SELECT arr[x:4] as col1 FROM demoArr where x=1`,
			R: [][]map[string]interface{}{
				{{
					"col1": []interface{}{
						float64(2), float64(3), float64(4),
					},
				}},
			},
		},
		{
			Name: `TestSingleSQLRule16`,
			Sql:  `SELECT arr[1:y] as col1 FROM demoArr where x=1`,
			R: [][]map[string]interface{}{
				{{
					"col1": []interface{}{
						float64(2),
					},
				}},
			},
		},
		{
			Name: `TestSingleSQLRule15`,
			Sql:  `SELECT arr[1] as col1 FROM demoArr where x=1`,
			R: [][]map[string]interface{}{
				{{
					"col1": float64(2),
				}},
			},
		},
		{
			Name: `TestLagAlias`,
			Sql:  "SELECT lag(size) as lastSize, lag(had_changed(true,size)), size, lastSize/size as changeRate FROM demo WHERE size > 2",
			R: [][]map[string]interface{}{
				{{
					"size": float64(3),
				}},
				{{
					"lastSize":   float64(3),
					"size":       float64(6),
					"lag":        true,
					"changeRate": float64(0),
				}},
				{{
					"lastSize":   float64(2),
					"size":       float64(4),
					"lag":        true,
					"changeRate": float64(0),
				}},
			},
			M: map[string]interface{}{
				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),
			},
		},
		{
			Name: `TestLagPartition`,
			Sql:  "SELECT color, lag(size) over (partition by color) as lastSize, size, lastSize/size as changeRate FROM demo",
			R: [][]map[string]interface{}{
				{{
					"color": "red",
					"size":  float64(3),
				}},
				{{
					"color": "blue",
					"size":  float64(6),
				}},
				{{
					"color":      "blue",
					"lastSize":   float64(6),
					"size":       float64(2),
					"changeRate": float64(3),
				}},
				{{
					"color": "yellow",
					"size":  float64(4),
				}},
				{{
					"color":      "red",
					"lastSize":   float64(3),
					"size":       float64(1),
					"changeRate": float64(3),
				}},
			},
			M: map[string]interface{}{
				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(5),
				"sink_mockSink_0_records_out_total": int64(5),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),
			},
		},
	}
	HandleStream(true, streamList, t)
	options := []*api.RuleOption{
		{
			BufferLength: 100,
			SendError:    true,
		}, {
			BufferLength:       100,
			SendError:          true,
			Qos:                api.AtLeastOnce,
			CheckpointInterval: 5000,
		}, {
			BufferLength:       100,
			SendError:          true,
			Qos:                api.ExactlyOnce,
			CheckpointInterval: 5000,
		},
	}
	for j, opt := range options {
		DoRuleTest(t, tests, j, opt, 0)
	}
}

func TestSingleSQLError(t *testing.T) {
	//Reset
	streamList := []string{"ldemo"}
	HandleStream(false, streamList, t)
	//Data setup
	var tests = []RuleTest{
		{
			Name: `TestSingleSQLErrorRule1`,
			Sql:  `SELECT color, ts FROM ldemo where size >= 3`,
			R: [][]map[string]interface{}{
				{{
					"color": "red",
					"ts":    float64(1541152486013),
				}},
				{{
					"error": "run Where error: invalid operation string(string) >= int64(3)",
				}},
				{{
					"ts": float64(1541152487632),
				}},
			},
			M: map[string]interface{}{
				"op_3_project_0_exceptions_total":   int64(1),
				"op_3_project_0_process_latency_us": int64(0),
				"op_3_project_0_records_in_total":   int64(3),
				"op_3_project_0_records_out_total":  int64(2),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),

				"source_ldemo_0_exceptions_total":  int64(0),
				"source_ldemo_0_records_in_total":  int64(5),
				"source_ldemo_0_records_out_total": int64(5),

				"op_2_filter_0_exceptions_total":   int64(1),
				"op_2_filter_0_process_latency_us": int64(0),
				"op_2_filter_0_records_in_total":   int64(5),
				"op_2_filter_0_records_out_total":  int64(2),
			},
		}, {
			Name: `TestSingleSQLErrorRule2`,
			Sql:  `SELECT size * 5 FROM ldemo`,
			R: [][]map[string]interface{}{
				{{
					"kuiper_field_0": float64(15),
				}},
				{{
					"error": "run Select error: invalid operation string(string) * int64(5)",
				}},
				{{
					"kuiper_field_0": float64(15),
				}},
				{{
					"kuiper_field_0": float64(10),
				}},
				{{}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(1),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(5),
				"op_2_project_0_records_out_total":  int64(4),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(5),
				"sink_mockSink_0_records_out_total": int64(5),

				"source_ldemo_0_exceptions_total":  int64(0),
				"source_ldemo_0_records_in_total":  int64(5),
				"source_ldemo_0_records_out_total": int64(5),
			},
		},
	}
	HandleStream(true, streamList, t)
	DoRuleTest(t, tests, 0, &api.RuleOption{
		BufferLength: 100,
		SendError:    true,
	}, 0)
}

func TestSingleSQLOmitError(t *testing.T) {
	//Reset
	streamList := []string{"ldemo"}
	HandleStream(false, streamList, t)
	//Data setup
	var tests = []RuleTest{
		{
			Name: `TestSingleSQLErrorRule1`,
			Sql:  `SELECT color, ts FROM ldemo where size >= 3`,
			R: [][]map[string]interface{}{
				{{
					"color": "red",
					"ts":    float64(1541152486013),
				}},
				{{
					"ts": float64(1541152487632),
				}},
			},
			M: map[string]interface{}{
				"op_3_project_0_exceptions_total":   int64(0),
				"op_3_project_0_process_latency_us": int64(0),
				"op_3_project_0_records_in_total":   int64(2),
				"op_3_project_0_records_out_total":  int64(2),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(2),
				"sink_mockSink_0_records_out_total": int64(2),

				"source_ldemo_0_exceptions_total":  int64(0),
				"source_ldemo_0_records_in_total":  int64(5),
				"source_ldemo_0_records_out_total": int64(5),

				"op_2_filter_0_exceptions_total":   int64(1),
				"op_2_filter_0_process_latency_us": int64(0),
				"op_2_filter_0_records_in_total":   int64(5),
				"op_2_filter_0_records_out_total":  int64(2),
			},
		}, {
			Name: `TestSingleSQLErrorRule2`,
			Sql:  `SELECT size * 5 FROM ldemo`,
			R: [][]map[string]interface{}{
				{{
					"kuiper_field_0": float64(15),
				}},
				{{
					"kuiper_field_0": float64(15),
				}},
				{{
					"kuiper_field_0": float64(10),
				}},
				{{}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(1),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(5),
				"op_2_project_0_records_out_total":  int64(4),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(4),
				"sink_mockSink_0_records_out_total": int64(4),

				"source_ldemo_0_exceptions_total":  int64(0),
				"source_ldemo_0_records_in_total":  int64(5),
				"source_ldemo_0_records_out_total": int64(5),
			},
		},
	}
	HandleStream(true, streamList, t)
	DoRuleTest(t, tests, 0, &api.RuleOption{
		BufferLength: 100,
		SendError:    false,
	}, 0)
}

func TestSingleSQLTemplate(t *testing.T) {
	//Reset
	streamList := []string{"demo"}
	HandleStream(false, streamList, t)
	//Data setup
	var tests = []RuleTest{
		{
			Name: `TestSingleSQLTemplateRule1`,
			Sql:  `SELECT * FROM demo`,
			R: []map[string]interface{}{
				{
					"c":       "red",
					"wrapper": "w1",
				},
				{
					"c":       "blue",
					"wrapper": "w1",
				},
				{
					"c":       "blue",
					"wrapper": "w1",
				},
				{
					"c":       "yellow",
					"wrapper": "w1",
				},
				{
					"c":       "red",
					"wrapper": "w1",
				},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(5),
				"op_2_project_0_records_out_total":  int64(5),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(5),
				"sink_mockSink_0_records_out_total": int64(5),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),
			},
		},
	}
	HandleStream(true, streamList, t)
	doRuleTestBySinkProps(t, tests, 0, &api.RuleOption{
		BufferLength: 100,
		SendError:    true,
	}, 0, map[string]interface{}{
		"dataTemplate": `{"wrapper":"w1", "c":"{{.color}}"}`,
		"sendSingle":   true,
	}, func(result [][]byte) interface{} {
		var maps []map[string]interface{}
		for _, v := range result {
			var mapRes map[string]interface{}
			err := json.Unmarshal(v, &mapRes)
			if err != nil {
				t.Errorf("Failed to parse the input into map")
				continue
			}
			maps = append(maps, mapRes)
		}
		return maps
	})
}

func TestNoneSingleSQLTemplate(t *testing.T) {
	//Reset
	streamList := []string{"demo"}
	HandleStream(false, streamList, t)
	//Data setup
	var tests = []RuleTest{
		{
			Name: `TestNoneSingleSQLTemplateRule1`,
			Sql:  `SELECT * FROM demo`,
			R: [][]byte{
				[]byte("<div>results</div><ul><li>red - 3</li></ul>"),
				[]byte("<div>results</div><ul><li>blue - 6</li></ul>"),
				[]byte("<div>results</div><ul><li>blue - 2</li></ul>"),
				[]byte("<div>results</div><ul><li>yellow - 4</li></ul>"),
				[]byte("<div>results</div><ul><li>red - 1</li></ul>"),
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(5),
				"op_2_project_0_records_out_total":  int64(5),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(5),
				"sink_mockSink_0_records_out_total": int64(5),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),
			},
		},
	}
	HandleStream(true, streamList, t)
	doRuleTestBySinkProps(t, tests, 0, &api.RuleOption{
		BufferLength: 100,
		SendError:    true,
	}, 0, map[string]interface{}{
		"dataTemplate": `<div>results</div><ul>{{range .}}<li>{{.color}} - {{.size}}</li>{{end}}</ul>`,
	}, func(result [][]byte) interface{} {
		return result
	})
}

func TestSingleSQLForBinary(t *testing.T) {
	//Reset
	streamList := []string{"binDemo"}
	HandleStream(false, streamList, t)
	//Data setup
	var tests = []RuleTest{
		{
			Name: `TestSingleSQLRule1`,
			Sql:  `SELECT * FROM binDemo`,
			R: [][]map[string]interface{}{
				{{
					"self": mocknode.Image,
				}},
			},
			W: 50,
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(1),
				"op_2_project_0_records_out_total":  int64(1),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(1),
				"sink_mockSink_0_records_out_total": int64(1),

				"source_binDemo_0_exceptions_total":  int64(0),
				"source_binDemo_0_records_in_total":  int64(1),
				"source_binDemo_0_records_out_total": int64(1),
			},
		},
	}
	HandleStream(true, streamList, t)
	options := []*api.RuleOption{
		{
			BufferLength: 100,
			SendError:    true,
		}, {
			BufferLength:       100,
			SendError:          true,
			Qos:                api.AtLeastOnce,
			CheckpointInterval: 5000,
		}, {
			BufferLength:       100,
			SendError:          true,
			Qos:                api.ExactlyOnce,
			CheckpointInterval: 5000,
		},
	}
	byteFunc := func(result [][]byte) interface{} {
		var maps [][]map[string]interface{}
		for _, v := range result {
			var mapRes []map[string][]byte
			err := json.Unmarshal(v, &mapRes)
			if err != nil {
				panic("Failed to parse the input into map")
			}
			mapInt := make([]map[string]interface{}, len(mapRes))
			for i, mv := range mapRes {
				mapInt[i] = make(map[string]interface{})
				//assume only one key
				for k, v := range mv {
					mapInt[i][k] = v
				}
			}
			maps = append(maps, mapInt)
		}
		return maps
	}
	for j, opt := range options {
		doRuleTestBySinkProps(t, tests, j, opt, 0, nil, byteFunc)
	}
}
