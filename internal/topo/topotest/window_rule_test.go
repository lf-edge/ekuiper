// Copyright 2021-2025 EMQ Technologies Co., Ltd.
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
	"strings"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

func TestWindow(t *testing.T) {
	// Reset
	streamList := []string{"demo", "demoError", "demo1", "sessionDemo", "table1", "demoE2"}
	HandleStream(false, streamList, t)
	tests := []RuleTest{
		{
			Name: `TestWindowRule0`,
			Sql:  `SELECT size,color FROM demo GROUP BY SlidingWindow(ss, 5) Filter (where color = "red") Over (when size = 1)`,
			R: [][]map[string]interface{}{
				{
					{
						"size":  3,
						"color": "red",
					},
					{
						"size":  1,
						"color": "red",
					},
				},
			},
			M: map[string]interface{}{},
		},
		{
			Name: `TestWindowRule1`,
			Sql:  `SELECT *, event_time() as et FROM demo GROUP BY HOPPINGWINDOW(ss, 2, 1)`,
			R: [][]map[string]interface{}{
				{{
					"color": "red",
					"size":  3,
					"ts":    1541152486013,
					"et":    int64(1541152486013),
				}, {
					"color": "blue",
					"size":  6,
					"ts":    1541152486822,
					"et":    int64(1541152486822),
				}},
				{{
					"color": "red",
					"size":  3,
					"ts":    1541152486013,
					"et":    int64(1541152486013),
				}, {
					"color": "blue",
					"size":  6,
					"ts":    1541152486822,
					"et":    int64(1541152486822),
				}, {
					"color": "blue",
					"size":  2,
					"ts":    1541152487632,
					"et":    int64(1541152487632),
				}},
				{{
					"color": "blue",
					"size":  2,
					"ts":    1541152487632,
					"et":    int64(1541152487632),
				}, {
					"color": "yellow",
					"size":  4,
					"ts":    1541152488442,
					"et":    int64(1541152488442),
				}},
			},
			M: map[string]interface{}{
				"op_3_project_0_exceptions_total":   int64(0),
				"op_3_project_0_process_latency_us": int64(0),
				"op_3_project_0_records_in_total":   int64(3),
				"op_3_project_0_records_out_total":  int64(3),

				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(3),
				"sink_memory_0_0_records_out_total": int64(3),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"op_2_window_0_exceptions_total":   int64(0),
				"op_2_window_0_process_latency_us": int64(0),
				"op_2_window_0_records_in_total":   int64(5),
				"op_2_window_0_records_out_total":  int64(3),
			},
		},
		{
			Name: `TestWindowRule2`,
			Sql:  `SELECT color, ts FROM demo where size > 2 GROUP BY tumblingwindow(ss, 1)`,
			R: [][]map[string]interface{}{
				{{
					"color": "red",
					"ts":    1541152486013,
				}, {
					"color": "blue",
					"ts":    1541152486822,
				}},
				{},
				{{
					"color": "yellow",
					"ts":    1541152488442,
				}},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(3),
				"sink_memory_0_0_records_out_total": int64(3),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"op_3_window_0_exceptions_total":   int64(0),
				"op_3_window_0_process_latency_us": int64(0),
				"op_3_window_0_records_in_total":   int64(3),
				"op_3_window_0_records_out_total":  int64(3),

				"op_2_filter_0_exceptions_total":   int64(0),
				"op_2_filter_0_process_latency_us": int64(0),
				"op_2_filter_0_records_in_total":   int64(5),
				"op_2_filter_0_records_out_total":  int64(3),
			},
		},
		{
			Name: `TestWindowRule3`,
			Sql:  `SELECT color, temp, demo.ts as ts1, demo1.ts as ts2, demo.ts - demo1.ts as diff FROM demo INNER JOIN demo1 ON ts1 = ts2 GROUP BY SlidingWindow(ss, 1) HAVING last_agg_hit_count() < 7`,
			R: [][]map[string]interface{}{
				{{
					"color": "red",
					"temp":  25.5,
					"ts1":   1541152486013,
					"ts2":   1541152486013,
					"diff":  int64(0),
				}}, {{
					"color": "red",
					"temp":  25.5,
					"ts1":   1541152486013,
					"ts2":   1541152486013,
					"diff":  int64(0),
				}}, {{
					"color": "red",
					"temp":  25.5,
					"ts1":   1541152486013,
					"ts2":   1541152486013,
					"diff":  int64(0),
				}}, {{
					"color": "blue",
					"temp":  28.1,
					"ts1":   1541152487632,
					"ts2":   1541152487632,
					"diff":  int64(0),
				}}, {{
					"color": "blue",
					"temp":  28.1,
					"ts1":   1541152487632,
					"ts2":   1541152487632,
					"diff":  int64(0),
				}}, {{
					"color": "blue",
					"temp":  28.1,
					"ts1":   1541152487632,
					"ts2":   1541152487632,
					"diff":  int64(0),
				}, {
					"color": "yellow",
					"temp":  27.4,
					"ts1":   1541152488442,
					"ts2":   1541152488442,
					"diff":  int64(0),
				}}, {{
					"color": "yellow",
					"temp":  27.4,
					"ts1":   1541152488442,
					"ts2":   1541152488442,
					"diff":  int64(0),
				}},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(7),
				"sink_memory_0_0_records_out_total": int64(7),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"source_demo1_0_exceptions_total":  int64(0),
				"source_demo1_0_records_in_total":  int64(5),
				"source_demo1_0_records_out_total": int64(5),

				"op_3_window_0_exceptions_total":   int64(0),
				"op_3_window_0_process_latency_us": int64(0),
				"op_3_window_0_records_in_total":   int64(10),
				"op_3_window_0_records_out_total":  int64(10),

				"op_4_join_0_exceptions_total":   int64(0),
				"op_4_join_0_process_latency_us": int64(0),
				"op_4_join_0_records_in_total":   int64(10),
				"op_4_join_0_records_out_total":  int64(8),
			},
			T: &def.PrintableTopo{
				Sources: []string{"source_demo", "source_demo1"},
				Edges: map[string][]interface{}{
					"source_demo":  {"op_3_window"},
					"source_demo1": {"op_3_window"},
					"op_3_window":  {"op_4_join"},
					"op_4_join":    {"op_5_having"},
					"op_5_having":  {"op_6_project"},
					"op_6_project": {"sink_memory_0"},
				},
			},
		},
		{
			Name: `TestWindowRule4`,
			Sql:  `SELECT color, count(*) as c FROM demo GROUP BY SlidingWindow(ss, 2) OVER(WHEN ts - last_hit_time() > 1000) , color ORDER BY color`,
			R: [][]map[string]interface{}{
				{{
					"color": "red",
					"c":     1,
				}}, {{
					"color": "blue",
					"c":     2,
				}, {
					"color": "red",
					"c":     1,
				}}, {{
					"color": "blue",
					"c":     1,
				}, {
					"color": "red",
					"c":     1,
				}, {
					"color": "yellow",
					"c":     1,
				}},
			},
			M: map[string]interface{}{
				"op_5_project_0_exceptions_total":   int64(0),
				"op_5_project_0_process_latency_us": int64(0),
				"op_5_project_0_records_in_total":   int64(3),
				"op_5_project_0_records_out_total":  int64(3),

				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(3),
				"sink_memory_0_0_records_out_total": int64(3),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"op_2_window_0_exceptions_total":   int64(0),
				"op_2_window_0_process_latency_us": int64(0),
				"op_2_window_0_records_in_total":   int64(5),
				"op_2_window_0_records_out_total":  int64(3),
			},
		},
		{
			Name: `TestWindowRule5`,
			Sql:  `SELECT count(temp), window_start() as ws, window_end(), event_time() as et FROM sessionDemo GROUP BY SessionWindow(ss, 2, 1) `,
			R: [][]map[string]interface{}{
				{{
					"count":      2,
					"ws":         int64(1541152486013),
					"window_end": int64(1541152487823), // timeout
					"et":         int64(1541152487823),
				}}, {{
					"count":      3,
					"ws":         int64(1541152487932),
					"window_end": int64(1541152490000), // tick
					"et":         int64(1541152490000),
				}}, {{
					"count":      5,
					"ws":         int64(1541152490000),
					"window_end": int64(1541152494000), // tick
					"et":         int64(1541152494000),
				}},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(3),
				"sink_memory_0_0_records_out_total": int64(3),

				"source_sessionDemo_0_exceptions_total":  int64(0),
				"source_sessionDemo_0_records_in_total":  int64(11),
				"source_sessionDemo_0_records_out_total": int64(11),

				"op_2_window_0_exceptions_total":   int64(0),
				"op_2_window_0_process_latency_us": int64(0),
				"op_2_window_0_records_in_total":   int64(11),
				"op_2_window_0_records_out_total":  int64(3),
			},
		},
		{
			Name: `TestWindowRule6`,
			Sql:  `SELECT window_end(), event_time(), sum(temp) as temp1, count(color) as c, window_start() FROM demo INNER JOIN demo1 ON demo.ts = demo1.ts GROUP BY SlidingWindow(ss, 1)`,
			R: [][]map[string]interface{}{
				{{
					"temp1":        25.5,
					"c":            1,
					"window_start": int64(1541152485115),
					"window_end":   int64(1541152486115),
					"event_time":   int64(1541152486115),
				}}, {{
					"temp1":        25.5,
					"c":            1,
					"window_start": int64(1541152485822),
					"window_end":   int64(1541152486822),
					"event_time":   int64(1541152486822),
				}}, {{
					"temp1":        25.5,
					"c":            1,
					"window_start": int64(1541152485903),
					"window_end":   int64(1541152486903),
					"event_time":   int64(1541152486903),
				}}, {{
					"temp1":        28.1,
					"c":            1,
					"window_start": int64(1541152486702),
					"window_end":   int64(1541152487702),
					"event_time":   int64(1541152487702),
				}}, {{
					"temp1":        28.1,
					"c":            1,
					"window_start": int64(1541152487442),
					"window_end":   int64(1541152488442),
					"event_time":   int64(1541152488442),
				}}, {{
					"temp1":        55.5,
					"c":            2,
					"window_start": int64(1541152487605),
					"window_end":   int64(1541152488605),
					"event_time":   int64(1541152488605),
				}}, {{
					"temp1":        27.4,
					"c":            1,
					"window_start": int64(1541152488252),
					"window_end":   int64(1541152489252),
					"event_time":   int64(1541152489252),
				}}, {{
					"temp1":        52.9,
					"c":            2,
					"window_start": int64(1541152488305),
					"window_end":   int64(1541152489305),
					"event_time":   int64(1541152489305),
				}},
			},
			M: map[string]interface{}{
				"op_5_project_0_exceptions_total":   int64(0),
				"op_5_project_0_process_latency_us": int64(0),
				"op_5_project_0_records_in_total":   int64(8),
				"op_5_project_0_records_out_total":  int64(8),

				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(8),
				"sink_memory_0_0_records_out_total": int64(8),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"source_demo1_0_exceptions_total":  int64(0),
				"source_demo1_0_records_in_total":  int64(5),
				"source_demo1_0_records_out_total": int64(5),

				"op_3_window_0_exceptions_total":   int64(0),
				"op_3_window_0_process_latency_us": int64(0),
				"op_3_window_0_records_in_total":   int64(10),
				"op_3_window_0_records_out_total":  int64(10),

				"op_4_join_0_exceptions_total":   int64(0),
				"op_4_join_0_process_latency_us": int64(0),
				"op_4_join_0_records_in_total":   int64(10),
				"op_4_join_0_records_out_total":  int64(8),
			},
		},
		{
			Name: `TestWindowRule7`,
			Sql:  `SELECT * FROM demoError GROUP BY HOPPINGWINDOW(ss, 2, 1)`,
			R: [][]map[string]interface{}{
				{{
					"error": "error in preprocessor: field size type mismatch: cannot convert string(red) to int64",
				}},
				{{
					"color": "blue",
					"size":  int64(6),
					"ts":    int64(1541152486822),
				}},
				{{
					"color": "blue",
					"size":  int64(6),
					"ts":    int64(1541152486822),
				}, {
					"color": "blue",
					"size":  int64(2),
					"ts":    int64(1541152487632),
				}},
				{{
					"error": "error in preprocessor: field color type mismatch: cannot convert int(7) to string",
				}},
				{{
					"color": "blue",
					"size":  int64(2),
					"ts":    int64(1541152487632),
				}},
				{{
					"error": "error in preprocessor: field size type mismatch: cannot convert string(blue) to int64",
				}},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(6),
				"sink_memory_0_0_records_out_total": int64(6),

				"source_demoError_0_exceptions_total":  int64(0),
				"source_demoError_0_records_in_total":  int64(5),
				"source_demoError_0_records_out_total": int64(5),
			},
		},
		{
			Name: `TestWindowRule8`,
			Sql:  `SELECT color, window_end(), event_time() as et, ts, count(*) as c, window_start() FROM demo where size > 2 GROUP BY tumblingwindow(ss, 1) having c > 1`,
			R: [][]map[string]interface{}{
				{{
					"color":        "red",
					"ts":           1541152486013,
					"c":            2,
					"window_start": int64(1541152486000),
					"window_end":   int64(1541152487000),
					"et":           int64(1541152487000),
				}},
			},
			M: map[string]interface{}{
				"op_5_project_0_exceptions_total":   int64(0),
				"op_5_project_0_process_latency_us": int64(0),
				"op_5_project_0_records_in_total":   int64(1),
				"op_5_project_0_records_out_total":  int64(1),

				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(1),
				"sink_memory_0_0_records_out_total": int64(1),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"op_3_window_0_exceptions_total":   int64(0),
				"op_3_window_0_process_latency_us": int64(0),
				"op_3_window_0_records_in_total":   int64(3),
				"op_3_window_0_records_out_total":  int64(3),

				"op_2_filter_0_exceptions_total":   int64(0),
				"op_2_filter_0_process_latency_us": int64(0),
				"op_2_filter_0_records_in_total":   int64(5),
				"op_2_filter_0_records_out_total":  int64(3),

				"op_4_having_0_exceptions_total":   int64(0),
				"op_4_having_0_process_latency_us": int64(0),
				"op_4_having_0_records_in_total":   int64(3),
				"op_4_having_0_records_out_total":  int64(1),
			},
		},
		{
			Name: `TestWindowRule9`,
			Sql:  `SELECT color, window_start(), window_end() FROM demo GROUP BY HOPPINGWINDOW(ss, 2, 1) FILTER( WHERE size > 2)`,
			R: [][]map[string]interface{}{
				{{
					"color":        "red",
					"window_start": int64(1541152485000),
					"window_end":   int64(1541152487000),
				}, {
					"color":        "blue",
					"window_start": int64(1541152485000),
					"window_end":   int64(1541152487000),
				}},
				{{
					"color":        "red",
					"window_start": int64(1541152486000),
					"window_end":   int64(1541152488000),
				}, {
					"color":        "blue",
					"window_start": int64(1541152486000),
					"window_end":   int64(1541152488000),
				}},
				{{
					"color":        "yellow",
					"window_start": int64(1541152487000),
					"window_end":   int64(1541152489000),
				}},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(3),
				"sink_memory_0_0_records_out_total": int64(3),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"op_3_window_0_exceptions_total":   int64(0),
				"op_3_window_0_process_latency_us": int64(0),
				"op_3_window_0_records_in_total":   int64(3),
				"op_3_window_0_records_out_total":  int64(3),
			},
		},
		{
			Name: `TestCountWindowRule1`,
			Sql:  `SELECT collect(*)[0]->color as c, window_end() as we FROM demo GROUP BY COUNTWINDOW(3)`,
			R: [][]map[string]interface{}{
				{{
					"c":  "red",
					"we": int64(1541152487632),
				}},
			},
			M: map[string]interface{}{
				"op_3_project_0_exceptions_total":   int64(0),
				"op_3_project_0_process_latency_us": int64(0),
				"op_3_project_0_records_in_total":   int64(1),
				"op_3_project_0_records_out_total":  int64(1),

				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(1),
				"sink_memory_0_0_records_out_total": int64(1),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"op_2_window_0_exceptions_total":   int64(0),
				"op_2_window_0_process_latency_us": int64(0),
				"op_2_window_0_records_in_total":   int64(5),
				"op_2_window_0_records_out_total":  int64(1),
			},
		},
		{
			Name: `TestWindowRule10`,
			Sql:  `SELECT deduplicate(color, false)->color as c FROM demo GROUP BY SlidingWindow(hh, 1)`,
			R: [][]map[string]interface{}{
				{{
					"c": "red",
				}}, {{
					"c": "blue",
				}}, {{}}, {{
					"c": "yellow",
				}}, {{}},
			},
			M: map[string]interface{}{
				"op_3_project_0_exceptions_total":   int64(0),
				"op_3_project_0_process_latency_us": int64(0),
				"op_3_project_0_records_in_total":   int64(5),
				"op_3_project_0_records_out_total":  int64(5),

				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(5),
				"sink_memory_0_0_records_out_total": int64(5),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"op_2_window_0_exceptions_total":   int64(0),
				"op_2_window_0_process_latency_us": int64(0),
				"op_2_window_0_records_in_total":   int64(5),
				"op_2_window_0_records_out_total":  int64(5),
			},
		},
		{
			Name: `TestWindowRule11`,
			Sql:  `SELECT color, name, window_start(), window_end() FROM demo INNER JOIN table1 on demo.ts = table1.id where demo.size > 2 and table1.size > 1 GROUP BY tumblingwindow(ss, 1)`,
			R: [][]map[string]interface{}{
				{{
					"color":        "red",
					"name":         "name1",
					"window_start": int64(1541152486000),
					"window_end":   int64(1541152487000),
				}},
			},
			M: map[string]interface{}{
				"op_2_window_0_exceptions_total":  int64(0),
				"op_2_window_0_records_in_total":  int64(5),
				"op_2_window_0_records_out_total": int64(3),

				"op_6_join_aligner_0_records_in_total":  int64(6),
				"op_6_join_aligner_0_records_out_total": int64(3),

				"op_7_join_0_exceptions_total":  int64(0),
				"op_7_join_0_records_in_total":  int64(3),
				"op_7_join_0_records_out_total": int64(1),

				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(1),
				"sink_memory_0_0_records_out_total": int64(1),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"source_table1_0_exceptions_total":  int64(0),
				"source_table1_0_records_in_total":  int64(1),
				"source_table1_0_records_out_total": int64(1),
			},
		},
		{
			Name: `TestWindowRule12`,
			Sql:  `SELECT collect(size) as allSize FROM demo GROUP BY HOPPINGWINDOW(ss, 2, 1), color ORDER BY color`,
			R: [][]map[string]interface{}{
				{{
					"allSize": []interface{}{6},
				}, {
					"allSize": []interface{}{3},
				}},
				{{
					"allSize": []interface{}{6, 2},
				}, {
					"allSize": []interface{}{3},
				}},
				{{
					"allSize": []interface{}{2},
				}, {
					"allSize": []interface{}{4},
				}},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(3),
				"sink_memory_0_0_records_out_total": int64(3),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"op_2_window_0_exceptions_total":   int64(0),
				"op_2_window_0_process_latency_us": int64(0),
				"op_2_window_0_records_in_total":   int64(5),
				"op_2_window_0_records_out_total":  int64(3),
			},
		},
		{
			Name: `TestWindowRule13`,
			Sql:  `SELECT color as c FROM demo GROUP BY SlidingWindow(ss, 3600) filter (where size = 3 )`,
			R: [][]map[string]interface{}{
				{{
					"c": "red",
				}},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(1),
				"sink_memory_0_0_records_out_total": int64(1),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),
			},
		},
		{
			Name: `TestSlidingDelay`,
			Sql:  `SELECT size,color FROM demo GROUP BY SlidingWindow(ss, 5, 1) Over (when size = 2)`,
			R: [][]map[string]interface{}{
				{
					{
						"size":  3,
						"color": "red",
					},
					{
						"size":  6,
						"color": "blue",
					},
					{
						"size":  2,
						"color": "blue",
					},
					{
						"size":  4,
						"color": "yellow",
					},
				},
			},
			M: map[string]interface{}{},
		},
	}
	HandleStream(true, streamList, t)
	options := []*def.RuleOption{
		{
			BufferLength: 100,
			SendError:    true,
		},
		{
			BufferLength:       100,
			SendError:          true,
			Qos:                def.AtLeastOnce,
			CheckpointInterval: cast.DurationConf(5 * time.Second),
		},
		{
			BufferLength:       100,
			SendError:          true,
			Qos:                def.ExactlyOnce,
			CheckpointInterval: cast.DurationConf(5 * time.Second),
		},
	}
	for _, opt := range options {
		DoRuleTest(t, tests, opt, 15)
	}

	v2Opt := &def.RuleOption{
		BufferLength: 100,
		SendError:    true,
		PlanOptimizeStrategy: &def.PlanOptimizeStrategy{
			WindowOption: &def.WindowOption{
				WindowVersion: "v2",
			},
		},
	}
	for _, tt := range tests {
		if strings.Contains(strings.ToLower(tt.Sql), "slidingwindow") {
			tts := []RuleTest{tt}
			DoRuleTest(t, tts, v2Opt, 15)
		}
	}
}

func TestEventWindow(t *testing.T) {
	// Reset
	streamList := []string{"demoE", "demoErr", "demo1E", "sessionDemoE", "demoE2", "demoE3"}
	HandleStream(false, streamList, t)
	tests := []RuleTest{
		{
			Name: `TestEventWindowDelayRule0`,
			Sql:  `SELECT size FROM demoE GROUP BY SlidingWindow(ss, 1,4) FILTER (where color = "red")`,
			R: [][]map[string]interface{}{
				{
					{
						"size": 3,
					},
					{
						"size": 1,
					},
				},
			},
			M: map[string]interface{}{
				"op_3_watermark_0_records_in_total":  int64(6),
				"op_3_watermark_0_records_out_total": int64(4),
				"op_3_watermark_0_exceptions_total":  int64(0),

				"op_4_windowFilter_0_records_in_total":  int64(4),
				"op_4_windowFilter_0_records_out_total": int64(2),
				"op_4_windowFilter_0_exceptions_total":  int64(0),

				"op_4_window_0_records_in_total":  int64(2),
				"op_4_window_0_records_out_total": int64(1),
				"op_4_window_0_exceptions_total":  int64(0),
			},
		},
		{
			Name: `TestEventWindowRule1`,
			Sql:  `SELECT count(*), last_agg_hit_time() as lt, last_agg_hit_count() as lc, event_time() as et FROM demoE GROUP BY HOPPINGWINDOW(ss, 2, 1) HAVING lc < 4`,
			R: [][]map[string]interface{}{
				{{
					"count": 1,
					"lc":    0,
					"lt":    0,
					"et":    int64(1541152487000),
				}},
				{{
					"count": 2,
					"lc":    1,
					"lt":    int64(1541152487000),
					"et":    int64(1541152488000),
				}},
				{{
					"count": 2,
					"lc":    2,
					"lt":    int64(1541152488000),
					"et":    int64(1541152489000),
				}},
				{{
					"count": 2,
					"lc":    3,
					"lt":    int64(1541152489000),
					"et":    int64(1541152490000),
				}},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(4),
				"sink_memory_0_0_records_out_total": int64(4),

				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(6),
				"source_demoE_0_records_out_total": int64(6),

				"op_4_window_0_exceptions_total":   int64(0),
				"op_4_window_0_process_latency_us": int64(0),
				"op_4_window_0_records_in_total":   int64(4),
				"op_4_window_0_records_out_total":  int64(5),

				"op_3_watermark_0_records_in_total":  int64(6),
				"op_3_watermark_0_records_out_total": int64(4),
			},
		},
		{
			Name: `TestEventWindowRule0`,
			Sql:  `SELECT count(*), event_time() as et FROM demoE GROUP BY HOPPINGWINDOW(ss, 2, 1) HAVING last_value(et, true) - last_agg_hit_time() > 1500`,
			R: [][]map[string]interface{}{
				{{
					"count": 1,
					"et":    int64(1541152487000),
				}},
				{{
					"count": 2,
					"et":    int64(1541152490000),
				}},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(2),
				"sink_memory_0_0_records_out_total": int64(2),

				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(6),
				"source_demoE_0_records_out_total": int64(6),

				"op_4_window_0_exceptions_total":   int64(0),
				"op_4_window_0_process_latency_us": int64(0),
				"op_4_window_0_records_in_total":   int64(4),
				"op_4_window_0_records_out_total":  int64(5),

				"op_3_watermark_0_records_in_total":  int64(6),
				"op_3_watermark_0_records_out_total": int64(4),
			},
		},
		{
			Name: `TestEventWindowRule2`,
			Sql:  `SELECT window_start(), window_end(), color, ts FROM demoE where size > 2 GROUP BY tumblingwindow(ss, 1)`,
			R: [][]map[string]interface{}{
				{{
					"window_start": int64(1541152486013),
					"window_end":   int64(1541152487000),
					"color":        "red",
					"ts":           1541152486013,
				}},
				{{
					"window_start": int64(1541152488000),
					"window_end":   int64(1541152489000),
					"color":        "yellow",
					"ts":           1541152488442,
				}},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(2),
				"sink_memory_0_0_records_out_total": int64(2),

				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(6),
				"source_demoE_0_records_out_total": int64(6),

				"op_4_window_0_exceptions_total":  int64(0),
				"op_4_window_0_records_in_total":  int64(4),
				"op_4_window_0_records_out_total": int64(5),

				"op_5_filter_0_exceptions_total":  int64(0),
				"op_5_filter_0_records_in_total":  int64(5),
				"op_5_filter_0_records_out_total": int64(2),
			},
		},
		{
			Name: `TestEventWindowRule3`,
			Sql:  `SELECT color, temp, demoE.ts FROM demoE INNER JOIN demo1E ON demoE.ts = demo1E.ts GROUP BY SlidingWindow(ss, 1) OVER (WHEN demoE.ts - last_hit_time() > 400 or demo1E.ts - last_hit_time() > 400)`,
			R: [][]map[string]interface{}{
				{
					{
						"color": "red",
						"temp":  25.5,
						"ts":    1541152486013,
					},
				},
				{
					{
						"color": "red",
						"temp":  25.5,
						"ts":    1541152486013,
					},
				},
				{
					{
						"color": "blue",
						"temp":  28.1,
						"ts":    1541152487632,
					},
				},
				{
					{
						"color": "blue",
						"temp":  28.1,
						"ts":    1541152487632,
					},
					{
						"color": "yellow",
						"temp":  27.4,
						"ts":    1541152488442,
					},
				},
				{
					{
						"color": "yellow",
						"temp":  27.4,
						"ts":    1541152488442,
					},
					{
						"color": "red",
						"temp":  25.5,
						"ts":    1541152489252,
					},
				},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(5),
				"sink_memory_0_0_records_out_total": int64(5),

				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(6),
				"source_demoE_0_records_out_total": int64(6),

				"source_demo1E_0_exceptions_total":  int64(0),
				"source_demo1E_0_records_in_total":  int64(6),
				"source_demo1E_0_records_out_total": int64(6),

				"op_6_window_0_records_in_total":  int64(9),
				"op_6_window_0_records_out_total": int64(5),
			},
		},
		{
			Name: `TestEventWindowRule4`,
			Sql:  `SELECT  window_start() as ws, color, window_end() as we FROM demoE GROUP BY SlidingWindow(ss, 2) OVER (WHEN ts - last_hit_time() > 1000), color ORDER BY color`,
			R: [][]map[string]interface{}{
				{{
					"color": "red",
					"ws":    int64(1541152484013),
					"we":    int64(1541152486013),
				}}, {{
					"color": "blue",
					"ws":    int64(1541152485632),
					"we":    int64(1541152487632),
				}, {
					"color": "red",
					"ws":    int64(1541152485632),
					"we":    int64(1541152487632),
				}}, {{
					"color": "blue",
					"ws":    int64(1541152487252),
					"we":    int64(1541152489252),
				}, {
					"color": "red",
					"ws":    int64(1541152487252),
					"we":    int64(1541152489252),
				}, {
					"color": "yellow",
					"ws":    int64(1541152487252),
					"we":    int64(1541152489252),
				}},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(3),
				"sink_memory_0_0_records_out_total": int64(3),

				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(6),
				"source_demoE_0_records_out_total": int64(6),

				"op_4_window_0_records_in_total":  int64(4),
				"op_4_window_0_records_out_total": int64(3),
			},
		},
		{
			Name: `TestEventWindowRule5`,
			Sql:  `SELECT temp FROM sessionDemoE GROUP BY SessionWindow(ss, 2, 1) `,
			R: [][]map[string]interface{}{
				{{
					"temp": 25.5,
				}}, {{
					"temp": 28.1,
				}, {
					"temp": 27.4,
				}, {
					"temp": 25.5,
				}}, {{
					"temp": 26.2,
				}, {
					"temp": 26.8,
				}, {
					"temp": 28.9,
				}, {
					"temp": 29.1,
				}, {
					"temp": 32.2,
				}}, {{
					"temp": 30.9,
				}},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(4),
				"sink_memory_0_0_records_out_total": int64(4),

				"source_sessionDemoE_0_exceptions_total":  int64(0),
				"source_sessionDemoE_0_records_in_total":  int64(12),
				"source_sessionDemoE_0_records_out_total": int64(12),

				"op_4_window_0_records_in_total":  int64(10),
				"op_4_window_0_records_out_total": int64(4),
			},
		},
		{
			Name: `TestEventWindowRule6`,
			Sql:  `SELECT max(temp) as m, count(color) as c FROM demoE INNER JOIN demo1E ON demoE.ts = demo1E.ts GROUP BY SlidingWindow(ss, 1)`,
			R: [][]map[string]interface{}{
				{
					{
						"m": 25.5,
						"c": 1,
					},
				},
				{
					{
						"m": 25.5,
						"c": 1,
					},
				},
				{
					{
						"m": 25.5,
						"c": 1,
					},
				},
				{
					{
						"m": 28.1,
						"c": 1,
					},
				},
				{
					{
						"m": 28.1,
						"c": 1,
					},
				},
				{
					{
						"m": 28.1,
						"c": 2,
					},
				},
				{
					{
						"m": 28.1,
						"c": 2,
					},
				},
				{
					{
						"m": 27.4,
						"c": 2,
					},
				},
				{
					{
						"m": 27.4,
						"c": 2,
					},
				},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(9),
				"sink_memory_0_0_records_out_total": int64(9),

				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(6),
				"source_demoE_0_records_out_total": int64(6),

				"source_demo1E_0_exceptions_total":  int64(0),
				"source_demo1E_0_records_in_total":  int64(6),
				"source_demo1E_0_records_out_total": int64(6),

				"op_6_window_0_records_in_total":  int64(9),
				"op_6_window_0_records_out_total": int64(9),
			},
		},
		{
			Name: `TestEventWindowRule7`,
			Sql:  `SELECT * FROM demoErr GROUP BY HOPPINGWINDOW(ss, 2, 1)`,
			R: [][]map[string]interface{}{
				{{
					"error": "error in preprocessor: field color type mismatch: cannot convert int(2) to string",
				}},
				{{
					"color": "red",
					"size":  int64(3),
					"ts":    int64(1541152486013),
				}},
				{{
					"color": "red",
					"size":  int64(3),
					"ts":    int64(1541152486013),
				}},
				{{
					"color": "yellow",
					"size":  int64(4),
					"ts":    int64(1541152488442),
				}},
				{{
					"color": "yellow",
					"size":  int64(4),
					"ts":    int64(1541152488442),
				}, {
					"color": "red",
					"size":  int64(1),
					"ts":    int64(1541152489252),
				}},
				{{
					"color": "red",
					"size":  int64(1),
					"ts":    int64(1541152489252),
				}},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(6),
				"sink_memory_0_0_records_out_total": int64(6),

				"source_demoErr_0_exceptions_total":  int64(0),
				"source_demoErr_0_records_in_total":  int64(6),
				"source_demoErr_0_records_out_total": int64(6),

				"op_4_window_0_exceptions_total":  int64(0),
				"op_4_window_0_records_in_total":  int64(3),
				"op_4_window_0_records_out_total": int64(5),
			},
		},
		{
			Name: `TestEventWindowRule8`,
			Sql:  `SELECT temp, window_start(), window_end() FROM sessionDemoE GROUP BY SessionWindow(ss, 2, 1) `,
			R: [][]map[string]interface{}{
				{{
					"temp":         25.5,
					"window_start": int64(1541152486013),
					"window_end":   int64(1541152487013),
				}}, {{
					"temp":         28.1,
					"window_start": int64(1541152487932),
					"window_end":   int64(1541152490000),
				}, {
					"temp":         27.4,
					"window_start": int64(1541152487932),
					"window_end":   int64(1541152490000),
				}, {
					"temp":         25.5,
					"window_start": int64(1541152487932),
					"window_end":   int64(1541152490000),
				}}, {{
					"temp":         26.2,
					"window_start": int64(1541152490000),
					"window_end":   int64(1541152494000),
				}, {
					"temp":         26.8,
					"window_start": int64(1541152490000),
					"window_end":   int64(1541152494000),
				}, {
					"temp":         28.9,
					"window_start": int64(1541152490000),
					"window_end":   int64(1541152494000),
				}, {
					"temp":         29.1,
					"window_start": int64(1541152490000),
					"window_end":   int64(1541152494000),
				}, {
					"temp":         32.2,
					"window_start": int64(1541152490000),
					"window_end":   int64(1541152494000),
				}}, {{
					"temp":         30.9,
					"window_start": int64(1541152494000),
					"window_end":   int64(1541152495112),
				}},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(4),
				"sink_memory_0_0_records_out_total": int64(4),

				"source_sessionDemoE_0_exceptions_total":  int64(0),
				"source_sessionDemoE_0_records_in_total":  int64(12),
				"source_sessionDemoE_0_records_out_total": int64(12),

				"op_4_window_0_records_in_total":  int64(10),
				"op_4_window_0_records_out_total": int64(4),
			},
		},
		{
			Name: `TestEventWindowRule9`,
			Sql:  `SELECT window_end(), color, window_start() FROM demoE GROUP BY HOPPINGWINDOW(ss, 2, 1)`,
			R: [][]map[string]interface{}{
				{{
					"color":        "red",
					"window_start": int64(1541152485013),
					"window_end":   int64(1541152487000),
				}},
				{{
					"color":        "red",
					"window_start": int64(1541152486000),
					"window_end":   int64(1541152488000),
				}, {
					"color":        "blue",
					"window_start": int64(1541152486000),
					"window_end":   int64(1541152488000),
				}},
				{{
					"color":        "blue",
					"window_start": int64(1541152487000),
					"window_end":   int64(1541152489000),
				}, {
					"color":        "yellow",
					"window_start": int64(1541152487000),
					"window_end":   int64(1541152489000),
				}},
				{{
					"color":        "yellow",
					"window_start": int64(1541152488000),
					"window_end":   int64(1541152490000),
				}, {
					"color":        "red",
					"window_start": int64(1541152488000),
					"window_end":   int64(1541152490000),
				}},
				{{
					"color":        "red",
					"window_start": int64(1541152489000),
					"window_end":   int64(1541152491000),
				}},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(5),
				"sink_memory_0_0_records_out_total": int64(5),

				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(6),
				"source_demoE_0_records_out_total": int64(6),
			},
		},
		{
			Name: `TestEventWindowCondition10`,
			Sql:  `SELECT color FROM demoE GROUP BY SlidingWindow(ss, 1) Over (When size = 3)`,
			R: [][]map[string]interface{}{
				{
					{
						"color": "red",
					},
				},
			},
			M: map[string]interface{}{
				"op_3_watermark_0_records_in_total":  int64(6),
				"op_3_watermark_0_records_out_total": int64(4),
				"op_3_watermark_0_exceptions_total":  int64(0),

				"op_4_window_0_records_in_total":  int64(4),
				"op_4_window_0_records_out_total": int64(1),
				"op_4_window_0_exceptions_total":  int64(0),
			},
		},
		{
			Name: `TestSlidingWindowInterval11`,
			Sql:  `SELECT temp FROM demoE2 GROUP BY SLIDINGWINDOW(ss, 1, 1)`,
			R: [][]map[string]interface{}{
				{
					{
						"temp": 27.5,
					},
				},
			},
			M: map[string]interface{}{
				"source_demoE2_0_records_in_total":   int64(3),
				"source_demoE2_0_records_out_total":  int64(3),
				"op_3_watermark_0_records_in_total":  int64(3),
				"op_3_watermark_0_records_out_total": int64(2),
				"op_4_window_0_records_in_total":     int64(2),
				"op_4_window_0_records_out_total":    int64(1),
				"sink_memory_0_0_records_in_total":   int64(1),
				"sink_memory_0_0_records_out_total":  int64(1),
			},
		},
		{
			Name: `TestSlidingWindowInterval12`,
			Sql:  `SELECT temp FROM demoE2 GROUP BY SLIDINGWINDOW(ss, 1)`,
			R: [][]map[string]interface{}{
				{
					{
						"temp": 27.5,
					},
				},
				{
					{
						"temp": 27.5,
					},
					{
						"temp": 25.5,
					},
				},
			},
			M: map[string]interface{}{
				"source_demoE2_0_records_in_total":   int64(3),
				"source_demoE2_0_records_out_total":  int64(3),
				"op_3_watermark_0_records_in_total":  int64(3),
				"op_3_watermark_0_records_out_total": int64(2),
				"op_4_window_0_records_in_total":     int64(2),
				"op_4_window_0_records_out_total":    int64(2),
				"sink_memory_0_0_records_in_total":   int64(2),
				"sink_memory_0_0_records_out_total":  int64(2),
			},
		},
		{
			Name: `TestTUMBLINGWindowInterval13`,
			Sql:  `SELECT temp FROM demoE2 GROUP BY TUMBLINGWINDOW(ss, 1)`,
			R: [][]map[string]interface{}{
				{
					{
						"temp": 27.5,
					},
				},
			},
			M: map[string]interface{}{
				"source_demoE2_0_records_in_total":   int64(3),
				"source_demoE2_0_records_out_total":  int64(3),
				"op_3_watermark_0_records_in_total":  int64(3),
				"op_3_watermark_0_records_out_total": int64(2),
				"op_4_window_0_records_in_total":     int64(2),
				"op_4_window_0_records_out_total":    int64(1),
				"sink_memory_0_0_records_in_total":   int64(1),
				"sink_memory_0_0_records_out_total":  int64(1),
			},
		},
		{
			Name: `TestTUMBLINGWindowInterval14`,
			Sql:  `SELECT temp,ts FROM demoE3 GROUP BY TUMBLINGWINDOW(ss, 1)`,
			R: [][]map[string]interface{}{
				{
					{
						"temp": 26.0,
						"ts":   1541152486000,
					},
				},
				{
					{
						"temp": 27.0,
						"ts":   1541152487000,
					},
				},
			},
		},
	}
	HandleStream(true, streamList, t)
	options := []*def.RuleOption{
		{
			BufferLength: 100,
			SendError:    true,
			IsEventTime:  true,
			LateTol:      cast.DurationConf(time.Second),
		},
		{
			BufferLength:       100,
			SendError:          true,
			Qos:                def.AtLeastOnce,
			CheckpointInterval: cast.DurationConf(5 * time.Second),
			IsEventTime:        true,
			LateTol:            cast.DurationConf(time.Second),
		},
		{
			BufferLength:       100,
			SendError:          true,
			Qos:                def.ExactlyOnce,
			CheckpointInterval: cast.DurationConf(5 * time.Second),
			IsEventTime:        true,
			LateTol:            cast.DurationConf(time.Second),
		},
	}
	for _, opt := range options {
		DoRuleTest(t, tests, opt, 10)
	}
}

func TestWindowError(t *testing.T) {
	// Reset
	streamList := []string{"ldemo", "ldemo1"}
	HandleStream(false, streamList, t)
	tests := []RuleTest{
		{
			Name: `TestWindowErrorRule1`,
			Sql:  `SELECT size * 3 FROM ldemo GROUP BY TUMBLINGWINDOW(ss, 2)`,
			R: [][]map[string]interface{}{
				{{
					"error": "run Select error: expr: binaryExpr:{ ldemo.size * 3 } meet error, err:invalid operation string(string) * int64(3)",
				}},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(1),
				"sink_memory_0_0_records_out_total": int64(1),

				"source_ldemo_0_exceptions_total":  int64(0),
				"source_ldemo_0_records_in_total":  int64(5),
				"source_ldemo_0_records_out_total": int64(5),

				"op_2_window_0_records_in_total":  int64(5),
				"op_2_window_0_records_out_total": int64(1),
			},
		}, {
			Name: `TestWindowErrorRule2`,
			Sql:  `SELECT color, ts FROM ldemo where size > 2 GROUP BY tumblingwindow(ss, 1)`,
			R: [][]map[string]interface{}{
				{{
					"error": "run Where error: invalid operation string(string) > int64(2)",
				}}, {{
					"color": "red",
					"ts":    1541152486013,
				}}, {{
					"ts": 1541152487632,
				}}, {},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(4),
				"sink_memory_0_0_records_out_total": int64(4),

				"source_ldemo_0_exceptions_total":  int64(0),
				"source_ldemo_0_records_in_total":  int64(5),
				"source_ldemo_0_records_out_total": int64(5),
			},
		}, {
			Name: `TestWindowErrorRule3`,
			Sql:  `SELECT color, temp, ts FROM ldemo INNER JOIN ldemo1 ON ldemo.ts = ldemo1.ts GROUP BY SlidingWindow(ss, 1)`,
			R: [][]map[string]interface{}{
				{{
					"color": "red",
					"temp":  25.5,
					"ts":    1541152486013,
				}}, {{
					"color": "red",
					"temp":  25.5,
					"ts":    1541152486013,
				}}, {{
					"color": "red",
					"temp":  25.5,
					"ts":    1541152486013,
				}}, {{
					"temp": 28.1,
					"ts":   1541152487632,
				}}, {{
					"temp": 28.1,
					"ts":   1541152487632,
				}}, {{
					"error": "run Join error: invalid operation int64(1541152487632) = string(1541152488442)",
				}}, {{
					"error": "run Join error: invalid operation int64(1541152488442) = string(1541152488442)",
				}}, {{
					"error": "run Join error: invalid operation int64(1541152488442) = string(1541152488442)",
				}},
			},
			M: map[string]interface{}{
				"op_5_project_0_exceptions_total":   int64(0),
				"op_5_project_0_process_latency_us": int64(0),
				"op_5_project_0_records_in_total":   int64(5),
				"op_5_project_0_records_out_total":  int64(5),

				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(8),
				"sink_memory_0_0_records_out_total": int64(8),

				"source_ldemo_0_exceptions_total":  int64(0),
				"source_ldemo_0_records_in_total":  int64(5),
				"source_ldemo_0_records_out_total": int64(5),

				"source_ldemo1_0_exceptions_total":  int64(0),
				"source_ldemo1_0_records_in_total":  int64(5),
				"source_ldemo1_0_records_out_total": int64(5),

				"op_3_window_0_exceptions_total":   int64(0),
				"op_3_window_0_process_latency_us": int64(0),
				"op_3_window_0_records_in_total":   int64(10),
				"op_3_window_0_records_out_total":  int64(10),

				"op_4_join_0_exceptions_total":   int64(3),
				"op_4_join_0_process_latency_us": int64(0),
				"op_4_join_0_records_in_total":   int64(10),
				"op_4_join_0_records_out_total":  int64(5),
			},
		}, {
			Name: `TestWindowErrorRule4`,
			Sql:  `SELECT color FROM ldemo GROUP BY SlidingWindow(ss, 2), color having collect(size)[0] >= 2 order by color`,
			R: [][]map[string]interface{}{
				{{
					"color": "red",
				}}, {{
					"error": "run Having error: invalid operation string(string) >= int64(2)",
				}}, {{
					"error": "run Having error: invalid operation string(string) >= int64(2)",
				}}, {{
					"error": "run Having error: invalid operation string(string) >= int64(2)",
				}}, {{
					"color": 49,
				}, {}},
			},
			M: map[string]interface{}{
				"op_6_project_0_exceptions_total":   int64(0),
				"op_6_project_0_process_latency_us": int64(0),
				"op_6_project_0_records_in_total":   int64(2),
				"op_6_project_0_records_out_total":  int64(2),

				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(5),
				"sink_memory_0_0_records_out_total": int64(5),

				"source_ldemo_0_exceptions_total":  int64(0),
				"source_ldemo_0_records_in_total":  int64(5),
				"source_ldemo_0_records_out_total": int64(5),

				"op_2_window_0_exceptions_total":   int64(0),
				"op_2_window_0_process_latency_us": int64(0),
				"op_2_window_0_records_in_total":   int64(5),
				"op_2_window_0_records_out_total":  int64(5),

				"op_3_aggregate_0_exceptions_total":   int64(0),
				"op_3_aggregate_0_process_latency_us": int64(0),
				"op_3_aggregate_0_records_in_total":   int64(5),
				"op_3_aggregate_0_records_out_total":  int64(5),

				"op_4_having_0_exceptions_total":   int64(3),
				"op_4_having_0_process_latency_us": int64(0),
				"op_4_having_0_records_in_total":   int64(5),
				"op_4_having_0_records_out_total":  int64(2),
			},
		}, {
			Name: `TestWindowErrorRule5`,
			Sql:  `SELECT color, size FROM ldemo GROUP BY tumblingwindow(ss, 1) ORDER BY size`,
			R: [][]map[string]interface{}{
				{{
					"error": "run Order By error: incompatible types for comparison: int and string",
				}}, {{
					"size": 3,
				}}, {{
					"color": 49,
					"size":  2,
				}},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(3),
				"sink_memory_0_0_records_out_total": int64(3),

				"source_ldemo_0_exceptions_total":  int64(0),
				"source_ldemo_0_records_in_total":  int64(5),
				"source_ldemo_0_records_out_total": int64(5),
			},
		},
	}
	HandleStream(true, streamList, t)
	DoRuleTest(t, tests, &def.RuleOption{
		BufferLength: 100,
		SendError:    true,
	}, 0)
}

func TestEventSlidingWindow(t *testing.T) {
	// Reset
	streamList := []string{"demoE", "demoErr", "demo1E", "sessionDemoE"}
	HandleStream(false, streamList, t)
	tests := []RuleTest{
		{
			Name: `TestEventWindowRuleDelay`,
			Sql:  `SELECT color  FROM demoE GROUP BY SlidingWindow(ss, 1,1) FILTER (where size = 3)`,
			R: [][]map[string]interface{}{
				{{
					"color": "red",
				}},
			},
			M: map[string]interface{}{
				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(6),
				"source_demoE_0_records_out_total": int64(6),
			},
		},
	}
	HandleStream(true, streamList, t)
	options := []*def.RuleOption{
		{
			BufferLength:       100,
			SendError:          true,
			Qos:                def.AtLeastOnce,
			CheckpointInterval: cast.DurationConf(5 * time.Second),
			IsEventTime:        true,
			LateTol:            cast.DurationConf(time.Second),
		},
		{
			BufferLength:       100,
			SendError:          true,
			Qos:                def.ExactlyOnce,
			CheckpointInterval: cast.DurationConf(5 * time.Second),
			IsEventTime:        true,
			LateTol:            cast.DurationConf(time.Second),
		},
		{
			BufferLength: 100,
			SendError:    true,
			IsEventTime:  true,
			LateTol:      cast.DurationConf(time.Second),
		},
	}
	for _, opt := range options {
		DoRuleTest(t, tests, opt, 10)
	}
}
