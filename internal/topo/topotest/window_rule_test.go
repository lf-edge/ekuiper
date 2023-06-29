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
	"testing"

	"github.com/lf-edge/ekuiper/pkg/api"
)

func TestWindow(t *testing.T) {
	// Reset
	streamList := []string{"demo", "demoError", "demo1", "sessionDemo", "table1"}
	HandleStream(false, streamList, t)
	tests := []RuleTest{
		{
			Name: `TestWindowRule0`,
			Sql:  `SELECT size,color FROM demo GROUP BY SlidingWindow(ss, 5) Filter (where color = "red") Over (when size = 1)`,
			R: [][]map[string]interface{}{
				{
					{
						"size":  float64(3),
						"color": "red",
					},
					{
						"size":  float64(1),
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
					"size":  float64(3),
					"ts":    float64(1541152486013),
					"et":    float64(1541152486013),
				}, {
					"color": "blue",
					"size":  float64(6),
					"ts":    float64(1541152486822),
					"et":    float64(1541152486822),
				}},
				{{
					"color": "red",
					"size":  float64(3),
					"ts":    float64(1541152486013),
					"et":    float64(1541152486013),
				}, {
					"color": "blue",
					"size":  float64(6),
					"ts":    float64(1541152486822),
					"et":    float64(1541152486822),
				}, {
					"color": "blue",
					"size":  float64(2),
					"ts":    float64(1541152487632),
					"et":    float64(1541152487632),
				}},
				{{
					"color": "blue",
					"size":  float64(2),
					"ts":    float64(1541152487632),
					"et":    float64(1541152487632),
				}, {
					"color": "yellow",
					"size":  float64(4),
					"ts":    float64(1541152488442),
					"et":    float64(1541152488442),
				}},
				{{
					"color": "yellow",
					"size":  float64(4),
					"ts":    float64(1541152488442),
					"et":    float64(1541152488442),
				}, {
					"color": "red",
					"size":  float64(1),
					"ts":    float64(1541152489252),
					"et":    float64(1541152489252),
				}},
			},
			M: map[string]interface{}{
				"op_3_project_0_exceptions_total":   int64(0),
				"op_3_project_0_process_latency_us": int64(0),
				"op_3_project_0_records_in_total":   int64(4),
				"op_3_project_0_records_out_total":  int64(4),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(4),
				"sink_mockSink_0_records_out_total": int64(4),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"op_2_window_0_exceptions_total":   int64(0),
				"op_2_window_0_process_latency_us": int64(0),
				"op_2_window_0_records_in_total":   int64(5),
				"op_2_window_0_records_out_total":  int64(4),
			},
		},
		{
			Name: `TestWindowRule2`,
			Sql:  `SELECT color, ts FROM demo where size > 2 GROUP BY tumblingwindow(ss, 1)`,
			R: [][]map[string]interface{}{
				{{
					"color": "red",
					"ts":    float64(1541152486013),
				}, {
					"color": "blue",
					"ts":    float64(1541152486822),
				}},
				{},
				{{
					"color": "yellow",
					"ts":    float64(1541152488442),
				}},
				{},
			},
			M: map[string]interface{}{
				"op_4_project_0_exceptions_total":   int64(0),
				"op_4_project_0_process_latency_us": int64(0),
				"op_4_project_0_records_in_total":   int64(4),
				"op_4_project_0_records_out_total":  int64(4),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(4),
				"sink_mockSink_0_records_out_total": int64(4),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"op_3_window_0_exceptions_total":   int64(0),
				"op_3_window_0_process_latency_us": int64(0),
				"op_3_window_0_records_in_total":   int64(3),
				"op_3_window_0_records_out_total":  int64(4),

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
					"ts1":   float64(1541152486013),
					"ts2":   float64(1541152486013),
					"diff":  float64(0),
				}}, {{
					"color": "red",
					"temp":  25.5,
					"ts1":   float64(1541152486013),
					"ts2":   float64(1541152486013),
					"diff":  float64(0),
				}}, {{
					"color": "red",
					"temp":  25.5,
					"ts1":   float64(1541152486013),
					"ts2":   float64(1541152486013),
					"diff":  float64(0),
				}}, {{
					"color": "blue",
					"temp":  28.1,
					"ts1":   float64(1541152487632),
					"ts2":   float64(1541152487632),
					"diff":  float64(0),
				}}, {{
					"color": "blue",
					"temp":  28.1,
					"ts1":   float64(1541152487632),
					"ts2":   float64(1541152487632),
					"diff":  float64(0),
				}}, {{
					"color": "blue",
					"temp":  28.1,
					"ts1":   float64(1541152487632),
					"ts2":   float64(1541152487632),
					"diff":  float64(0),
				}, {
					"color": "yellow",
					"temp":  27.4,
					"ts1":   float64(1541152488442),
					"ts2":   float64(1541152488442),
					"diff":  float64(0),
				}}, {{
					"color": "yellow",
					"temp":  27.4,
					"ts1":   float64(1541152488442),
					"ts2":   float64(1541152488442),
					"diff":  float64(0),
				}},
			},
			M: map[string]interface{}{
				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(7),
				"sink_mockSink_0_records_out_total": int64(7),

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
			T: &api.PrintableTopo{
				Sources: []string{"source_demo", "source_demo1"},
				Edges: map[string][]interface{}{
					"source_demo":  {"op_3_window"},
					"source_demo1": {"op_3_window"},
					"op_3_window":  {"op_4_join"},
					"op_4_join":    {"op_5_having"},
					"op_5_having":  {"op_6_project"},
					"op_6_project": {"sink_mockSink"},
				},
			},
		},
		{
			Name: `TestWindowRule4`,
			Sql:  `SELECT color, count(*) as c FROM demo GROUP BY SlidingWindow(ss, 2) OVER(WHEN ts - last_hit_time() > 1000) , color ORDER BY color`,
			R: [][]map[string]interface{}{
				{{
					"color": "red",
					"c":     float64(1),
				}}, {{
					"color": "blue",
					"c":     float64(2),
				}, {
					"color": "red",
					"c":     float64(1),
				}}, {{
					"color": "blue",
					"c":     float64(1),
				}, {
					"color": "red",
					"c":     float64(1),
				}, {
					"color": "yellow",
					"c":     float64(1),
				}},
			},
			M: map[string]interface{}{
				"op_5_project_0_exceptions_total":   int64(0),
				"op_5_project_0_process_latency_us": int64(0),
				"op_5_project_0_records_in_total":   int64(3),
				"op_5_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),

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
					"count":      float64(2),
					"ws":         float64(1541152486013),
					"window_end": float64(1541152487823), // timeout
					"et":         float64(1541152487823),
				}}, {{
					"count":      float64(3),
					"ws":         float64(1541152487932),
					"window_end": float64(1541152490000), // tick
					"et":         float64(1541152490000),
				}}, {{
					"count":      float64(5),
					"ws":         float64(1541152490000),
					"window_end": float64(1541152494000), // tick
					"et":         float64(1541152494000),
				}}, {{
					"count":      float64(1),
					"ws":         float64(1541152494000),
					"window_end": float64(1541152495112), // timeout
					"et":         float64(1541152495112),
				}},
			},
			M: map[string]interface{}{
				"op_3_project_0_exceptions_total":   int64(0),
				"op_3_project_0_process_latency_us": int64(0),
				"op_3_project_0_records_in_total":   int64(4),
				"op_3_project_0_records_out_total":  int64(4),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(4),
				"sink_mockSink_0_records_out_total": int64(4),

				"source_sessionDemo_0_exceptions_total":  int64(0),
				"source_sessionDemo_0_records_in_total":  int64(11),
				"source_sessionDemo_0_records_out_total": int64(11),

				"op_2_window_0_exceptions_total":   int64(0),
				"op_2_window_0_process_latency_us": int64(0),
				"op_2_window_0_records_in_total":   int64(11),
				"op_2_window_0_records_out_total":  int64(4),
			},
		},
		{
			Name: `TestWindowRule6`,
			Sql:  `SELECT window_end(), event_time(), sum(temp) as temp, count(color) as c, window_start() FROM demo INNER JOIN demo1 ON demo.ts = demo1.ts GROUP BY SlidingWindow(ss, 1)`,
			R: [][]map[string]interface{}{
				{{
					"temp":         25.5,
					"c":            float64(1),
					"window_start": float64(1541152485115),
					"window_end":   float64(1541152486115),
					"event_time":   float64(1541152486115),
				}}, {{
					"temp":         25.5,
					"c":            float64(1),
					"window_start": float64(1541152485822),
					"window_end":   float64(1541152486822),
					"event_time":   float64(1541152486822),
				}}, {{
					"temp":         25.5,
					"c":            float64(1),
					"window_start": float64(1541152485903),
					"window_end":   float64(1541152486903),
					"event_time":   float64(1541152486903),
				}}, {{
					"temp":         28.1,
					"c":            float64(1),
					"window_start": float64(1541152486702),
					"window_end":   float64(1541152487702),
					"event_time":   float64(1541152487702),
				}}, {{
					"temp":         28.1,
					"c":            float64(1),
					"window_start": float64(1541152487442),
					"window_end":   float64(1541152488442),
					"event_time":   float64(1541152488442),
				}}, {{
					"temp":         55.5,
					"c":            float64(2),
					"window_start": float64(1541152487605),
					"window_end":   float64(1541152488605),
					"event_time":   float64(1541152488605),
				}}, {{
					"temp":         27.4,
					"c":            float64(1),
					"window_start": float64(1541152488252),
					"window_end":   float64(1541152489252),
					"event_time":   float64(1541152489252),
				}}, {{
					"temp":         52.9,
					"c":            float64(2),
					"window_start": float64(1541152488305),
					"window_end":   float64(1541152489305),
					"event_time":   float64(1541152489305),
				}},
			},
			M: map[string]interface{}{
				"op_5_project_0_exceptions_total":   int64(0),
				"op_5_project_0_process_latency_us": int64(0),
				"op_5_project_0_records_in_total":   int64(8),
				"op_5_project_0_records_out_total":  int64(8),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(8),
				"sink_mockSink_0_records_out_total": int64(8),

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
					"size":  float64(6),
					"ts":    float64(1541152486822),
				}},
				{{
					"color": "blue",
					"size":  float64(6),
					"ts":    float64(1541152486822),
				}, {
					"color": "blue",
					"size":  float64(2),
					"ts":    float64(1541152487632),
				}},
				{{
					"error": "error in preprocessor: field color type mismatch: cannot convert int(7) to string",
				}},
				{{
					"color": "blue",
					"size":  float64(2),
					"ts":    float64(1541152487632),
				}},
				{{
					"error": "error in preprocessor: field size type mismatch: cannot convert string(blue) to int64",
				}},
				{},
			},
			M: map[string]interface{}{
				"op_3_project_0_exceptions_total":   int64(3),
				"op_3_project_0_process_latency_us": int64(0),
				"op_3_project_0_records_in_total":   int64(4),
				"op_3_project_0_records_out_total":  int64(4),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(7),
				"sink_mockSink_0_records_out_total": int64(7),

				"source_demoError_0_exceptions_total":  int64(3),
				"source_demoError_0_records_in_total":  int64(5),
				"source_demoError_0_records_out_total": int64(2),

				"op_2_window_0_exceptions_total":   int64(3),
				"op_2_window_0_process_latency_us": int64(0),
				"op_2_window_0_records_in_total":   int64(5),
				"op_2_window_0_records_out_total":  int64(4),
			},
		},
		{
			Name: `TestWindowRule8`,
			Sql:  `SELECT color, window_end(), event_time() as et, ts, count(*) as c, window_start() FROM demo where size > 2 GROUP BY tumblingwindow(ss, 1) having c > 1`,
			R: [][]map[string]interface{}{
				{{
					"color":        "red",
					"ts":           float64(1541152486013),
					"c":            float64(2),
					"window_start": float64(1541152486000),
					"window_end":   float64(1541152487000),
					"et":           float64(1541152487000),
				}},
			},
			M: map[string]interface{}{
				"op_5_project_0_exceptions_total":   int64(0),
				"op_5_project_0_process_latency_us": int64(0),
				"op_5_project_0_records_in_total":   int64(1),
				"op_5_project_0_records_out_total":  int64(1),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(1),
				"sink_mockSink_0_records_out_total": int64(1),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"op_3_window_0_exceptions_total":   int64(0),
				"op_3_window_0_process_latency_us": int64(0),
				"op_3_window_0_records_in_total":   int64(3),
				"op_3_window_0_records_out_total":  int64(4),

				"op_2_filter_0_exceptions_total":   int64(0),
				"op_2_filter_0_process_latency_us": int64(0),
				"op_2_filter_0_records_in_total":   int64(5),
				"op_2_filter_0_records_out_total":  int64(3),

				"op_4_having_0_exceptions_total":   int64(0),
				"op_4_having_0_process_latency_us": int64(0),
				"op_4_having_0_records_in_total":   int64(4),
				"op_4_having_0_records_out_total":  int64(1),
			},
		},
		{
			Name: `TestWindowRule9`,
			Sql:  `SELECT color, window_start(), window_end() FROM demo GROUP BY HOPPINGWINDOW(ss, 2, 1) FILTER( WHERE size > 2)`,
			R: [][]map[string]interface{}{
				{{
					"color":        "red",
					"window_start": float64(1541152485000),
					"window_end":   float64(1541152487000),
				}, {
					"color":        "blue",
					"window_start": float64(1541152485000),
					"window_end":   float64(1541152487000),
				}},
				{{
					"color":        "red",
					"window_start": float64(1541152486000),
					"window_end":   float64(1541152488000),
				}, {
					"color":        "blue",
					"window_start": float64(1541152486000),
					"window_end":   float64(1541152488000),
				}},
				{{
					"color":        "yellow",
					"window_start": float64(1541152487000),
					"window_end":   float64(1541152489000),
				}},
				{{
					"color":        "yellow",
					"window_start": float64(1541152488000),
					"window_end":   float64(1541152490000),
				}},
			},
			M: map[string]interface{}{
				"op_4_project_0_exceptions_total":   int64(0),
				"op_4_project_0_process_latency_us": int64(0),
				"op_4_project_0_records_in_total":   int64(4),
				"op_4_project_0_records_out_total":  int64(4),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(4),
				"sink_mockSink_0_records_out_total": int64(4),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"op_3_window_0_exceptions_total":   int64(0),
				"op_3_window_0_process_latency_us": int64(0),
				"op_3_window_0_records_in_total":   int64(3),
				"op_3_window_0_records_out_total":  int64(4),
			},
		},
		{
			Name: `TestCountWindowRule1`,
			Sql:  `SELECT collect(*)[0]->color as c, window_end() as we FROM demo GROUP BY COUNTWINDOW(3)`,
			R: [][]map[string]interface{}{
				{{
					"c":  "red",
					"we": 1.541152487632e+12,
				}},
			},
			M: map[string]interface{}{
				"op_3_project_0_exceptions_total":   int64(0),
				"op_3_project_0_process_latency_us": int64(0),
				"op_3_project_0_records_in_total":   int64(1),
				"op_3_project_0_records_out_total":  int64(1),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(1),
				"sink_mockSink_0_records_out_total": int64(1),

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

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(5),
				"sink_mockSink_0_records_out_total": int64(5),

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
					"window_start": float64(1541152486000),
					"window_end":   float64(1541152487000),
				}},
			},
			M: map[string]interface{}{
				"op_2_window_0_exceptions_total":   int64(0),
				"op_2_window_0_process_latency_us": int64(0),
				"op_2_window_0_records_in_total":   int64(5),
				"op_2_window_0_records_out_total":  int64(4),

				"op_4_join_aligner_0_records_in_total":  int64(5),
				"op_4_join_aligner_0_records_out_total": int64(4),

				"op_5_join_0_exceptions_total":  int64(0),
				"op_5_join_0_records_in_total":  int64(4),
				"op_5_join_0_records_out_total": int64(1),

				"op_6_project_0_exceptions_total":  int64(0),
				"op_6_project_0_records_in_total":  int64(1),
				"op_6_project_0_records_out_total": int64(1),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(1),
				"sink_mockSink_0_records_out_total": int64(1),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"source_table1_0_exceptions_total":  int64(0),
				"source_table1_0_records_in_total":  int64(4),
				"source_table1_0_records_out_total": int64(1),
			},
		},
		{
			Name: `TestWindowRule12`,
			Sql:  `SELECT collect(size) as allSize FROM demo GROUP BY HOPPINGWINDOW(ss, 2, 1), color ORDER BY color`,
			R: [][]map[string]interface{}{
				{{
					"allSize": []interface{}{float64(6)},
				}, {
					"allSize": []interface{}{float64(3)},
				}},
				{{
					"allSize": []interface{}{float64(6), float64(2)},
				}, {
					"allSize": []interface{}{float64(3)},
				}},
				{{
					"allSize": []interface{}{float64(2)},
				}, {
					"allSize": []interface{}{float64(4)},
				}},
				{{
					"allSize": []interface{}{float64(1)},
				}, {
					"allSize": []interface{}{float64(4)},
				}},
			},
			M: map[string]interface{}{
				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(4),
				"sink_mockSink_0_records_out_total": int64(4),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"op_2_window_0_exceptions_total":   int64(0),
				"op_2_window_0_process_latency_us": int64(0),
				"op_2_window_0_records_in_total":   int64(5),
				"op_2_window_0_records_out_total":  int64(4),
			},
		},
		{
			Name: `TestWindowRule13`,
			Sql:  `SELECT color as c FROM demo GROUP BY SlidingWindow(ss, 3600,1) filter (where size = 3 )`,
			R: [][]map[string]interface{}{
				{{
					"c": "red",
				}},
			},
			M: map[string]interface{}{
				"op_3_project_0_exceptions_total":   int64(0),
				"op_3_project_0_process_latency_us": int64(0),
				"op_3_project_0_records_in_total":   int64(1),
				"op_3_project_0_records_out_total":  int64(1),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(1),
				"sink_mockSink_0_records_out_total": int64(1),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"op_2_window_0_exceptions_total":   int64(0),
				"op_2_window_0_process_latency_us": int64(0),
				"op_2_window_0_records_in_total":   int64(1),
				"op_2_window_0_records_out_total":  int64(1),
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
		DoRuleTest(t, tests, j, opt, 15)
	}
}

func TestEventWindow(t *testing.T) {
	// Reset
	streamList := []string{"demoE", "demoErr", "demo1E", "sessionDemoE"}
	HandleStream(false, streamList, t)
	tests := []RuleTest{
		{
			Name: `TestEventWindowDelayRule0`,
			Sql:  `SELECT size FROM demoE GROUP BY SlidingWindow(ss, 1,4) FILTER (where color = "red")`,
			R: [][]map[string]interface{}{
				{
					{
						"size": float64(3),
					},
					{
						"size": float64(1),
					},
				},
			},
			M: map[string]interface{}{
				"op_2_watermark_0_records_in_total":  int64(6),
				"op_2_watermark_0_records_out_total": int64(4),
				"op_2_watermark_0_exceptions_total":  int64(0),

				"op_3_windowFilter_0_records_in_total":  int64(4),
				"op_3_windowFilter_0_records_out_total": int64(2),
				"op_3_windowFilter_0_exceptions_total":  int64(0),

				"op_3_window_0_records_in_total":  int64(2),
				"op_3_window_0_records_out_total": int64(1),
				"op_3_window_0_exceptions_total":  int64(0),
			},
		},
		{
			Name: `TestEventWindowRule1`,
			Sql:  `SELECT count(*), last_agg_hit_time() as lt, last_agg_hit_count() as lc, event_time() as et FROM demoE GROUP BY HOPPINGWINDOW(ss, 2, 1) HAVING lc < 4`,
			R: [][]map[string]interface{}{
				{{
					"count": float64(1),
					"lc":    float64(0),
					"lt":    float64(0),
					"et":    float64(1541152487000),
				}},
				{{
					"count": float64(2),
					"lc":    float64(1),
					"lt":    float64(1541152487000),
					"et":    float64(1541152488000),
				}},
				{{
					"count": float64(2),
					"lc":    float64(2),
					"lt":    float64(1541152488000),
					"et":    float64(1541152489000),
				}},
				{{
					"count": float64(2),
					"lc":    float64(3),
					"lt":    float64(1541152489000),
					"et":    float64(1541152490000),
				}},
			},
			M: map[string]interface{}{
				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(4),
				"sink_mockSink_0_records_out_total": int64(4),

				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(6),
				"source_demoE_0_records_out_total": int64(6),

				"op_3_window_0_exceptions_total":   int64(0),
				"op_3_window_0_process_latency_us": int64(0),
				"op_3_window_0_records_in_total":   int64(4),
				"op_3_window_0_records_out_total":  int64(5),

				"op_2_watermark_0_records_in_total":  int64(6),
				"op_2_watermark_0_records_out_total": int64(4),
			},
		},
		{
			Name: `TestEventWindowRule2`,
			Sql:  `SELECT window_start(), window_end(), color, ts FROM demoE where size > 2 GROUP BY tumblingwindow(ss, 1)`,
			R: [][]map[string]interface{}{
				{{
					"window_start": float64(1541152486013),
					"window_end":   float64(1541152487000),
					"color":        "red",
					"ts":           float64(1541152486013),
				}},
				{{
					"window_start": float64(1541152488000),
					"window_end":   float64(1541152489000),
					"color":        "yellow",
					"ts":           float64(1541152488442),
				}},
			},
			M: map[string]interface{}{
				"op_5_project_0_exceptions_total":   int64(0),
				"op_5_project_0_process_latency_us": int64(0),
				"op_5_project_0_records_in_total":   int64(2),
				"op_5_project_0_records_out_total":  int64(2),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(2),
				"sink_mockSink_0_records_out_total": int64(2),

				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(6),
				"source_demoE_0_records_out_total": int64(6),

				"op_3_window_0_exceptions_total":   int64(0),
				"op_3_window_0_process_latency_us": int64(0),
				"op_3_window_0_records_in_total":   int64(4),
				"op_3_window_0_records_out_total":  int64(5),

				"op_4_filter_0_exceptions_total":   int64(0),
				"op_4_filter_0_process_latency_us": int64(0),
				"op_4_filter_0_records_in_total":   int64(5),
				"op_4_filter_0_records_out_total":  int64(2),
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
						"ts":    float64(1541152486013),
					},
				},
				{
					{
						"color": "red",
						"temp":  25.5,
						"ts":    float64(1541152486013),
					},
				},
				{
					{
						"color": "blue",
						"temp":  28.1,
						"ts":    float64(1541152487632),
					},
				},
				{
					{
						"color": "blue",
						"temp":  28.1,
						"ts":    float64(1541152487632),
					},
					{
						"color": "yellow",
						"temp":  27.4,
						"ts":    float64(1541152488442),
					},
				},
				{
					{
						"color": "yellow",
						"temp":  27.4,
						"ts":    float64(1541152488442),
					},
					{
						"color": "red",
						"temp":  25.5,
						"ts":    float64(1541152489252),
					},
				},
			},
			M: map[string]interface{}{
				"op_6_project_0_exceptions_total":   int64(0),
				"op_6_project_0_process_latency_us": int64(0),
				"op_6_project_0_records_in_total":   int64(5),
				"op_6_project_0_records_out_total":  int64(5),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(5),
				"sink_mockSink_0_records_out_total": int64(5),

				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(6),
				"source_demoE_0_records_out_total": int64(6),

				"source_demo1E_0_exceptions_total":  int64(0),
				"source_demo1E_0_records_in_total":  int64(6),
				"source_demo1E_0_records_out_total": int64(6),

				"op_4_window_0_exceptions_total":   int64(0),
				"op_4_window_0_process_latency_us": int64(0),
				"op_4_window_0_records_in_total":   int64(9),
				"op_4_window_0_records_out_total":  int64(5),

				"op_5_join_0_exceptions_total":   int64(0),
				"op_5_join_0_process_latency_us": int64(0),
				"op_5_join_0_records_in_total":   int64(5),
				"op_5_join_0_records_out_total":  int64(5),
			},
		},
		{
			Name: `TestEventWindowRule4`,
			Sql:  `SELECT  window_start() as ws, color, window_end() as we FROM demoE GROUP BY SlidingWindow(ss, 2) OVER (WHEN ts - last_hit_time() > 1000), color ORDER BY color`,
			R: [][]map[string]interface{}{
				{{
					"color": "red",
					"ws":    float64(1541152484013),
					"we":    float64(1541152486013),
				}}, {{
					"color": "blue",
					"ws":    float64(1541152485632),
					"we":    float64(1541152487632),
				}, {
					"color": "red",
					"ws":    float64(1541152485632),
					"we":    float64(1541152487632),
				}}, {{
					"color": "blue",
					"ws":    float64(1541152487252),
					"we":    float64(1541152489252),
				}, {
					"color": "red",
					"ws":    float64(1541152487252),
					"we":    float64(1541152489252),
				}, {
					"color": "yellow",
					"ws":    float64(1541152487252),
					"we":    float64(1541152489252),
				}},
			},
			M: map[string]interface{}{
				"op_6_project_0_exceptions_total":   int64(0),
				"op_6_project_0_process_latency_us": int64(0),
				"op_6_project_0_records_in_total":   int64(3),
				"op_6_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),

				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(6),
				"source_demoE_0_records_out_total": int64(6),

				"op_3_window_0_exceptions_total":   int64(0),
				"op_3_window_0_process_latency_us": int64(0),
				"op_3_window_0_records_in_total":   int64(4),
				"op_3_window_0_records_out_total":  int64(3),

				"op_4_aggregate_0_exceptions_total":   int64(0),
				"op_4_aggregate_0_process_latency_us": int64(0),
				"op_4_aggregate_0_records_in_total":   int64(3),
				"op_4_aggregate_0_records_out_total":  int64(3),

				"op_5_order_0_exceptions_total":   int64(0),
				"op_5_order_0_process_latency_us": int64(0),
				"op_5_order_0_records_in_total":   int64(3),
				"op_5_order_0_records_out_total":  int64(3),
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
				"op_4_project_0_exceptions_total":   int64(0),
				"op_4_project_0_process_latency_us": int64(0),
				"op_4_project_0_records_in_total":   int64(4),
				"op_4_project_0_records_out_total":  int64(4),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(4),
				"sink_mockSink_0_records_out_total": int64(4),

				"source_sessionDemoE_0_exceptions_total":  int64(0),
				"source_sessionDemoE_0_records_in_total":  int64(12),
				"source_sessionDemoE_0_records_out_total": int64(12),

				"op_3_window_0_exceptions_total":   int64(0),
				"op_3_window_0_process_latency_us": int64(0),
				"op_3_window_0_records_in_total":   int64(10),
				"op_3_window_0_records_out_total":  int64(4),
			},
		},
		{
			Name: `TestEventWindowRule6`,
			Sql:  `SELECT max(temp) as m, count(color) as c FROM demoE INNER JOIN demo1E ON demoE.ts = demo1E.ts GROUP BY SlidingWindow(ss, 1)`,
			R: [][]map[string]interface{}{
				{
					{
						"m": 25.5,
						"c": float64(1),
					},
				},
				{
					{
						"m": 25.5,
						"c": float64(1),
					},
				},
				{
					{
						"m": 25.5,
						"c": float64(1),
					},
				},
				{
					{
						"m": 28.1,
						"c": float64(1),
					},
				},
				{
					{
						"m": 28.1,
						"c": float64(1),
					},
				},
				{
					{
						"m": 28.1,
						"c": float64(2),
					},
				},
				{
					{
						"m": 28.1,
						"c": float64(2),
					},
				},
				{
					{
						"m": 27.4,
						"c": float64(2),
					},
				},
				{
					{
						"m": 27.4,
						"c": float64(2),
					},
				},
			},
			M: map[string]interface{}{
				"op_6_project_0_exceptions_total":   int64(0),
				"op_6_project_0_process_latency_us": int64(0),
				"op_6_project_0_records_in_total":   int64(9),
				"op_6_project_0_records_out_total":  int64(9),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(9),
				"sink_mockSink_0_records_out_total": int64(9),

				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(6),
				"source_demoE_0_records_out_total": int64(6),

				"source_demo1E_0_exceptions_total":  int64(0),
				"source_demo1E_0_records_in_total":  int64(6),
				"source_demo1E_0_records_out_total": int64(6),

				"op_4_window_0_exceptions_total":  int64(0),
				"op_4_window_0_records_in_total":  int64(9),
				"op_4_window_0_records_out_total": int64(9),

				"op_5_join_0_exceptions_total":   int64(0),
				"op_5_join_0_process_latency_us": int64(0),
				"op_5_join_0_records_in_total":   int64(9),
				"op_5_join_0_records_out_total":  int64(9),
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
					"size":  float64(3),
					"ts":    float64(1541152486013),
				}},
				{{
					"color": "red",
					"size":  float64(3),
					"ts":    float64(1541152486013),
				}},
				{{
					"color": "yellow",
					"size":  float64(4),
					"ts":    float64(1541152488442),
				}},
				{{
					"color": "yellow",
					"size":  float64(4),
					"ts":    float64(1541152488442),
				}, {
					"color": "red",
					"size":  float64(1),
					"ts":    float64(1541152489252),
				}},
				{{
					"color": "red",
					"size":  float64(1),
					"ts":    float64(1541152489252),
				}},
			},
			M: map[string]interface{}{
				"op_4_project_0_exceptions_total":   int64(1),
				"op_4_project_0_process_latency_us": int64(0),
				"op_4_project_0_records_in_total":   int64(5),
				"op_4_project_0_records_out_total":  int64(5),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(6),
				"sink_mockSink_0_records_out_total": int64(6),

				"source_demoErr_0_exceptions_total":  int64(1),
				"source_demoErr_0_records_in_total":  int64(6),
				"source_demoErr_0_records_out_total": int64(5),

				"op_3_window_0_exceptions_total":   int64(1),
				"op_3_window_0_process_latency_us": int64(0),
				"op_3_window_0_records_in_total":   int64(3),
				"op_3_window_0_records_out_total":  int64(5),
			},
		},
		{
			Name: `TestEventWindowRule8`,
			Sql:  `SELECT temp, window_start(), window_end() FROM sessionDemoE GROUP BY SessionWindow(ss, 2, 1) `,
			R: [][]map[string]interface{}{
				{{
					"temp":         25.5,
					"window_start": float64(1541152486013),
					"window_end":   float64(1541152487013),
				}}, {{
					"temp":         28.1,
					"window_start": float64(1541152487932),
					"window_end":   float64(1541152490000),
				}, {
					"temp":         27.4,
					"window_start": float64(1541152487932),
					"window_end":   float64(1541152490000),
				}, {
					"temp":         25.5,
					"window_start": float64(1541152487932),
					"window_end":   float64(1541152490000),
				}}, {{
					"temp":         26.2,
					"window_start": float64(1541152490000),
					"window_end":   float64(1541152494000),
				}, {
					"temp":         26.8,
					"window_start": float64(1541152490000),
					"window_end":   float64(1541152494000),
				}, {
					"temp":         28.9,
					"window_start": float64(1541152490000),
					"window_end":   float64(1541152494000),
				}, {
					"temp":         29.1,
					"window_start": float64(1541152490000),
					"window_end":   float64(1541152494000),
				}, {
					"temp":         32.2,
					"window_start": float64(1541152490000),
					"window_end":   float64(1541152494000),
				}}, {{
					"temp":         30.9,
					"window_start": float64(1541152494000),
					"window_end":   float64(1541152495112),
				}},
			},
			M: map[string]interface{}{
				"op_4_project_0_exceptions_total":   int64(0),
				"op_4_project_0_process_latency_us": int64(0),
				"op_4_project_0_records_in_total":   int64(4),
				"op_4_project_0_records_out_total":  int64(4),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(4),
				"sink_mockSink_0_records_out_total": int64(4),

				"source_sessionDemoE_0_exceptions_total":  int64(0),
				"source_sessionDemoE_0_records_in_total":  int64(12),
				"source_sessionDemoE_0_records_out_total": int64(12),

				"op_3_window_0_exceptions_total":   int64(0),
				"op_3_window_0_process_latency_us": int64(0),
				"op_3_window_0_records_in_total":   int64(10),
				"op_3_window_0_records_out_total":  int64(4),
			},
		},
		{
			Name: `TestEventWindowRule9`,
			Sql:  `SELECT window_end(), color, window_start() FROM demoE GROUP BY HOPPINGWINDOW(ss, 2, 1)`,
			R: [][]map[string]interface{}{
				{{
					"color":        "red",
					"window_start": float64(1541152485013),
					"window_end":   float64(1541152487000),
				}},
				{{
					"color":        "red",
					"window_start": float64(1541152486000),
					"window_end":   float64(1541152488000),
				}, {
					"color":        "blue",
					"window_start": float64(1541152486000),
					"window_end":   float64(1541152488000),
				}},
				{{
					"color":        "blue",
					"window_start": float64(1541152487000),
					"window_end":   float64(1541152489000),
				}, {
					"color":        "yellow",
					"window_start": float64(1541152487000),
					"window_end":   float64(1541152489000),
				}},
				{{
					"color":        "yellow",
					"window_start": float64(1541152488000),
					"window_end":   float64(1541152490000),
				}, {
					"color":        "red",
					"window_start": float64(1541152488000),
					"window_end":   float64(1541152490000),
				}},
				{{
					"color":        "red",
					"window_start": float64(1541152489000),
					"window_end":   float64(1541152491000),
				}},
			},
			M: map[string]interface{}{
				"op_4_project_0_exceptions_total":   int64(0),
				"op_4_project_0_process_latency_us": int64(0),
				"op_4_project_0_records_in_total":   int64(5),
				"op_4_project_0_records_out_total":  int64(5),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(5),
				"sink_mockSink_0_records_out_total": int64(5),

				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(6),
				"source_demoE_0_records_out_total": int64(6),

				"op_3_window_0_exceptions_total":   int64(0),
				"op_3_window_0_process_latency_us": int64(0),
				"op_3_window_0_records_in_total":   int64(4),
				"op_3_window_0_records_out_total":  int64(5),
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
				"op_2_watermark_0_records_in_total":  int64(6),
				"op_2_watermark_0_records_out_total": int64(4),
				"op_2_watermark_0_exceptions_total":  int64(0),

				"op_3_window_0_records_in_total":  int64(4),
				"op_3_window_0_records_out_total": int64(1),
				"op_3_window_0_exceptions_total":  int64(0),
			},
		},
	}
	HandleStream(true, streamList, t)
	options := []*api.RuleOption{
		{
			BufferLength: 100,
			SendError:    true,
			IsEventTime:  true,
			LateTol:      1000,
		},
		{
			BufferLength:       100,
			SendError:          true,
			Qos:                api.AtLeastOnce,
			CheckpointInterval: 5000,
			IsEventTime:        true,
			LateTol:            1000,
		},
		{
			BufferLength:       100,
			SendError:          true,
			Qos:                api.ExactlyOnce,
			CheckpointInterval: 5000,
			IsEventTime:        true,
			LateTol:            1000,
		},
	}
	for j, opt := range options {
		DoRuleTest(t, tests, j, opt, 10)
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
					"error": "run Select error: invalid operation string(string) * int64(3)",
				}}, {{
					"kuiper_field_0": float64(6),
				}, {}},
			},
			M: map[string]interface{}{
				"op_3_project_0_exceptions_total":   int64(1),
				"op_3_project_0_process_latency_us": int64(0),
				"op_3_project_0_records_in_total":   int64(2),
				"op_3_project_0_records_out_total":  int64(1),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(2),
				"sink_mockSink_0_records_out_total": int64(2),

				"source_ldemo_0_exceptions_total":  int64(0),
				"source_ldemo_0_records_in_total":  int64(5),
				"source_ldemo_0_records_out_total": int64(5),

				"op_2_window_0_exceptions_total":   int64(0),
				"op_2_window_0_process_latency_us": int64(0),
				"op_2_window_0_records_in_total":   int64(5),
				"op_2_window_0_records_out_total":  int64(2),
			},
		}, {
			Name: `TestWindowErrorRule2`,
			Sql:  `SELECT color, ts FROM ldemo where size > 2 GROUP BY tumblingwindow(ss, 1)`,
			R: [][]map[string]interface{}{
				{{
					"error": "run Where error: invalid operation string(string) > int64(2)",
				}}, {{
					"color": "red",
					"ts":    float64(1541152486013),
				}}, {{
					"ts": float64(1541152487632),
				}}, {}, {},
			},
			M: map[string]interface{}{
				"op_4_project_0_exceptions_total":   int64(1),
				"op_4_project_0_process_latency_us": int64(0),
				"op_4_project_0_records_in_total":   int64(4),
				"op_4_project_0_records_out_total":  int64(4),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(5),
				"sink_mockSink_0_records_out_total": int64(5),

				"source_ldemo_0_exceptions_total":  int64(0),
				"source_ldemo_0_records_in_total":  int64(5),
				"source_ldemo_0_records_out_total": int64(5),

				"op_3_window_0_exceptions_total":   int64(1),
				"op_3_window_0_process_latency_us": int64(0),
				"op_3_window_0_records_in_total":   int64(3),
				"op_3_window_0_records_out_total":  int64(4),

				"op_2_filter_0_exceptions_total":   int64(1),
				"op_2_filter_0_process_latency_us": int64(0),
				"op_2_filter_0_records_in_total":   int64(5),
				"op_2_filter_0_records_out_total":  int64(2),
			},
		}, {
			Name: `TestWindowErrorRule3`,
			Sql:  `SELECT color, temp, ts FROM ldemo INNER JOIN ldemo1 ON ldemo.ts = ldemo1.ts GROUP BY SlidingWindow(ss, 1)`,
			R: [][]map[string]interface{}{
				{{
					"color": "red",
					"temp":  25.5,
					"ts":    float64(1541152486013),
				}}, {{
					"color": "red",
					"temp":  25.5,
					"ts":    float64(1541152486013),
				}}, {{
					"color": "red",
					"temp":  25.5,
					"ts":    float64(1541152486013),
				}}, {{
					"temp": 28.1,
					"ts":   float64(1541152487632),
				}}, {{
					"temp": 28.1,
					"ts":   float64(1541152487632),
				}}, {{
					"error": "run Join error: invalid operation int64(1541152487632) = string(1541152488442)",
				}}, {{
					"error": "run Join error: invalid operation int64(1541152488442) = string(1541152488442)",
				}}, {{
					"error": "run Join error: invalid operation int64(1541152488442) = string(1541152488442)",
				}},
			},
			M: map[string]interface{}{
				"op_5_project_0_exceptions_total":   int64(3),
				"op_5_project_0_process_latency_us": int64(0),
				"op_5_project_0_records_in_total":   int64(5),
				"op_5_project_0_records_out_total":  int64(5),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(8),
				"sink_mockSink_0_records_out_total": int64(8),

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
					"color": float64(49),
				}, {}},
			},
			M: map[string]interface{}{
				"op_6_project_0_exceptions_total":   int64(3),
				"op_6_project_0_process_latency_us": int64(0),
				"op_6_project_0_records_in_total":   int64(2),
				"op_6_project_0_records_out_total":  int64(2),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(5),
				"sink_mockSink_0_records_out_total": int64(5),

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
					"size": float64(3),
				}}, {{
					"color": float64(49),
					"size":  float64(2),
				}}, {{
					"color": "red",
				}},
			},
			M: map[string]interface{}{
				"op_4_project_0_exceptions_total":   int64(1),
				"op_4_project_0_process_latency_us": int64(0),
				"op_4_project_0_records_in_total":   int64(3),
				"op_4_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(4),
				"sink_mockSink_0_records_out_total": int64(4),

				"source_ldemo_0_exceptions_total":  int64(0),
				"source_ldemo_0_records_in_total":  int64(5),
				"source_ldemo_0_records_out_total": int64(5),

				"op_2_window_0_exceptions_total":   int64(0),
				"op_2_window_0_process_latency_us": int64(0),
				"op_2_window_0_records_in_total":   int64(5),
				"op_2_window_0_records_out_total":  int64(4),

				"op_3_order_0_exceptions_total":   int64(1),
				"op_3_order_0_process_latency_us": int64(0),
				"op_3_order_0_records_in_total":   int64(4),
				"op_3_order_0_records_out_total":  int64(3),
			},
		},
	}
	HandleStream(true, streamList, t)
	DoRuleTest(t, tests, 0, &api.RuleOption{
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
	options := []*api.RuleOption{
		{
			BufferLength:       100,
			SendError:          true,
			Qos:                api.AtLeastOnce,
			CheckpointInterval: 5000,
			IsEventTime:        true,
			LateTol:            1000,
		},
		{
			BufferLength:       100,
			SendError:          true,
			Qos:                api.ExactlyOnce,
			CheckpointInterval: 5000,
			IsEventTime:        true,
			LateTol:            1000,
		},
		{
			BufferLength: 100,
			SendError:    true,
			IsEventTime:  true,
			LateTol:      1000,
		},
	}
	for j, opt := range options {
		DoRuleTest(t, tests, j, opt, 10)
	}
}
