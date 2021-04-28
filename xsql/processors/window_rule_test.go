package processors

import (
	"github.com/emqx/kuiper/xstream"
	"github.com/emqx/kuiper/xstream/api"
	"testing"
)

func TestWindow(t *testing.T) {
	//Reset
	streamList := []string{"demo", "demoError", "demo1", "sessionDemo", "table1"}
	handleStream(false, streamList, t)
	var tests = []ruleTest{
		{
			name: `TestWindowRule1`,
			sql:  `SELECT * FROM demo GROUP BY HOPPINGWINDOW(ss, 2, 1)`,
			r: [][]map[string]interface{}{
				{{
					"color": "red",
					"size":  float64(3),
					"ts":    float64(1541152486013),
				}, {
					"color": "blue",
					"size":  float64(6),
					"ts":    float64(1541152486822),
				}},
				{{
					"color": "red",
					"size":  float64(3),
					"ts":    float64(1541152486013),
				}, {
					"color": "blue",
					"size":  float64(6),
					"ts":    float64(1541152486822),
				}, {
					"color": "blue",
					"size":  float64(2),
					"ts":    float64(1541152487632),
				}},
				{{
					"color": "blue",
					"size":  float64(2),
					"ts":    float64(1541152487632),
				}, {
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
			},
			m: map[string]interface{}{
				"op_1_preprocessor_demo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_demo_0_process_latency_us": int64(0),
				"op_1_preprocessor_demo_0_records_in_total":   int64(5),
				"op_1_preprocessor_demo_0_records_out_total":  int64(5),

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
		}, {
			name: `TestWindowRule2`,
			sql:  `SELECT color, ts FROM demo where size > 2 GROUP BY tumblingwindow(ss, 1)`,
			r: [][]map[string]interface{}{
				{{
					"color": "red",
					"ts":    float64(1541152486013),
				}, {
					"color": "blue",
					"ts":    float64(1541152486822),
				}},
				{{
					"color": "yellow",
					"ts":    float64(1541152488442),
				}},
			},
			m: map[string]interface{}{
				"op_1_preprocessor_demo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_demo_0_process_latency_us": int64(0),
				"op_1_preprocessor_demo_0_records_in_total":   int64(5),
				"op_1_preprocessor_demo_0_records_out_total":  int64(5),

				"op_4_project_0_exceptions_total":   int64(0),
				"op_4_project_0_process_latency_us": int64(0),
				"op_4_project_0_records_in_total":   int64(2),
				"op_4_project_0_records_out_total":  int64(2),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(2),
				"sink_mockSink_0_records_out_total": int64(2),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"op_3_window_0_exceptions_total":   int64(0),
				"op_3_window_0_process_latency_us": int64(0),
				"op_3_window_0_records_in_total":   int64(3),
				"op_3_window_0_records_out_total":  int64(2),

				"op_2_filter_0_exceptions_total":   int64(0),
				"op_2_filter_0_process_latency_us": int64(0),
				"op_2_filter_0_records_in_total":   int64(5),
				"op_2_filter_0_records_out_total":  int64(3),
			},
		}, {
			name: `TestWindowRule3`,
			sql:  `SELECT color, temp, ts FROM demo INNER JOIN demo1 ON demo.ts = demo1.ts GROUP BY SlidingWindow(ss, 1)`,
			r: [][]map[string]interface{}{
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
					"color": "blue",
					"temp":  28.1,
					"ts":    float64(1541152487632),
				}}, {{
					"color": "blue",
					"temp":  28.1,
					"ts":    float64(1541152487632),
				}}, {{
					"color": "blue",
					"temp":  28.1,
					"ts":    float64(1541152487632),
				}, {
					"color": "yellow",
					"temp":  27.4,
					"ts":    float64(1541152488442),
				}}, {{
					"color": "yellow",
					"temp":  27.4,
					"ts":    float64(1541152488442),
				}}, {{
					"color": "yellow",
					"temp":  27.4,
					"ts":    float64(1541152488442),
				}, {
					"color": "red",
					"temp":  25.5,
					"ts":    float64(1541152489252),
				}},
			},
			m: map[string]interface{}{
				"op_1_preprocessor_demo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_demo_0_process_latency_us": int64(0),
				"op_1_preprocessor_demo_0_records_in_total":   int64(5),
				"op_1_preprocessor_demo_0_records_out_total":  int64(5),

				"op_2_preprocessor_demo1_0_exceptions_total":   int64(0),
				"op_2_preprocessor_demo1_0_process_latency_us": int64(0),
				"op_2_preprocessor_demo1_0_records_in_total":   int64(5),
				"op_2_preprocessor_demo1_0_records_out_total":  int64(5),

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
			t: &xstream.PrintableTopo{
				Sources: []string{"source_demo", "source_demo1"},
				Edges: map[string][]string{
					"source_demo":             {"op_1_preprocessor_demo"},
					"source_demo1":            {"op_2_preprocessor_demo1"},
					"op_1_preprocessor_demo":  {"op_3_window"},
					"op_2_preprocessor_demo1": {"op_3_window"},
					"op_3_window":             {"op_4_join"},
					"op_4_join":               {"op_5_project"},
					"op_5_project":            {"sink_mockSink"},
				},
			},
		}, {
			name: `TestWindowRule4`,
			sql:  `SELECT color FROM demo GROUP BY SlidingWindow(ss, 2), color ORDER BY color`,
			r: [][]map[string]interface{}{
				{{
					"color": "red",
				}}, {{
					"color": "blue",
				}, {
					"color": "red",
				}}, {{
					"color": "blue",
				}, {
					"color": "red",
				}}, {{
					"color": "blue",
				}, {
					"color": "yellow",
				}}, {{
					"color": "blue",
				}, {
					"color": "red",
				}, {
					"color": "yellow",
				}},
			},
			m: map[string]interface{}{
				"op_1_preprocessor_demo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_demo_0_process_latency_us": int64(0),
				"op_1_preprocessor_demo_0_records_in_total":   int64(5),
				"op_1_preprocessor_demo_0_records_out_total":  int64(5),

				"op_5_project_0_exceptions_total":   int64(0),
				"op_5_project_0_process_latency_us": int64(0),
				"op_5_project_0_records_in_total":   int64(5),
				"op_5_project_0_records_out_total":  int64(5),

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

				"op_3_aggregate_0_exceptions_total":   int64(0),
				"op_3_aggregate_0_process_latency_us": int64(0),
				"op_3_aggregate_0_records_in_total":   int64(5),
				"op_3_aggregate_0_records_out_total":  int64(5),

				"op_4_order_0_exceptions_total":   int64(0),
				"op_4_order_0_process_latency_us": int64(0),
				"op_4_order_0_records_in_total":   int64(5),
				"op_4_order_0_records_out_total":  int64(5),
			},
		}, {
			name: `TestWindowRule5`,
			sql:  `SELECT temp FROM sessionDemo GROUP BY SessionWindow(ss, 2, 1) `,
			r: [][]map[string]interface{}{
				{{
					"temp": 25.5,
				}, {
					"temp": 27.5,
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
			m: map[string]interface{}{
				"op_1_preprocessor_sessionDemo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_sessionDemo_0_process_latency_us": int64(0),
				"op_1_preprocessor_sessionDemo_0_records_in_total":   int64(11),
				"op_1_preprocessor_sessionDemo_0_records_out_total":  int64(11),

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
		}, {
			name: `TestWindowRule6`,
			sql:  `SELECT max(temp) as m, count(color) as c FROM demo INNER JOIN demo1 ON demo.ts = demo1.ts GROUP BY SlidingWindow(ss, 1)`,
			r: [][]map[string]interface{}{
				{{
					"m": 25.5,
					"c": float64(1),
				}}, {{
					"m": 25.5,
					"c": float64(1),
				}}, {{
					"m": 25.5,
					"c": float64(1),
				}}, {{
					"m": 28.1,
					"c": float64(1),
				}}, {{
					"m": 28.1,
					"c": float64(1),
				}}, {{
					"m": 28.1,
					"c": float64(2),
				}}, {{
					"m": 27.4,
					"c": float64(1),
				}}, {{
					"m": 27.4,
					"c": float64(2),
				}},
			},
			m: map[string]interface{}{
				"op_1_preprocessor_demo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_demo_0_process_latency_us": int64(0),
				"op_1_preprocessor_demo_0_records_in_total":   int64(5),
				"op_1_preprocessor_demo_0_records_out_total":  int64(5),

				"op_2_preprocessor_demo1_0_exceptions_total":   int64(0),
				"op_2_preprocessor_demo1_0_process_latency_us": int64(0),
				"op_2_preprocessor_demo1_0_records_in_total":   int64(5),
				"op_2_preprocessor_demo1_0_records_out_total":  int64(5),

				"op_6_project_0_exceptions_total":   int64(0),
				"op_6_project_0_process_latency_us": int64(0),
				"op_6_project_0_records_in_total":   int64(8),
				"op_6_project_0_records_out_total":  int64(8),

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
		}, {
			name: `TestWindowRule7`,
			sql:  `SELECT * FROM demoError GROUP BY HOPPINGWINDOW(ss, 2, 1)`,
			r: [][]map[string]interface{}{
				{{
					"error": "error in preprocessor: invalid data type for color, expect string but found int(3)",
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
					"error": "error in preprocessor: invalid data type for color, expect string but found int(7)",
				}},
				{{
					"color": "blue",
					"size":  float64(2),
					"ts":    float64(1541152487632),
				}},
				{{
					"error": "error in preprocessor: invalid data type for size, expect bigint but found string(blue)",
				}},
			},
			m: map[string]interface{}{
				"op_1_preprocessor_demoError_0_exceptions_total":   int64(3),
				"op_1_preprocessor_demoError_0_process_latency_us": int64(0),
				"op_1_preprocessor_demoError_0_records_in_total":   int64(5),
				"op_1_preprocessor_demoError_0_records_out_total":  int64(2),

				"op_3_project_0_exceptions_total":   int64(3),
				"op_3_project_0_process_latency_us": int64(0),
				"op_3_project_0_records_in_total":   int64(6),
				"op_3_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(6),
				"sink_mockSink_0_records_out_total": int64(6),

				"source_demoError_0_exceptions_total":  int64(0),
				"source_demoError_0_records_in_total":  int64(5),
				"source_demoError_0_records_out_total": int64(5),

				"op_2_window_0_exceptions_total":   int64(3),
				"op_2_window_0_process_latency_us": int64(0),
				"op_2_window_0_records_in_total":   int64(5),
				"op_2_window_0_records_out_total":  int64(3),
			},
		}, {
			name: `TestWindowRule8`,
			sql:  `SELECT color, ts, count(*) as c FROM demo where size > 2 GROUP BY tumblingwindow(ss, 1) having c > 1`,
			r: [][]map[string]interface{}{
				{{
					"color": "red",
					"ts":    float64(1541152486013),
					"c":     float64(2),
				}},
			},
			m: map[string]interface{}{
				"op_1_preprocessor_demo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_demo_0_process_latency_us": int64(0),
				"op_1_preprocessor_demo_0_records_in_total":   int64(5),
				"op_1_preprocessor_demo_0_records_out_total":  int64(5),

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

				"op_3_window_0_exceptions_total":   int64(0),
				"op_3_window_0_process_latency_us": int64(0),
				"op_3_window_0_records_in_total":   int64(3),
				"op_3_window_0_records_out_total":  int64(2),

				"op_2_filter_0_exceptions_total":   int64(0),
				"op_2_filter_0_process_latency_us": int64(0),
				"op_2_filter_0_records_in_total":   int64(5),
				"op_2_filter_0_records_out_total":  int64(3),

				"op_4_aggregate_0_exceptions_total":   int64(0),
				"op_4_aggregate_0_process_latency_us": int64(0),
				"op_4_aggregate_0_records_in_total":   int64(2),
				"op_4_aggregate_0_records_out_total":  int64(2),

				"op_5_having_0_exceptions_total":   int64(0),
				"op_5_having_0_process_latency_us": int64(0),
				"op_5_having_0_records_in_total":   int64(2),
				"op_5_having_0_records_out_total":  int64(1),
			},
		}, {
			name: `TestWindowRule9`,
			sql:  `SELECT * FROM demo GROUP BY HOPPINGWINDOW(ss, 2, 1) FILTER( WHERE size > 2)`,
			r: [][]map[string]interface{}{
				{{
					"color": "red",
					"size":  float64(3),
					"ts":    float64(1541152486013),
				}, {
					"color": "blue",
					"size":  float64(6),
					"ts":    float64(1541152486822),
				}},
				{{
					"color": "red",
					"size":  float64(3),
					"ts":    float64(1541152486013),
				}, {
					"color": "blue",
					"size":  float64(6),
					"ts":    float64(1541152486822),
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
				}},
			},
			m: map[string]interface{}{
				"op_1_preprocessor_demo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_demo_0_process_latency_us": int64(0),
				"op_1_preprocessor_demo_0_records_in_total":   int64(5),
				"op_1_preprocessor_demo_0_records_out_total":  int64(5),

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
		}, {
			name: `TestCountWindowRule1`,
			sql:  `SELECT collect(*)[0]->color as c FROM demo GROUP BY COUNTWINDOW(3)`,
			r: [][]map[string]interface{}{
				{{
					"c": "red",
				}},
			},
			m: map[string]interface{}{
				"op_1_preprocessor_demo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_demo_0_process_latency_us": int64(0),
				"op_1_preprocessor_demo_0_records_in_total":   int64(5),
				"op_1_preprocessor_demo_0_records_out_total":  int64(5),

				"op_4_project_0_exceptions_total":   int64(0),
				"op_4_project_0_process_latency_us": int64(0),
				"op_4_project_0_records_in_total":   int64(1),
				"op_4_project_0_records_out_total":  int64(1),

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
		}, {
			name: `TestWindowRule10`,
			sql:  `SELECT deduplicate(color, false)->color as c FROM demo GROUP BY SlidingWindow(hh, 1)`,
			r: [][]map[string]interface{}{
				{{
					"c": "red",
				}}, {{
					"c": "blue",
				}}, {{}}, {{
					"c": "yellow",
				}}, {{}},
			},
			m: map[string]interface{}{
				"op_1_preprocessor_demo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_demo_0_process_latency_us": int64(0),
				"op_1_preprocessor_demo_0_records_in_total":   int64(5),
				"op_1_preprocessor_demo_0_records_out_total":  int64(5),

				"op_4_project_0_exceptions_total":   int64(0),
				"op_4_project_0_process_latency_us": int64(0),
				"op_4_project_0_records_in_total":   int64(5),
				"op_4_project_0_records_out_total":  int64(5),

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
		}, {
			name: `TestWindowRule11`,
			sql:  `SELECT color, name FROM demo INNER JOIN table1 on demo.ts = table1.id where demo.size > 2 and table1.size > 1 GROUP BY tumblingwindow(ss, 1)`,
			r: [][]map[string]interface{}{
				{{
					"color": "red",
					"name":  "name1",
				}},
			},
			m: map[string]interface{}{
				//"op_4_project_0_exceptions_total":   int64(0),
				//"op_4_project_0_process_latency_us": int64(0),
				//"op_4_project_0_records_in_total":   int64(2),
				//"op_4_project_0_records_out_total":  int64(2),

				"op_3_window_0_exceptions_total":   int64(0),
				"op_3_window_0_process_latency_us": int64(0),
				"op_3_window_0_records_in_total":   int64(3),
				"op_3_window_0_records_out_total":  int64(2),

				"op_2_filter_0_exceptions_total":   int64(0),
				"op_2_filter_0_process_latency_us": int64(0),
				"op_2_filter_0_records_in_total":   int64(5),
				"op_2_filter_0_records_out_total":  int64(3),

				"op_1_preprocessor_demo_0_exceptions_total":  int64(0),
				"op_1_preprocessor_demo_0_records_in_total":  int64(5),
				"op_1_preprocessor_demo_0_records_out_total": int64(5),

				"op_4_tableprocessor_table1_0_exceptions_total":  int64(0),
				"op_4_tableprocessor_table1_0_records_in_total":  int64(1),
				"op_4_tableprocessor_table1_0_records_out_total": int64(1),

				"op_5_filter_0_exceptions_total":  int64(0),
				"op_5_filter_0_records_in_total":  int64(1),
				"op_5_filter_0_records_out_total": int64(1),

				"op_6_join_aligner_0_records_in_total":  int64(3),
				"op_6_join_aligner_0_records_out_total": int64(2),

				"op_7_join_0_exceptions_total":  int64(0),
				"op_7_join_0_records_in_total":  int64(2),
				"op_7_join_0_records_out_total": int64(1),

				"op_8_project_0_exceptions_total":  int64(0),
				"op_8_project_0_records_in_total":  int64(1),
				"op_8_project_0_records_out_total": int64(1),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(1),
				"sink_mockSink_0_records_out_total": int64(1),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"source_table1_0_exceptions_total":  int64(0),
				"source_table1_0_records_in_total":  int64(1),
				"source_table1_0_records_out_total": int64(1),
			},
		},
	}
	handleStream(true, streamList, t)
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
		doRuleTest(t, tests, j, opt)
	}
}

func TestEventWindow(t *testing.T) {
	//Reset
	streamList := []string{"demoE", "demoErr", "demo1E", "sessionDemoE"}
	handleStream(false, streamList, t)
	var tests = []ruleTest{
		{
			name: `TestEventWindowRule1`,
			sql:  `SELECT * FROM demoE GROUP BY HOPPINGWINDOW(ss, 2, 1)`,
			r: [][]map[string]interface{}{
				{{
					"color": "red",
					"size":  float64(3),
					"ts":    float64(1541152486013),
				}},
				{{
					"color": "red",
					"size":  float64(3),
					"ts":    float64(1541152486013),
				}, {
					"color": "blue",
					"size":  float64(2),
					"ts":    float64(1541152487632),
				}},
				{{
					"color": "blue",
					"size":  float64(2),
					"ts":    float64(1541152487632),
				}, {
					"color": "yellow",
					"size":  float64(4),
					"ts":    float64(1541152488442),
				}}, {{
					"color": "yellow",
					"size":  float64(4),
					"ts":    float64(1541152488442),
				}, {
					"color": "red",
					"size":  float64(1),
					"ts":    float64(1541152489252),
				}}, {{
					"color": "red",
					"size":  float64(1),
					"ts":    float64(1541152489252),
				}},
			},
			m: map[string]interface{}{
				"op_1_preprocessor_demoE_0_exceptions_total":   int64(0),
				"op_1_preprocessor_demoE_0_process_latency_us": int64(0),
				"op_1_preprocessor_demoE_0_records_in_total":   int64(6),
				"op_1_preprocessor_demoE_0_records_out_total":  int64(6),

				"op_3_project_0_exceptions_total":   int64(0),
				"op_3_project_0_process_latency_us": int64(0),
				"op_3_project_0_records_in_total":   int64(5),
				"op_3_project_0_records_out_total":  int64(5),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(5),
				"sink_mockSink_0_records_out_total": int64(5),

				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(6),
				"source_demoE_0_records_out_total": int64(6),

				"op_2_window_0_exceptions_total":   int64(0),
				"op_2_window_0_process_latency_us": int64(0),
				"op_2_window_0_records_in_total":   int64(6),
				"op_2_window_0_records_out_total":  int64(5),
			},
		}, {
			name: `TestEventWindowRule2`,
			sql:  `SELECT color, ts FROM demoE where size > 2 GROUP BY tumblingwindow(ss, 1)`,
			r: [][]map[string]interface{}{
				{{
					"color": "red",
					"ts":    float64(1541152486013),
				}},
				{{
					"color": "yellow",
					"ts":    float64(1541152488442),
				}},
			},
			m: map[string]interface{}{
				"op_1_preprocessor_demoE_0_exceptions_total":   int64(0),
				"op_1_preprocessor_demoE_0_process_latency_us": int64(0),
				"op_1_preprocessor_demoE_0_records_in_total":   int64(6),
				"op_1_preprocessor_demoE_0_records_out_total":  int64(6),

				"op_4_project_0_exceptions_total":   int64(0),
				"op_4_project_0_process_latency_us": int64(0),
				"op_4_project_0_records_in_total":   int64(2),
				"op_4_project_0_records_out_total":  int64(2),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(2),
				"sink_mockSink_0_records_out_total": int64(2),

				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(6),
				"source_demoE_0_records_out_total": int64(6),

				"op_2_window_0_exceptions_total":   int64(0),
				"op_2_window_0_process_latency_us": int64(0),
				"op_2_window_0_records_in_total":   int64(6),
				"op_2_window_0_records_out_total":  int64(4),

				"op_3_filter_0_exceptions_total":   int64(0),
				"op_3_filter_0_process_latency_us": int64(0),
				"op_3_filter_0_records_in_total":   int64(4),
				"op_3_filter_0_records_out_total":  int64(2),
			},
		}, {
			name: `TestEventWindowRule3`,
			sql:  `SELECT color, temp, ts FROM demoE INNER JOIN demo1E ON demoE.ts = demo1E.ts GROUP BY SlidingWindow(ss, 1)`,
			r: [][]map[string]interface{}{
				{{
					"color": "red",
					"temp":  25.5,
					"ts":    float64(1541152486013),
				}}, {{
					"color": "red",
					"temp":  25.5,
					"ts":    float64(1541152486013),
				}}, {{
					"color": "blue",
					"temp":  28.1,
					"ts":    float64(1541152487632),
				}}, {{
					"color": "blue",
					"temp":  28.1,
					"ts":    float64(1541152487632),
				}, {
					"color": "yellow",
					"temp":  27.4,
					"ts":    float64(1541152488442),
				}}, {{
					"color": "yellow",
					"temp":  27.4,
					"ts":    float64(1541152488442),
				}, {
					"color": "red",
					"temp":  25.5,
					"ts":    float64(1541152489252),
				}},
			},
			m: map[string]interface{}{
				"op_1_preprocessor_demoE_0_exceptions_total":   int64(0),
				"op_1_preprocessor_demoE_0_process_latency_us": int64(0),
				"op_1_preprocessor_demoE_0_records_in_total":   int64(6),
				"op_1_preprocessor_demoE_0_records_out_total":  int64(6),

				"op_2_preprocessor_demo1E_0_exceptions_total":   int64(0),
				"op_2_preprocessor_demo1E_0_process_latency_us": int64(0),
				"op_2_preprocessor_demo1E_0_records_in_total":   int64(6),
				"op_2_preprocessor_demo1E_0_records_out_total":  int64(6),

				"op_5_project_0_exceptions_total":   int64(0),
				"op_5_project_0_process_latency_us": int64(0),
				"op_5_project_0_records_in_total":   int64(5),
				"op_5_project_0_records_out_total":  int64(5),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(5),
				"sink_mockSink_0_records_out_total": int64(5),

				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(6),
				"source_demoE_0_records_out_total": int64(6),

				"source_demo1E_0_exceptions_total":  int64(0),
				"source_demo1E_0_records_in_total":  int64(6),
				"source_demo1E_0_records_out_total": int64(6),

				"op_3_window_0_exceptions_total":   int64(0),
				"op_3_window_0_process_latency_us": int64(0),
				"op_3_window_0_records_in_total":   int64(12),
				"op_3_window_0_records_out_total":  int64(5),

				"op_4_join_0_exceptions_total":   int64(0),
				"op_4_join_0_process_latency_us": int64(0),
				"op_4_join_0_records_in_total":   int64(5),
				"op_4_join_0_records_out_total":  int64(5),
			},
		}, {
			name: `TestEventWindowRule4`,
			sql:  `SELECT color FROM demoE GROUP BY SlidingWindow(ss, 2), color ORDER BY color`,
			r: [][]map[string]interface{}{
				{{
					"color": "red",
				}}, {{
					"color": "blue",
				}, {
					"color": "red",
				}}, {{
					"color": "blue",
				}, {
					"color": "yellow",
				}}, {{
					"color": "blue",
				}, {
					"color": "red",
				}, {
					"color": "yellow",
				}},
			},
			m: map[string]interface{}{
				"op_1_preprocessor_demoE_0_exceptions_total":   int64(0),
				"op_1_preprocessor_demoE_0_process_latency_us": int64(0),
				"op_1_preprocessor_demoE_0_records_in_total":   int64(6),
				"op_1_preprocessor_demoE_0_records_out_total":  int64(6),

				"op_5_project_0_exceptions_total":   int64(0),
				"op_5_project_0_process_latency_us": int64(0),
				"op_5_project_0_records_in_total":   int64(4),
				"op_5_project_0_records_out_total":  int64(4),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(4),
				"sink_mockSink_0_records_out_total": int64(4),

				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(6),
				"source_demoE_0_records_out_total": int64(6),

				"op_2_window_0_exceptions_total":   int64(0),
				"op_2_window_0_process_latency_us": int64(0),
				"op_2_window_0_records_in_total":   int64(6),
				"op_2_window_0_records_out_total":  int64(4),

				"op_3_aggregate_0_exceptions_total":   int64(0),
				"op_3_aggregate_0_process_latency_us": int64(0),
				"op_3_aggregate_0_records_in_total":   int64(4),
				"op_3_aggregate_0_records_out_total":  int64(4),

				"op_4_order_0_exceptions_total":   int64(0),
				"op_4_order_0_process_latency_us": int64(0),
				"op_4_order_0_records_in_total":   int64(4),
				"op_4_order_0_records_out_total":  int64(4),
			},
		}, {
			name: `TestEventWindowRule5`,
			sql:  `SELECT temp FROM sessionDemoE GROUP BY SessionWindow(ss, 2, 1) `,
			r: [][]map[string]interface{}{
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
			m: map[string]interface{}{
				"op_1_preprocessor_sessionDemoE_0_exceptions_total":   int64(0),
				"op_1_preprocessor_sessionDemoE_0_process_latency_us": int64(0),
				"op_1_preprocessor_sessionDemoE_0_records_in_total":   int64(12),
				"op_1_preprocessor_sessionDemoE_0_records_out_total":  int64(12),

				"op_3_project_0_exceptions_total":   int64(0),
				"op_3_project_0_process_latency_us": int64(0),
				"op_3_project_0_records_in_total":   int64(4),
				"op_3_project_0_records_out_total":  int64(4),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(4),
				"sink_mockSink_0_records_out_total": int64(4),

				"source_sessionDemoE_0_exceptions_total":  int64(0),
				"source_sessionDemoE_0_records_in_total":  int64(12),
				"source_sessionDemoE_0_records_out_total": int64(12),

				"op_2_window_0_exceptions_total":   int64(0),
				"op_2_window_0_process_latency_us": int64(0),
				"op_2_window_0_records_in_total":   int64(12),
				"op_2_window_0_records_out_total":  int64(4),
			},
		}, {
			name: `TestEventWindowRule6`,
			sql:  `SELECT max(temp) as m, count(color) as c FROM demoE INNER JOIN demo1E ON demoE.ts = demo1E.ts GROUP BY SlidingWindow(ss, 1)`,
			r: [][]map[string]interface{}{
				{{
					"m": 25.5,
					"c": float64(1),
				}}, {{
					"m": 25.5,
					"c": float64(1),
				}}, {{
					"m": 28.1,
					"c": float64(1),
				}}, {{
					"m": 28.1,
					"c": float64(2),
				}}, {{
					"m": 27.4,
					"c": float64(2),
				}},
			},
			m: map[string]interface{}{
				"op_1_preprocessor_demoE_0_exceptions_total":   int64(0),
				"op_1_preprocessor_demoE_0_process_latency_us": int64(0),
				"op_1_preprocessor_demoE_0_records_in_total":   int64(6),
				"op_1_preprocessor_demoE_0_records_out_total":  int64(6),

				"op_2_preprocessor_demo1E_0_exceptions_total":   int64(0),
				"op_2_preprocessor_demo1E_0_process_latency_us": int64(0),
				"op_2_preprocessor_demo1E_0_records_in_total":   int64(6),
				"op_2_preprocessor_demo1E_0_records_out_total":  int64(6),

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

				"op_3_window_0_exceptions_total":  int64(0),
				"op_3_window_0_records_in_total":  int64(12),
				"op_3_window_0_records_out_total": int64(5),

				"op_4_join_0_exceptions_total":   int64(0),
				"op_4_join_0_process_latency_us": int64(0),
				"op_4_join_0_records_in_total":   int64(5),
				"op_4_join_0_records_out_total":  int64(5),
			},
		}, {
			name: `TestEventWindowRule7`,
			sql:  `SELECT * FROM demoErr GROUP BY HOPPINGWINDOW(ss, 2, 1)`,
			r: [][]map[string]interface{}{
				{{
					"error": "error in preprocessor: invalid data type for color, expect string but found int(2)",
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
				}}, {{
					"color": "yellow",
					"size":  float64(4),
					"ts":    float64(1541152488442),
				}, {
					"color": "red",
					"size":  float64(1),
					"ts":    float64(1541152489252),
				}}, {{
					"color": "red",
					"size":  float64(1),
					"ts":    float64(1541152489252),
				}},
			},
			m: map[string]interface{}{
				"op_1_preprocessor_demoErr_0_exceptions_total":   int64(1),
				"op_1_preprocessor_demoErr_0_process_latency_us": int64(0),
				"op_1_preprocessor_demoErr_0_records_in_total":   int64(6),
				"op_1_preprocessor_demoErr_0_records_out_total":  int64(5),

				"op_3_project_0_exceptions_total":   int64(1),
				"op_3_project_0_process_latency_us": int64(0),
				"op_3_project_0_records_in_total":   int64(6),
				"op_3_project_0_records_out_total":  int64(5),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(6),
				"sink_mockSink_0_records_out_total": int64(6),

				"source_demoErr_0_exceptions_total":  int64(0),
				"source_demoErr_0_records_in_total":  int64(6),
				"source_demoErr_0_records_out_total": int64(6),

				"op_2_window_0_exceptions_total":   int64(1),
				"op_2_window_0_process_latency_us": int64(0),
				"op_2_window_0_records_in_total":   int64(6),
				"op_2_window_0_records_out_total":  int64(5),
			},
		},
	}
	handleStream(true, streamList, t)
	options := []*api.RuleOption{
		{
			BufferLength: 100,
			SendError:    true,
			IsEventTime:  true,
			LateTol:      1000,
		}, {
			BufferLength:       100,
			SendError:          true,
			Qos:                api.AtLeastOnce,
			CheckpointInterval: 5000,
			IsEventTime:        true,
			LateTol:            1000,
		}, {
			BufferLength:       100,
			SendError:          true,
			Qos:                api.ExactlyOnce,
			CheckpointInterval: 5000,
			IsEventTime:        true,
			LateTol:            1000,
		},
	}
	for j, opt := range options {
		doRuleTest(t, tests, j, opt)
	}
}

func TestWindowError(t *testing.T) {
	//Reset
	streamList := []string{"ldemo", "ldemo1"}
	handleStream(false, streamList, t)
	var tests = []ruleTest{
		{
			name: `TestWindowErrorRule1`,
			sql:  `SELECT size * 3 FROM ldemo GROUP BY TUMBLINGWINDOW(ss, 2)`,
			r: [][]map[string]interface{}{
				{{
					"error": "run Select error: invalid operation string(string) * int64(3)",
				}}, {{
					"kuiper_field_0": float64(6),
				}, {}},
			},
			m: map[string]interface{}{
				"op_1_preprocessor_ldemo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_ldemo_0_process_latency_us": int64(0),
				"op_1_preprocessor_ldemo_0_records_in_total":   int64(5),
				"op_1_preprocessor_ldemo_0_records_out_total":  int64(5),

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
			name: `TestWindowErrorRule2`,
			sql:  `SELECT color, ts FROM ldemo where size > 2 GROUP BY tumblingwindow(ss, 1)`,
			r: [][]map[string]interface{}{
				{{
					"error": "run Where error: invalid operation string(string) > int64(2)",
				}}, {{
					"color": "red",
					"ts":    float64(1541152486013),
				}}, {{
					"ts": float64(1541152487632),
				}},
			},
			m: map[string]interface{}{
				"op_1_preprocessor_ldemo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_ldemo_0_process_latency_us": int64(0),
				"op_1_preprocessor_ldemo_0_records_in_total":   int64(5),
				"op_1_preprocessor_ldemo_0_records_out_total":  int64(5),

				"op_4_project_0_exceptions_total":   int64(1),
				"op_4_project_0_process_latency_us": int64(0),
				"op_4_project_0_records_in_total":   int64(3),
				"op_4_project_0_records_out_total":  int64(2),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),

				"source_ldemo_0_exceptions_total":  int64(0),
				"source_ldemo_0_records_in_total":  int64(5),
				"source_ldemo_0_records_out_total": int64(5),

				"op_3_window_0_exceptions_total":   int64(1),
				"op_3_window_0_process_latency_us": int64(0),
				"op_3_window_0_records_in_total":   int64(3),
				"op_3_window_0_records_out_total":  int64(2),

				"op_2_filter_0_exceptions_total":   int64(1),
				"op_2_filter_0_process_latency_us": int64(0),
				"op_2_filter_0_records_in_total":   int64(5),
				"op_2_filter_0_records_out_total":  int64(2),
			},
		}, {
			name: `TestWindowErrorRule3`,
			sql:  `SELECT color, temp, ts FROM ldemo INNER JOIN ldemo1 ON ldemo.ts = ldemo1.ts GROUP BY SlidingWindow(ss, 1)`,
			r: [][]map[string]interface{}{
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
			m: map[string]interface{}{
				"op_1_preprocessor_ldemo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_ldemo_0_process_latency_us": int64(0),
				"op_1_preprocessor_ldemo_0_records_in_total":   int64(5),
				"op_1_preprocessor_ldemo_0_records_out_total":  int64(5),

				"op_2_preprocessor_ldemo1_0_exceptions_total":   int64(0),
				"op_2_preprocessor_ldemo1_0_process_latency_us": int64(0),
				"op_2_preprocessor_ldemo1_0_records_in_total":   int64(5),
				"op_2_preprocessor_ldemo1_0_records_out_total":  int64(5),

				"op_5_project_0_exceptions_total":   int64(3),
				"op_5_project_0_process_latency_us": int64(0),
				"op_5_project_0_records_in_total":   int64(8),
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
			name: `TestWindowErrorRule4`,
			sql:  `SELECT color FROM ldemo GROUP BY SlidingWindow(ss, 2), color having size >= 2 order by color`,
			r: [][]map[string]interface{}{
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
			m: map[string]interface{}{
				"op_1_preprocessor_ldemo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_ldemo_0_process_latency_us": int64(0),
				"op_1_preprocessor_ldemo_0_records_in_total":   int64(5),
				"op_1_preprocessor_ldemo_0_records_out_total":  int64(5),

				"op_6_project_0_exceptions_total":   int64(3),
				"op_6_project_0_process_latency_us": int64(0),
				"op_6_project_0_records_in_total":   int64(5),
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
			name: `TestWindowErrorRule5`,
			sql:  `SELECT color, size FROM ldemo GROUP BY tumblingwindow(ss, 1) ORDER BY size`,
			r: [][]map[string]interface{}{
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
			m: map[string]interface{}{
				"op_1_preprocessor_ldemo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_ldemo_0_process_latency_us": int64(0),
				"op_1_preprocessor_ldemo_0_records_in_total":   int64(5),
				"op_1_preprocessor_ldemo_0_records_out_total":  int64(5),

				"op_4_project_0_exceptions_total":   int64(1),
				"op_4_project_0_process_latency_us": int64(0),
				"op_4_project_0_records_in_total":   int64(4),
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
	handleStream(true, streamList, t)
	doRuleTest(t, tests, 0, &api.RuleOption{
		BufferLength: 100,
		SendError:    true,
	})
}
