package topotest

import (
	"encoding/json"
	"github.com/emqx/kuiper/xstream"
	"github.com/emqx/kuiper/xstream/api"
	"testing"
)

func TestSingleSQL(t *testing.T) {
	//Reset
	streamList := []string{"demo", "demoError", "demo1", "table1"}
	HandleStream(false, streamList, t)
	//Data setup
	var tests = []RuleTest{
		{
			Name: `TestSingleSQLRule1`,
			Sql:  `SELECT * FROM demo`,
			R: [][]map[string]interface{}{
				{{
					"color": "red",
					"size":  float64(3),
					"ts":    float64(1541152486013),
				}},
				{{
					"color": "blue",
					"size":  float64(6),
					"ts":    float64(1541152486822),
				}},
				{{
					"color": "blue",
					"size":  float64(2),
					"ts":    float64(1541152487632),
				}},
				{{
					"color": "yellow",
					"size":  float64(4),
					"ts":    float64(1541152488442),
				}},
				{{
					"color": "red",
					"size":  float64(1),
					"ts":    float64(1541152489252),
				}},
			},
			M: map[string]interface{}{
				"op_1_preprocessor_demo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_demo_0_process_latency_us": int64(0),
				"op_1_preprocessor_demo_0_records_in_total":   int64(5),
				"op_1_preprocessor_demo_0_records_out_total":  int64(5),

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
			T: &xstream.PrintableTopo{
				Sources: []string{"source_demo"},
				Edges: map[string][]string{
					"source_demo":            {"op_1_preprocessor_demo"},
					"op_1_preprocessor_demo": {"op_2_project"},
					"op_2_project":           {"sink_mockSink"},
				},
			},
		}, {
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
				"op_1_preprocessor_demo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_demo_0_process_latency_us": int64(0),
				"op_1_preprocessor_demo_0_records_in_total":   int64(5),
				"op_1_preprocessor_demo_0_records_out_total":  int64(5),

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
				"op_1_preprocessor_demo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_demo_0_process_latency_us": int64(0),
				"op_1_preprocessor_demo_0_records_in_total":   int64(5),
				"op_1_preprocessor_demo_0_records_out_total":  int64(5),

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
					"error": "error in preprocessor: invalid data type for size, expect bigint but found string(red)",
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
					"error": "error in preprocessor: invalid data type for size, expect bigint but found string(blue)",
				}},
			},
			M: map[string]interface{}{
				"op_1_preprocessor_demoError_0_exceptions_total":   int64(2),
				"op_1_preprocessor_demoError_0_process_latency_us": int64(0),
				"op_1_preprocessor_demoError_0_records_in_total":   int64(5),
				"op_1_preprocessor_demoError_0_records_out_total":  int64(3),

				"op_3_project_0_exceptions_total":   int64(2),
				"op_3_project_0_process_latency_us": int64(0),
				"op_3_project_0_records_in_total":   int64(4),
				"op_3_project_0_records_out_total":  int64(2),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(4),
				"sink_mockSink_0_records_out_total": int64(4),

				"source_demoError_0_exceptions_total":  int64(0),
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
				"op_1_preprocessor_demo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_demo_0_process_latency_us": int64(0),
				"op_1_preprocessor_demo_0_records_in_total":   int64(5),
				"op_1_preprocessor_demo_0_records_out_total":  int64(5),

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
				"op_1_preprocessor_demo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_demo_0_process_latency_us": int64(0),
				"op_1_preprocessor_demo_0_records_in_total":   int64(5),
				"op_1_preprocessor_demo_0_records_out_total":  int64(5),

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
				"op_1_preprocessor_demo1_0_exceptions_total":   int64(0),
				"op_1_preprocessor_demo1_0_process_latency_us": int64(0),
				"op_1_preprocessor_demo1_0_records_in_total":   int64(5),
				"op_1_preprocessor_demo1_0_records_out_total":  int64(5),

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
				"op_1_preprocessor_demo1_0_exceptions_total":   int64(0),
				"op_1_preprocessor_demo1_0_process_latency_us": int64(0),
				"op_1_preprocessor_demo1_0_records_in_total":   int64(5),
				"op_1_preprocessor_demo1_0_records_out_total":  int64(5),

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
				"op_1_preprocessor_demo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_demo_0_process_latency_us": int64(0),
				"op_1_preprocessor_demo_0_records_in_total":   int64(5),
				"op_1_preprocessor_demo_0_records_out_total":  int64(5),

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
			T: &xstream.PrintableTopo{
				Sources: []string{"source_demo"},
				Edges: map[string][]string{
					"source_demo":            {"op_1_preprocessor_demo"},
					"op_1_preprocessor_demo": {"op_2_project"},
					"op_2_project":           {"sink_mockSink"},
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
			M: map[string]interface{}{
				"op_1_preprocessor_demo_0_exceptions_total":  int64(0),
				"op_1_preprocessor_demo_0_records_in_total":  int64(5),
				"op_1_preprocessor_demo_0_records_out_total": int64(5),

				"op_2_tableprocessor_table1_0_exceptions_total":  int64(0),
				"op_2_tableprocessor_table1_0_records_in_total":  int64(4),
				"op_2_tableprocessor_table1_0_records_out_total": int64(1),

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
				"source_table1_0_records_out_total": int64(4),
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
				"op_1_preprocessor_ldemo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_ldemo_0_process_latency_us": int64(0),
				"op_1_preprocessor_ldemo_0_records_in_total":   int64(5),
				"op_1_preprocessor_ldemo_0_records_out_total":  int64(5),

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
				"op_1_preprocessor_ldemo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_ldemo_0_process_latency_us": int64(0),
				"op_1_preprocessor_ldemo_0_records_in_total":   int64(5),
				"op_1_preprocessor_ldemo_0_records_out_total":  int64(5),

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
				"op_1_preprocessor_ldemo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_ldemo_0_process_latency_us": int64(0),
				"op_1_preprocessor_ldemo_0_records_in_total":   int64(5),
				"op_1_preprocessor_ldemo_0_records_out_total":  int64(5),

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
				"op_1_preprocessor_ldemo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_ldemo_0_process_latency_us": int64(0),
				"op_1_preprocessor_ldemo_0_records_in_total":   int64(5),
				"op_1_preprocessor_ldemo_0_records_out_total":  int64(5),

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
				"op_1_preprocessor_demo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_demo_0_process_latency_us": int64(0),
				"op_1_preprocessor_demo_0_records_in_total":   int64(5),
				"op_1_preprocessor_demo_0_records_out_total":  int64(5),

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
				"op_1_preprocessor_demo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_demo_0_process_latency_us": int64(0),
				"op_1_preprocessor_demo_0_records_in_total":   int64(5),
				"op_1_preprocessor_demo_0_records_out_total":  int64(5),

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
					"self": image,
				}},
			},
			W: 50,
			M: map[string]interface{}{
				"op_1_preprocessor_binDemo_0_exceptions_total":   int64(0),
				"op_1_preprocessor_binDemo_0_process_latency_us": int64(0),
				"op_1_preprocessor_binDemo_0_records_in_total":   int64(1),
				"op_1_preprocessor_binDemo_0_records_out_total":  int64(1),

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
