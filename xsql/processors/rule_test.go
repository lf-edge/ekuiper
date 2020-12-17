package processors

import (
	"encoding/json"
	"github.com/emqx/kuiper/xstream"
	"github.com/emqx/kuiper/xstream/api"
	"testing"
)

func TestSingleSQL(t *testing.T) {
	//Reset
	streamList := []string{"demo", "demoError", "demo1"}
	handleStream(false, streamList, t)
	//Data setup
	var tests = []ruleTest{
		{
			name: `TestSingleSQLRule1`,
			sql:  `SELECT * FROM demo`,
			r: [][]map[string]interface{}{
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
			m: map[string]interface{}{
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
			t: &xstream.PrintableTopo{
				Sources: []string{"source_demo"},
				Edges: map[string][]string{
					"source_demo":            {"op_1_preprocessor_demo"},
					"op_1_preprocessor_demo": {"op_2_project"},
					"op_2_project":           {"sink_mockSink"},
				},
			},
		}, {
			name: `TestSingleSQLRule2`,
			sql:  `SELECT color, ts FROM demo where size > 3`,
			r: [][]map[string]interface{}{
				{{
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
			name: `TestSingleSQLRule3`,
			sql:  `SELECT size as Int8, ts FROM demo where size > 3`,
			r: [][]map[string]interface{}{
				{{
					"Int8": float64(6),
					"ts":   float64(1541152486822),
				}},
				{{
					"Int8": float64(4),
					"ts":   float64(1541152488442),
				}},
			},
			m: map[string]interface{}{
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
			name: `TestSingleSQLRule4`,
			sql:  `SELECT size as Int8, ts FROM demoError where size > 3`,
			r: [][]map[string]interface{}{
				{{
					"error": "error in preprocessor: invalid data type for color, expect string but found int(3)",
				}},
				{{
					"Int8": float64(6),
					"ts":   float64(1541152486822),
				}},
				{{
					"error": "error in preprocessor: invalid data type for color, expect string but found int(7)",
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
				"op_3_project_0_records_in_total":   int64(4),
				"op_3_project_0_records_out_total":  int64(1),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(4),
				"sink_mockSink_0_records_out_total": int64(4),

				"source_demoError_0_exceptions_total":  int64(0),
				"source_demoError_0_records_in_total":  int64(5),
				"source_demoError_0_records_out_total": int64(5),

				"op_2_filter_0_exceptions_total":   int64(3),
				"op_2_filter_0_process_latency_us": int64(0),
				"op_2_filter_0_records_in_total":   int64(5),
				"op_2_filter_0_records_out_total":  int64(1),
			},
		}, {
			name: `TestSingleSQLRule4`,
			sql:  `SELECT size as Int8, ts FROM demoError where size > 3`,
			r: [][]map[string]interface{}{
				{{
					"error": "error in preprocessor: invalid data type for color, expect string but found int(3)",
				}},
				{{
					"Int8": float64(6),
					"ts":   float64(1541152486822),
				}},
				{{
					"error": "error in preprocessor: invalid data type for color, expect string but found int(7)",
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
				"op_3_project_0_records_in_total":   int64(4),
				"op_3_project_0_records_out_total":  int64(1),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(4),
				"sink_mockSink_0_records_out_total": int64(4),

				"source_demoError_0_exceptions_total":  int64(0),
				"source_demoError_0_records_in_total":  int64(5),
				"source_demoError_0_records_out_total": int64(5),

				"op_2_filter_0_exceptions_total":   int64(3),
				"op_2_filter_0_process_latency_us": int64(0),
				"op_2_filter_0_records_in_total":   int64(5),
				"op_2_filter_0_records_out_total":  int64(1),
			},
		}, {
			name: `TestSingleSQLRule5`,
			sql:  `SELECT meta(topic) as m, ts FROM demo`,
			r: [][]map[string]interface{}{
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
			m: map[string]interface{}{
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
			name: `TestSingleSQLRule6`,
			sql:  `SELECT color, ts FROM demo where size > 3 and meta(topic)="mock"`,
			r: [][]map[string]interface{}{
				{{
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
			name: `TestSingleSQLRule7`,
			sql:  "SELECT `from` FROM demo1",
			r: [][]map[string]interface{}{
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
			m: map[string]interface{}{
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
			name: `TestSingleSQLRule8`,
			sql:  "SELECT * FROM demo1 where `from`=\"device1\"",
			r: [][]map[string]interface{}{
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
			m: map[string]interface{}{
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
		},
	}
	handleStream(true, streamList, t)
	options := []*api.RuleOption{
		{
			BufferLength: 100,
		}, {
			BufferLength:       100,
			Qos:                api.AtLeastOnce,
			CheckpointInterval: 5000,
		}, {
			BufferLength:       100,
			Qos:                api.ExactlyOnce,
			CheckpointInterval: 5000,
		},
	}
	for j, opt := range options {
		doRuleTest(t, tests, j, opt)
	}
}

func TestSingleSQLError(t *testing.T) {
	//Reset
	streamList := []string{"ldemo"}
	handleStream(false, streamList, t)
	//Data setup
	var tests = []ruleTest{
		{
			name: `TestSingleSQLErrorRule1`,
			sql:  `SELECT color, ts FROM ldemo where size >= 3`,
			r: [][]map[string]interface{}{
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
			m: map[string]interface{}{
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
			name: `TestSingleSQLErrorRule2`,
			sql:  `SELECT size * 5 FROM ldemo`,
			r: [][]map[string]interface{}{
				{{
					"rengine_field_0": float64(15),
				}},
				{{
					"error": "run Select error: invalid operation string(string) * int64(5)",
				}},
				{{
					"rengine_field_0": float64(15),
				}},
				{{
					"rengine_field_0": float64(10),
				}},
				{{}},
			},
			m: map[string]interface{}{
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
	handleStream(true, streamList, t)
	doRuleTest(t, tests, 0, &api.RuleOption{
		BufferLength: 100,
	})
}

func TestSingleSQLTemplate(t *testing.T) {
	//Reset
	streamList := []string{"demo"}
	handleStream(false, streamList, t)
	//Data setup
	var tests = []ruleTest{
		{
			name: `TestSingleSQLTemplateRule1`,
			sql:  `SELECT * FROM demo`,
			r: []map[string]interface{}{
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
			m: map[string]interface{}{
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
	handleStream(true, streamList, t)
	doRuleTestBySinkProps(t, tests, 0, &api.RuleOption{
		BufferLength: 100,
	}, map[string]interface{}{
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
	handleStream(false, streamList, t)
	//Data setup
	var tests = []ruleTest{
		{
			name: `TestNoneSingleSQLTemplateRule1`,
			sql:  `SELECT * FROM demo`,
			r: [][]byte{
				[]byte("<div>results</div><ul><li>red - 3</li></ul>"),
				[]byte("<div>results</div><ul><li>blue - 6</li></ul>"),
				[]byte("<div>results</div><ul><li>blue - 2</li></ul>"),
				[]byte("<div>results</div><ul><li>yellow - 4</li></ul>"),
				[]byte("<div>results</div><ul><li>red - 1</li></ul>"),
			},
			m: map[string]interface{}{
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
	handleStream(true, streamList, t)
	doRuleTestBySinkProps(t, tests, 0, &api.RuleOption{
		BufferLength: 100,
	}, map[string]interface{}{
		"dataTemplate": `<div>results</div><ul>{{range .}}<li>{{.color}} - {{.size}}</li>{{end}}</ul>`,
	}, func(result [][]byte) interface{} {
		return result
	})
}

func TestSingleSQLForBinary(t *testing.T) {
	//Reset
	streamList := []string{"binDemo"}
	handleStream(false, streamList, t)
	//Data setup
	var tests = []ruleTest{
		{
			name: `TestSingleSQLRule1`,
			sql:  `SELECT * FROM binDemo`,
			r: [][]map[string]interface{}{
				{{
					"self": image,
				}},
			},
			m: map[string]interface{}{
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
	handleStream(true, streamList, t)
	options := []*api.RuleOption{
		{
			BufferLength: 100,
		}, {
			BufferLength:       100,
			Qos:                api.AtLeastOnce,
			CheckpointInterval: 5000,
		}, {
			BufferLength:       100,
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
		doRuleTestBySinkProps(t, tests, j, opt, nil, byteFunc)
	}
}
