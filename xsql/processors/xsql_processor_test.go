package processors

import (
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/nodes"
	"github.com/emqx/kuiper/xstream/test"
	"path"
	"reflect"
	"strings"
	"testing"
	"time"
)

var DbDir = getDbDir()

func getDbDir() string {
	dbDir, err := common.GetAndCreateDataLoc("test")
	if err != nil {
		log.Panic(err)
	}
	log.Infof("db location is %s", dbDir)
	return dbDir
}

func TestStreamCreateProcessor(t *testing.T) {
	var tests = []struct {
		s   string
		r   []string
		err string
	}{
		{
			s: `SHOW STREAMS;`,
			r: []string{"No stream definitions are found."},
		},
		{
			s:   `EXPLAIN STREAM topic1;`,
			err: "Stream topic1 is not found.",
		},
		{
			s: `CREATE STREAM topic1 (
					USERID BIGINT,
					FIRST_NAME STRING,
					LAST_NAME STRING,
					NICKNAMES ARRAY(STRING),
					Gender BOOLEAN,
					ADDRESS STRUCT(STREET_NAME STRING, NUMBER BIGINT),
				) WITH (DATASOURCE="users", FORMAT="AVRO", KEY="USERID");`,
			r: []string{"Stream topic1 is created."},
		},
		{
			s: `CREATE STREAM topic1 (
					USERID BIGINT,
				) WITH (DATASOURCE="users", FORMAT="AVRO", KEY="USERID");`,
			err: "Create stream fails: Item topic1 already exists.",
		},
		{
			s: `EXPLAIN STREAM topic1;`,
			r: []string{"TO BE SUPPORTED"},
		},
		{
			s: `DESCRIBE STREAM topic1;`,
			r: []string{"Fields\n--------------------------------------------------------------------------------\nUSERID\tbigint\nFIRST_NAME\tstring\nLAST_NAME\tstring\nNICKNAMES\t" +
				"array(string)\nGender\tboolean\nADDRESS\tstruct(STREET_NAME string, NUMBER bigint)\n\n" +
				"DATASOURCE: users\nFORMAT: AVRO\nKEY: USERID\n"},
		},
		{
			s: `SHOW STREAMS;`,
			r: []string{"topic1"},
		},
		{
			s: `DROP STREAM topic1;`,
			r: []string{"Stream topic1 is dropped."},
		},
		{
			s:   `DESCRIBE STREAM topic1;`,
			err: "Stream topic1 is not found.",
		},
		{
			s:   `DROP STREAM topic1;`,
			err: "Drop stream fails: topic1 is not found.",
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))

	streamDB := path.Join(getDbDir(), "streamTest")
	for i, tt := range tests {
		results, err := NewStreamProcessor(streamDB).ExecStmt(tt.s)
		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" {
			if !reflect.DeepEqual(tt.r, results) {
				t.Errorf("%d. %q\n\nstmt mismatch:\nexp=%s\ngot=%#v\n\n", i, tt.s, tt.r, results)
			}
		}
	}
}

func createStreams(t *testing.T) {
	p := NewStreamProcessor(path.Join(DbDir, "stream"))
	demo := `CREATE STREAM demo (
					color STRING,
					size BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="demo", FORMAT="json", KEY="ts");`
	_, err := p.ExecStmt(demo)
	if err != nil {
		t.Log(err)
	}
	demoE := `CREATE STREAM demoE (
					color STRING,
					size BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="demoE", FORMAT="json", KEY="ts");`
	_, err = p.ExecStmt(demoE)
	if err != nil {
		t.Log(err)
	}
	demo1 := `CREATE STREAM demo1 (
					temp FLOAT,
					hum BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="demo1", FORMAT="json", KEY="ts");`
	_, err = p.ExecStmt(demo1)
	if err != nil {
		t.Log(err)
	}
	sessionDemo := `CREATE STREAM sessionDemo (
					temp FLOAT,
					hum BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="sessionDemo", FORMAT="json", KEY="ts");`
	_, err = p.ExecStmt(sessionDemo)
	if err != nil {
		t.Log(err)
	}
}

func dropStreams(t *testing.T) {
	p := NewStreamProcessor(path.Join(DbDir, "stream"))
	demo := `DROP STREAM demo`
	_, err := p.ExecStmt(demo)
	if err != nil {
		t.Log(err)
	}
	demoE := `DROP STREAM demoE`
	_, err = p.ExecStmt(demoE)
	if err != nil {
		t.Log(err)
	}
	demo1 := `DROP STREAM demo1`
	_, err = p.ExecStmt(demo1)
	if err != nil {
		t.Log(err)
	}
	sessionDemo := `DROP STREAM sessionDemo`
	_, err = p.ExecStmt(sessionDemo)
	if err != nil {
		t.Log(err)
	}
}

func createSchemalessStreams(t *testing.T) {
	p := NewStreamProcessor(path.Join(DbDir, "stream"))
	demo := `CREATE STREAM ldemo (					
				) WITH (DATASOURCE="ldemo", FORMAT="json");`
	_, err := p.ExecStmt(demo)
	if err != nil {
		t.Log(err)
	}
	demo1 := `CREATE STREAM ldemo1 (
				) WITH (DATASOURCE="ldemo1", FORMAT="json");`
	_, err = p.ExecStmt(demo1)
	if err != nil {
		t.Log(err)
	}
	sessionDemo := `CREATE STREAM lsessionDemo (
				) WITH (DATASOURCE="lsessionDemo", FORMAT="json");`
	_, err = p.ExecStmt(sessionDemo)
	if err != nil {
		t.Log(err)
	}
}

func dropSchemalessStreams(t *testing.T) {
	p := NewStreamProcessor(path.Join(DbDir, "stream"))
	demo := `DROP STREAM ldemo`
	_, err := p.ExecStmt(demo)
	if err != nil {
		t.Log(err)
	}
	demo1 := `DROP STREAM ldemo1`
	_, err = p.ExecStmt(demo1)
	if err != nil {
		t.Log(err)
	}
	sessionDemo := `DROP STREAM lsessionDemo`
	_, err = p.ExecStmt(sessionDemo)
	if err != nil {
		t.Log(err)
	}
}

func getMockSource(name string, done <-chan int, size int) *nodes.SourceNode {
	var data []*xsql.Tuple
	switch name {
	case "demo":
		data = []*xsql.Tuple{
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "red",
					"size":  3,
					"ts":    1541152486013,
				},
				Timestamp: 1541152486013,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "blue",
					"size":  6,
					"ts":    1541152486822,
				},
				Timestamp: 1541152486822,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "blue",
					"size":  2,
					"ts":    1541152487632,
				},
				Timestamp: 1541152487632,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "yellow",
					"size":  4,
					"ts":    1541152488442,
				},
				Timestamp: 1541152488442,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "red",
					"size":  1,
					"ts":    1541152489252,
				},
				Timestamp: 1541152489252,
			},
		}
	case "demoE":
		data = []*xsql.Tuple{
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": 3,
					"size":  "red",
					"ts":    1541152486013,
				},
				Timestamp: 1541152486013,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "blue",
					"size":  6,
					"ts":    "1541152486822",
				},
				Timestamp: 1541152486822,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "blue",
					"size":  2,
					"ts":    1541152487632,
				},
				Timestamp: 1541152487632,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": 7,
					"size":  4,
					"ts":    1541152488442,
				},
				Timestamp: 1541152488442,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "red",
					"size":  "blue",
					"ts":    1541152489252,
				},
				Timestamp: 1541152489252,
			},
		}
	case "demo1":
		data = []*xsql.Tuple{
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 25.5,
					"hum":  65,
					"ts":   1541152486013,
				},
				Timestamp: 1541152486013,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 27.5,
					"hum":  59,
					"ts":   1541152486823,
				},
				Timestamp: 1541152486823,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 28.1,
					"hum":  75,
					"ts":   1541152487632,
				},
				Timestamp: 1541152487632,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 27.4,
					"hum":  80,
					"ts":   1541152488442,
				},
				Timestamp: 1541152488442,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 25.5,
					"hum":  62,
					"ts":   1541152489252,
				},
				Timestamp: 1541152489252,
			},
		}
	case "sessionDemo":
		data = []*xsql.Tuple{
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 25.5,
					"hum":  65,
					"ts":   1541152486013,
				},
				Timestamp: 1541152486013,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 27.5,
					"hum":  59,
					"ts":   1541152486823,
				},
				Timestamp: 1541152486823,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 28.1,
					"hum":  75,
					"ts":   1541152487932,
				},
				Timestamp: 1541152487932,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 27.4,
					"hum":  80,
					"ts":   1541152488442,
				},
				Timestamp: 1541152488442,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 25.5,
					"hum":  62,
					"ts":   1541152489252,
				},
				Timestamp: 1541152489252,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 26.2,
					"hum":  63,
					"ts":   1541152490062,
				},
				Timestamp: 1541152490062,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 26.8,
					"hum":  71,
					"ts":   1541152490872,
				},
				Timestamp: 1541152490872,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 28.9,
					"hum":  85,
					"ts":   1541152491682,
				},
				Timestamp: 1541152491682,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 29.1,
					"hum":  92,
					"ts":   1541152492492,
				},
				Timestamp: 1541152492492,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 32.2,
					"hum":  99,
					"ts":   1541152493202,
				},
				Timestamp: 1541152493202,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 30.9,
					"hum":  87,
					"ts":   1541152494112,
				},
				Timestamp: 1541152494112,
			},
		}
	}
	return nodes.NewSourceNodeWithSource(name, test.NewMockSource(data[:size], done, false), map[string]string{
		"DATASOURCE": name,
	})
}

func TestSingleSQL(t *testing.T) {
	var tests = []struct {
		name string
		sql  string
		r    [][]map[string]interface{}
		s    string
		m    map[string]interface{}
	}{
		{
			name: `rule1`,
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
				"op_preprocessor_demo_0_exceptions_total":   int64(0),
				"op_preprocessor_demo_0_process_latency_ms": int64(0),
				"op_preprocessor_demo_0_records_in_total":   int64(5),
				"op_preprocessor_demo_0_records_out_total":  int64(5),

				"op_project_0_exceptions_total":   int64(0),
				"op_project_0_process_latency_ms": int64(0),
				"op_project_0_records_in_total":   int64(5),
				"op_project_0_records_out_total":  int64(5),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(5),
				"sink_mockSink_0_records_out_total": int64(5),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),
			},
			s: "sink_mockSink_0_records_out_total",
		}, {
			name: `rule2`,
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
			s: "op_filter_0_records_in_total",
			m: map[string]interface{}{
				"op_preprocessor_demo_0_exceptions_total":   int64(0),
				"op_preprocessor_demo_0_process_latency_ms": int64(0),
				"op_preprocessor_demo_0_records_in_total":   int64(5),
				"op_preprocessor_demo_0_records_out_total":  int64(5),

				"op_project_0_exceptions_total":   int64(0),
				"op_project_0_process_latency_ms": int64(0),
				"op_project_0_records_in_total":   int64(2),
				"op_project_0_records_out_total":  int64(2),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(2),
				"sink_mockSink_0_records_out_total": int64(2),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"op_filter_0_exceptions_total":   int64(0),
				"op_filter_0_process_latency_ms": int64(0),
				"op_filter_0_records_in_total":   int64(5),
				"op_filter_0_records_out_total":  int64(2),
			},
		}, {
			name: `rule3`,
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
			s: "op_filter_0_records_in_total",
			m: map[string]interface{}{
				"op_preprocessor_demo_0_exceptions_total":   int64(0),
				"op_preprocessor_demo_0_process_latency_ms": int64(0),
				"op_preprocessor_demo_0_records_in_total":   int64(5),
				"op_preprocessor_demo_0_records_out_total":  int64(5),

				"op_project_0_exceptions_total":   int64(0),
				"op_project_0_process_latency_ms": int64(0),
				"op_project_0_records_in_total":   int64(2),
				"op_project_0_records_out_total":  int64(2),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(2),
				"sink_mockSink_0_records_out_total": int64(2),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"op_filter_0_exceptions_total":   int64(0),
				"op_filter_0_process_latency_ms": int64(0),
				"op_filter_0_records_in_total":   int64(5),
				"op_filter_0_records_out_total":  int64(2),
			},
		}, {
			name: `rule4`,
			sql:  `SELECT size as Int8, ts FROM demoE where size > 3`,
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
			s: "op_filter_0_records_in_total",
			m: map[string]interface{}{
				"op_preprocessor_demoE_0_exceptions_total":   int64(3),
				"op_preprocessor_demoE_0_process_latency_ms": int64(0),
				"op_preprocessor_demoE_0_records_in_total":   int64(5),
				"op_preprocessor_demoE_0_records_out_total":  int64(2),

				"op_project_0_exceptions_total":   int64(3),
				"op_project_0_process_latency_ms": int64(0),
				"op_project_0_records_in_total":   int64(4),
				"op_project_0_records_out_total":  int64(1),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(4),
				"sink_mockSink_0_records_out_total": int64(4),

				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(5),
				"source_demoE_0_records_out_total": int64(5),

				"op_filter_0_exceptions_total":   int64(3),
				"op_filter_0_process_latency_ms": int64(0),
				"op_filter_0_records_in_total":   int64(5),
				"op_filter_0_records_out_total":  int64(1),
			},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	createStreams(t)
	defer dropStreams(t)
	//defer close(done)
	for i, tt := range tests {
		test.ResetClock(1541152486000)
		p := NewRuleProcessor(DbDir)
		parser := xsql.NewParser(strings.NewReader(tt.sql))
		var (
			sources []*nodes.SourceNode
			syncs   []chan int
		)
		if stmt, err := xsql.Language.Parse(parser); err != nil {
			t.Errorf("parse sql %s error: %s", tt.sql, err)
		} else {
			if selectStmt, ok := stmt.(*xsql.SelectStatement); !ok {
				t.Errorf("sql %s is not a select statement", tt.sql)
			} else {
				streams := xsql.GetStreams(selectStmt)
				for _, stream := range streams {
					next := make(chan int)
					syncs = append(syncs, next)
					source := getMockSource(stream, next, 5)
					sources = append(sources, source)
				}
			}
		}
		tp, inputs, err := p.createTopoWithSources(&api.Rule{Id: tt.name, Sql: tt.sql, Options: map[string]interface{}{
			"bufferLength": float64(100),
		}}, sources)
		if err != nil {
			t.Error(err)
		}
		mockSink := test.NewMockSink()
		sink := nodes.NewSinkNodeWithSink("mockSink", mockSink)
		tp.AddSink(inputs, sink)
		errCh := tp.Open()
		func() {
			for i := 0; i < 5; i++ {
				syncs[i%len(syncs)] <- i
				select {
				case err = <-errCh:
					t.Log(err)
					tp.Cancel()
					return
				default:
				}
			}
			for retry := 100; retry > 0; retry-- {
				if err := compareMetrics(tp, tt.m, tt.sql); err == nil {
					break
				}
				time.Sleep(time.Duration(retry) * time.Millisecond)
			}
		}()
		results := mockSink.GetResults()
		var maps [][]map[string]interface{}
		for _, v := range results {
			var mapRes []map[string]interface{}
			err := json.Unmarshal(v, &mapRes)
			if err != nil {
				t.Errorf("Failed to parse the input into map")
				continue
			}
			maps = append(maps, mapRes)
		}
		if !reflect.DeepEqual(tt.r, maps) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.r, maps)
			continue
		}
		if err := compareMetrics(tp, tt.m, tt.sql); err != nil {
			t.Errorf("%d. %q\n\n%v", i, tt.sql, err)
		}
		tp.Cancel()
	}
}

func getMockSourceL(name string, done <-chan int, size int) *nodes.SourceNode {
	var data []*xsql.Tuple
	switch name {
	case "ldemo":
		data = []*xsql.Tuple{
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "red",
					"size":  3,
					"ts":    1541152486013,
				},
				Timestamp: 1541152486013,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "blue",
					"size":  "string",
					"ts":    1541152486822,
				},
				Timestamp: 1541152486822,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"size": 3,
					"ts":   1541152487632,
				},
				Timestamp: 1541152487632,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": 49,
					"size":  2,
					"ts":    1541152488442,
				},
				Timestamp: 1541152488442,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "red",
					"ts":    1541152489252,
				},
				Timestamp: 1541152489252,
			},
		}
	case "ldemo1":
		data = []*xsql.Tuple{
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 25.5,
					"hum":  65,
					"ts":   1541152486013,
				},
				Timestamp: 1541152486013,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 27.5,
					"hum":  59,
					"ts":   1541152486823,
				},
				Timestamp: 1541152486823,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 28.1,
					"hum":  75,
					"ts":   1541152487632,
				},
				Timestamp: 1541152487632,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 27.4,
					"hum":  80,
					"ts":   "1541152488442",
				},
				Timestamp: 1541152488442,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 25.5,
					"hum":  62,
					"ts":   1541152489252,
				},
				Timestamp: 1541152489252,
			},
		}
	case "lsessionDemo":
		data = []*xsql.Tuple{
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 25.5,
					"hum":  65,
					"ts":   1541152486013,
				},
				Timestamp: 1541152486013,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 27.5,
					"hum":  59,
					"ts":   1541152486823,
				},
				Timestamp: 1541152486823,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 28.1,
					"hum":  75,
					"ts":   1541152487932,
				},
				Timestamp: 1541152487932,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 27.4,
					"hum":  80,
					"ts":   1541152488442,
				},
				Timestamp: 1541152488442,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 25.5,
					"hum":  62,
					"ts":   1541152489252,
				},
				Timestamp: 1541152489252,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 26.2,
					"hum":  63,
					"ts":   1541152490062,
				},
				Timestamp: 1541152490062,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 26.8,
					"hum":  71,
					"ts":   1541152490872,
				},
				Timestamp: 1541152490872,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 28.9,
					"hum":  85,
					"ts":   1541152491682,
				},
				Timestamp: 1541152491682,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 29.1,
					"hum":  92,
					"ts":   1541152492492,
				},
				Timestamp: 1541152492492,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 2.2,
					"hum":  99,
					"ts":   1541152493202,
				},
				Timestamp: 1541152493202,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 30.9,
					"hum":  87,
					"ts":   1541152494112,
				},
				Timestamp: 1541152494112,
			},
		}
	}
	return nodes.NewSourceNodeWithSource(name, test.NewMockSource(data[:size], done, false), map[string]string{
		"DATASOURCE": name,
	})
}
func TestSingleSQLError(t *testing.T) {
	var tests = []struct {
		name string
		sql  string
		r    [][]map[string]interface{}
		s    string
		m    map[string]interface{}
	}{
		{
			name: `rule1`,
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
			s: "op_filter_0_records_in_total",
			m: map[string]interface{}{
				"op_preprocessor_ldemo_0_exceptions_total":   int64(0),
				"op_preprocessor_ldemo_0_process_latency_ms": int64(0),
				"op_preprocessor_ldemo_0_records_in_total":   int64(5),
				"op_preprocessor_ldemo_0_records_out_total":  int64(5),

				"op_project_0_exceptions_total":   int64(1),
				"op_project_0_process_latency_ms": int64(0),
				"op_project_0_records_in_total":   int64(3),
				"op_project_0_records_out_total":  int64(2),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),

				"source_ldemo_0_exceptions_total":  int64(0),
				"source_ldemo_0_records_in_total":  int64(5),
				"source_ldemo_0_records_out_total": int64(5),

				"op_filter_0_exceptions_total":   int64(1),
				"op_filter_0_process_latency_ms": int64(0),
				"op_filter_0_records_in_total":   int64(5),
				"op_filter_0_records_out_total":  int64(2),
			},
		}, {
			name: `rule2`,
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
			s: "op_filter_0_records_in_total",
			m: map[string]interface{}{
				"op_preprocessor_ldemo_0_exceptions_total":   int64(0),
				"op_preprocessor_ldemo_0_process_latency_ms": int64(0),
				"op_preprocessor_ldemo_0_records_in_total":   int64(5),
				"op_preprocessor_ldemo_0_records_out_total":  int64(5),

				"op_project_0_exceptions_total":   int64(1),
				"op_project_0_process_latency_ms": int64(0),
				"op_project_0_records_in_total":   int64(5),
				"op_project_0_records_out_total":  int64(4),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(5),
				"sink_mockSink_0_records_out_total": int64(5),

				"source_ldemo_0_exceptions_total":  int64(0),
				"source_ldemo_0_records_in_total":  int64(5),
				"source_ldemo_0_records_out_total": int64(5),
			},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	createSchemalessStreams(t)
	defer dropSchemalessStreams(t)
	//defer close(done)
	for i, tt := range tests {
		test.ResetClock(1541152486000)
		p := NewRuleProcessor(DbDir)
		parser := xsql.NewParser(strings.NewReader(tt.sql))
		var (
			sources []*nodes.SourceNode
			syncs   []chan int
		)
		if stmt, err := xsql.Language.Parse(parser); err != nil {
			t.Errorf("parse sql %s error: %s", tt.sql, err)
		} else {
			if selectStmt, ok := stmt.(*xsql.SelectStatement); !ok {
				t.Errorf("sql %s is not a select statement", tt.sql)
			} else {
				streams := xsql.GetStreams(selectStmt)
				for _, stream := range streams {
					next := make(chan int)
					syncs = append(syncs, next)
					source := getMockSourceL(stream, next, 5)
					sources = append(sources, source)
				}
			}
		}
		tp, inputs, err := p.createTopoWithSources(&api.Rule{Id: tt.name, Sql: tt.sql, Options: map[string]interface{}{
			"bufferLength": float64(100),
		}}, sources)
		if err != nil {
			t.Error(err)
		}
		mockSink := test.NewMockSink()
		sink := nodes.NewSinkNodeWithSink("mockSink", mockSink)
		tp.AddSink(inputs, sink)
		errCh := tp.Open()
		func() {
			for i := 0; i < 5; i++ {
				syncs[i%len(syncs)] <- i
				select {
				case err = <-errCh:
					t.Log(err)
					tp.Cancel()
					return
				default:
				}
			}
			for retry := 100; retry > 0; retry-- {
				if err := compareMetrics(tp, tt.m, tt.sql); err == nil {
					break
				}
				time.Sleep(time.Duration(retry) * time.Millisecond)
			}
		}()
		results := mockSink.GetResults()
		var maps [][]map[string]interface{}
		for _, v := range results {
			var mapRes []map[string]interface{}
			err := json.Unmarshal(v, &mapRes)
			if err != nil {
				t.Errorf("Failed to parse the input into map")
				continue
			}
			maps = append(maps, mapRes)
		}
		if !reflect.DeepEqual(tt.r, maps) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.r, maps)
			continue
		}
		if err := compareMetrics(tp, tt.m, tt.sql); err != nil {
			t.Errorf("%d. %q\n\n%v", i, tt.sql, err)
		}
		tp.Cancel()
	}
}

func TestWindow(t *testing.T) {
	var tests = []struct {
		name string
		sql  string
		size int
		r    [][]map[string]interface{}
		m    map[string]interface{}
	}{
		{
			name: `rule1`,
			sql:  `SELECT * FROM demo GROUP BY HOPPINGWINDOW(ss, 2, 1)`,
			size: 5,
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
			},
			m: map[string]interface{}{
				"op_preprocessor_demo_0_exceptions_total":   int64(0),
				"op_preprocessor_demo_0_process_latency_ms": int64(0),
				"op_preprocessor_demo_0_records_in_total":   int64(5),
				"op_preprocessor_demo_0_records_out_total":  int64(5),

				"op_project_0_exceptions_total":   int64(0),
				"op_project_0_process_latency_ms": int64(0),
				"op_project_0_records_in_total":   int64(3),
				"op_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"op_window_0_exceptions_total":   int64(0),
				"op_window_0_process_latency_ms": int64(0),
				"op_window_0_records_in_total":   int64(5),
				"op_window_0_records_out_total":  int64(3),
			},
		}, {
			name: `rule2`,
			sql:  `SELECT color, ts FROM demo where size > 2 GROUP BY tumblingwindow(ss, 1)`,
			size: 5,
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
				"op_preprocessor_demo_0_exceptions_total":   int64(0),
				"op_preprocessor_demo_0_process_latency_ms": int64(0),
				"op_preprocessor_demo_0_records_in_total":   int64(5),
				"op_preprocessor_demo_0_records_out_total":  int64(5),

				"op_project_0_exceptions_total":   int64(0),
				"op_project_0_process_latency_ms": int64(0),
				"op_project_0_records_in_total":   int64(2),
				"op_project_0_records_out_total":  int64(2),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(2),
				"sink_mockSink_0_records_out_total": int64(2),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"op_window_0_exceptions_total":   int64(0),
				"op_window_0_process_latency_ms": int64(0),
				"op_window_0_records_in_total":   int64(5),
				"op_window_0_records_out_total":  int64(3),

				"op_filter_0_exceptions_total":   int64(0),
				"op_filter_0_process_latency_ms": int64(0),
				"op_filter_0_records_in_total":   int64(3),
				"op_filter_0_records_out_total":  int64(2),
			},
		}, {
			name: `rule3`,
			sql:  `SELECT color, temp, ts FROM demo INNER JOIN demo1 ON demo.ts = demo1.ts GROUP BY SlidingWindow(ss, 1)`,
			size: 5,
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
				"op_preprocessor_demo_0_exceptions_total":   int64(0),
				"op_preprocessor_demo_0_process_latency_ms": int64(0),
				"op_preprocessor_demo_0_records_in_total":   int64(5),
				"op_preprocessor_demo_0_records_out_total":  int64(5),

				"op_preprocessor_demo1_0_exceptions_total":   int64(0),
				"op_preprocessor_demo1_0_process_latency_ms": int64(0),
				"op_preprocessor_demo1_0_records_in_total":   int64(5),
				"op_preprocessor_demo1_0_records_out_total":  int64(5),

				"op_project_0_exceptions_total":   int64(0),
				"op_project_0_process_latency_ms": int64(0),
				"op_project_0_records_in_total":   int64(8),
				"op_project_0_records_out_total":  int64(8),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(8),
				"sink_mockSink_0_records_out_total": int64(8),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"source_demo1_0_exceptions_total":  int64(0),
				"source_demo1_0_records_in_total":  int64(5),
				"source_demo1_0_records_out_total": int64(5),

				"op_window_0_exceptions_total":   int64(0),
				"op_window_0_process_latency_ms": int64(0),
				"op_window_0_records_in_total":   int64(10),
				"op_window_0_records_out_total":  int64(10),

				"op_join_0_exceptions_total":   int64(0),
				"op_join_0_process_latency_ms": int64(0),
				"op_join_0_records_in_total":   int64(10),
				"op_join_0_records_out_total":  int64(8),
			},
		}, {
			name: `rule4`,
			sql:  `SELECT color FROM demo GROUP BY SlidingWindow(ss, 2), color ORDER BY color`,
			size: 5,
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
				"op_preprocessor_demo_0_exceptions_total":   int64(0),
				"op_preprocessor_demo_0_process_latency_ms": int64(0),
				"op_preprocessor_demo_0_records_in_total":   int64(5),
				"op_preprocessor_demo_0_records_out_total":  int64(5),

				"op_project_0_exceptions_total":   int64(0),
				"op_project_0_process_latency_ms": int64(0),
				"op_project_0_records_in_total":   int64(5),
				"op_project_0_records_out_total":  int64(5),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(5),
				"sink_mockSink_0_records_out_total": int64(5),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"op_window_0_exceptions_total":   int64(0),
				"op_window_0_process_latency_ms": int64(0),
				"op_window_0_records_in_total":   int64(5),
				"op_window_0_records_out_total":  int64(5),

				"op_aggregate_0_exceptions_total":   int64(0),
				"op_aggregate_0_process_latency_ms": int64(0),
				"op_aggregate_0_records_in_total":   int64(5),
				"op_aggregate_0_records_out_total":  int64(5),

				"op_order_0_exceptions_total":   int64(0),
				"op_order_0_process_latency_ms": int64(0),
				"op_order_0_records_in_total":   int64(5),
				"op_order_0_records_out_total":  int64(5),
			},
		}, {
			name: `rule5`,
			sql:  `SELECT temp FROM sessionDemo GROUP BY SessionWindow(ss, 2, 1) `,
			size: 11,
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
				}},
			},
			m: map[string]interface{}{
				"op_preprocessor_sessionDemo_0_exceptions_total":   int64(0),
				"op_preprocessor_sessionDemo_0_process_latency_ms": int64(0),
				"op_preprocessor_sessionDemo_0_records_in_total":   int64(11),
				"op_preprocessor_sessionDemo_0_records_out_total":  int64(11),

				"op_project_0_exceptions_total":   int64(0),
				"op_project_0_process_latency_ms": int64(0),
				"op_project_0_records_in_total":   int64(3),
				"op_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),

				"source_sessionDemo_0_exceptions_total":  int64(0),
				"source_sessionDemo_0_records_in_total":  int64(11),
				"source_sessionDemo_0_records_out_total": int64(11),

				"op_window_0_exceptions_total":   int64(0),
				"op_window_0_process_latency_ms": int64(0),
				"op_window_0_records_in_total":   int64(11),
				"op_window_0_records_out_total":  int64(3),
			},
		}, {
			name: `rule6`,
			sql:  `SELECT max(temp) as m, count(color) as c FROM demo INNER JOIN demo1 ON demo.ts = demo1.ts GROUP BY SlidingWindow(ss, 1)`,
			size: 5,
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
				"op_preprocessor_demo_0_exceptions_total":   int64(0),
				"op_preprocessor_demo_0_process_latency_ms": int64(0),
				"op_preprocessor_demo_0_records_in_total":   int64(5),
				"op_preprocessor_demo_0_records_out_total":  int64(5),

				"op_preprocessor_demo1_0_exceptions_total":   int64(0),
				"op_preprocessor_demo1_0_process_latency_ms": int64(0),
				"op_preprocessor_demo1_0_records_in_total":   int64(5),
				"op_preprocessor_demo1_0_records_out_total":  int64(5),

				"op_project_0_exceptions_total":   int64(0),
				"op_project_0_process_latency_ms": int64(0),
				"op_project_0_records_in_total":   int64(8),
				"op_project_0_records_out_total":  int64(8),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(8),
				"sink_mockSink_0_records_out_total": int64(8),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"source_demo1_0_exceptions_total":  int64(0),
				"source_demo1_0_records_in_total":  int64(5),
				"source_demo1_0_records_out_total": int64(5),

				"op_window_0_exceptions_total":   int64(0),
				"op_window_0_process_latency_ms": int64(0),
				"op_window_0_records_in_total":   int64(10),
				"op_window_0_records_out_total":  int64(10),

				"op_join_0_exceptions_total":   int64(0),
				"op_join_0_process_latency_ms": int64(0),
				"op_join_0_records_in_total":   int64(10),
				"op_join_0_records_out_total":  int64(8),
			},
		}, {
			name: `rule7`,
			sql:  `SELECT * FROM demoE GROUP BY HOPPINGWINDOW(ss, 2, 1)`,
			size: 5,
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
				"op_preprocessor_demoE_0_exceptions_total":   int64(3),
				"op_preprocessor_demoE_0_process_latency_ms": int64(0),
				"op_preprocessor_demoE_0_records_in_total":   int64(5),
				"op_preprocessor_demoE_0_records_out_total":  int64(2),

				"op_project_0_exceptions_total":   int64(3),
				"op_project_0_process_latency_ms": int64(0),
				"op_project_0_records_in_total":   int64(6),
				"op_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(6),
				"sink_mockSink_0_records_out_total": int64(6),

				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(5),
				"source_demoE_0_records_out_total": int64(5),

				"op_window_0_exceptions_total":   int64(3),
				"op_window_0_process_latency_ms": int64(0),
				"op_window_0_records_in_total":   int64(5),
				"op_window_0_records_out_total":  int64(3),
			},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	createStreams(t)
	defer dropStreams(t)
	for i, tt := range tests {
		test.ResetClock(1541152486000)
		p := NewRuleProcessor(DbDir)
		parser := xsql.NewParser(strings.NewReader(tt.sql))
		var (
			sources []*nodes.SourceNode
			syncs   []chan int
		)
		if stmt, err := xsql.Language.Parse(parser); err != nil {
			t.Errorf("parse sql %s error: %s", tt.sql, err)
		} else {
			if selectStmt, ok := stmt.(*xsql.SelectStatement); !ok {
				t.Errorf("sql %s is not a select statement", tt.sql)
			} else {
				streams := xsql.GetStreams(selectStmt)
				for _, stream := range streams {
					next := make(chan int)
					syncs = append(syncs, next)
					source := getMockSource(stream, next, tt.size)
					sources = append(sources, source)
				}
			}
		}
		tp, inputs, err := p.createTopoWithSources(&api.Rule{Id: tt.name, Sql: tt.sql}, sources)
		if err != nil {
			t.Error(err)
		}
		mockSink := test.NewMockSink()
		sink := nodes.NewSinkNodeWithSink("mockSink", mockSink)
		tp.AddSink(inputs, sink)
		errCh := tp.Open()
		func() {
			for i := 0; i < tt.size*len(syncs); i++ {
				syncs[i%len(syncs)] <- i
				for {
					time.Sleep(1)
					if getMetric(tp, "op_window_0_records_in_total") == (i + 1) {
						break
					}
				}
				select {
				case err = <-errCh:
					t.Log(err)
					tp.Cancel()
					return
				default:
				}
			}
			retry := 100
			for ; retry > 0; retry-- {
				if err := compareMetrics(tp, tt.m, tt.sql); err == nil {
					break
				}
				t.Logf("wait to try another %d times", retry)
				time.Sleep(time.Duration(retry) * time.Millisecond)
			}
			if retry == 0 {
				err := compareMetrics(tp, tt.m, tt.sql)
				t.Errorf("could not get correct metrics: %v", err)
			}
		}()
		results := mockSink.GetResults()
		var maps [][]map[string]interface{}
		for _, v := range results {
			var mapRes []map[string]interface{}
			err := json.Unmarshal(v, &mapRes)
			if err != nil {
				t.Errorf("Failed to parse the input into map")
				continue
			}
			maps = append(maps, mapRes)
		}
		if !reflect.DeepEqual(tt.r, maps) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.r, maps)
		}
		if err := compareMetrics(tp, tt.m, tt.sql); err != nil {
			t.Errorf("%d. %q\n\n%v", i, tt.sql, err)
		}
		tp.Cancel()
	}
}

func TestWindowError(t *testing.T) {
	var tests = []struct {
		name string
		sql  string
		size int
		r    [][]map[string]interface{}
		m    map[string]interface{}
	}{
		{
			name: `rule1`,
			sql:  `SELECT size * 3 FROM ldemo GROUP BY TUMBLINGWINDOW(ss, 2)`,
			size: 5,
			r: [][]map[string]interface{}{
				{{
					"error": "run Select error: invalid operation string(string) * int64(3)",
				}},
			},
			m: map[string]interface{}{
				"op_preprocessor_ldemo_0_exceptions_total":   int64(0),
				"op_preprocessor_ldemo_0_process_latency_ms": int64(0),
				"op_preprocessor_ldemo_0_records_in_total":   int64(5),
				"op_preprocessor_ldemo_0_records_out_total":  int64(5),

				"op_project_0_exceptions_total":   int64(1),
				"op_project_0_process_latency_ms": int64(0),
				"op_project_0_records_in_total":   int64(1),
				"op_project_0_records_out_total":  int64(0),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(1),
				"sink_mockSink_0_records_out_total": int64(1),

				"source_ldemo_0_exceptions_total":  int64(0),
				"source_ldemo_0_records_in_total":  int64(5),
				"source_ldemo_0_records_out_total": int64(5),

				"op_window_0_exceptions_total":   int64(0),
				"op_window_0_process_latency_ms": int64(0),
				"op_window_0_records_in_total":   int64(5),
				"op_window_0_records_out_total":  int64(1),
			},
		}, {
			name: `rule2`,
			sql:  `SELECT color, ts FROM ldemo where size > 2 GROUP BY tumblingwindow(ss, 1)`,
			size: 5,
			r: [][]map[string]interface{}{
				{{
					"error": "run Where error: invalid operation string(string) > int64(2)",
				}}, {{
					"ts": float64(1541152487632),
				}},
			},
			m: map[string]interface{}{
				"op_preprocessor_ldemo_0_exceptions_total":   int64(0),
				"op_preprocessor_ldemo_0_process_latency_ms": int64(0),
				"op_preprocessor_ldemo_0_records_in_total":   int64(5),
				"op_preprocessor_ldemo_0_records_out_total":  int64(5),

				"op_project_0_exceptions_total":   int64(1),
				"op_project_0_process_latency_ms": int64(0),
				"op_project_0_records_in_total":   int64(2),
				"op_project_0_records_out_total":  int64(1),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(2),
				"sink_mockSink_0_records_out_total": int64(2),

				"source_ldemo_0_exceptions_total":  int64(0),
				"source_ldemo_0_records_in_total":  int64(5),
				"source_ldemo_0_records_out_total": int64(5),

				"op_window_0_exceptions_total":   int64(0),
				"op_window_0_process_latency_ms": int64(0),
				"op_window_0_records_in_total":   int64(5),
				"op_window_0_records_out_total":  int64(3),

				"op_filter_0_exceptions_total":   int64(1),
				"op_filter_0_process_latency_ms": int64(0),
				"op_filter_0_records_in_total":   int64(3),
				"op_filter_0_records_out_total":  int64(1),
			},
		}, {
			name: `rule3`,
			sql:  `SELECT color, temp, ts FROM ldemo INNER JOIN ldemo1 ON ldemo.ts = ldemo1.ts GROUP BY SlidingWindow(ss, 1)`,
			size: 5,
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
				"op_preprocessor_ldemo_0_exceptions_total":   int64(0),
				"op_preprocessor_ldemo_0_process_latency_ms": int64(0),
				"op_preprocessor_ldemo_0_records_in_total":   int64(5),
				"op_preprocessor_ldemo_0_records_out_total":  int64(5),

				"op_preprocessor_ldemo1_0_exceptions_total":   int64(0),
				"op_preprocessor_ldemo1_0_process_latency_ms": int64(0),
				"op_preprocessor_ldemo1_0_records_in_total":   int64(5),
				"op_preprocessor_ldemo1_0_records_out_total":  int64(5),

				"op_project_0_exceptions_total":   int64(3),
				"op_project_0_process_latency_ms": int64(0),
				"op_project_0_records_in_total":   int64(8),
				"op_project_0_records_out_total":  int64(5),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(8),
				"sink_mockSink_0_records_out_total": int64(8),

				"source_ldemo_0_exceptions_total":  int64(0),
				"source_ldemo_0_records_in_total":  int64(5),
				"source_ldemo_0_records_out_total": int64(5),

				"source_ldemo1_0_exceptions_total":  int64(0),
				"source_ldemo1_0_records_in_total":  int64(5),
				"source_ldemo1_0_records_out_total": int64(5),

				"op_window_0_exceptions_total":   int64(0),
				"op_window_0_process_latency_ms": int64(0),
				"op_window_0_records_in_total":   int64(10),
				"op_window_0_records_out_total":  int64(10),

				"op_join_0_exceptions_total":   int64(3),
				"op_join_0_process_latency_ms": int64(0),
				"op_join_0_records_in_total":   int64(10),
				"op_join_0_records_out_total":  int64(5),
			},
		}, {
			name: `rule4`,
			sql:  `SELECT color FROM ldemo GROUP BY SlidingWindow(ss, 2), color having size >= 2`,
			size: 5,
			r: [][]map[string]interface{}{
				{{
					"color": "red",
				}}, {{
					"error": "run Having error: invalid operation string(string) >= int64(2)",
				}}, {{
					"error": "run Having error: invalid operation string(string) >= int64(2)",
				}}, {{
					"error": "run Having error: invalid operation string(string) >= int64(2)",
				}}, {{}, {
					"color": float64(49),
				}},
			},
			m: map[string]interface{}{
				"op_preprocessor_ldemo_0_exceptions_total":   int64(0),
				"op_preprocessor_ldemo_0_process_latency_ms": int64(0),
				"op_preprocessor_ldemo_0_records_in_total":   int64(5),
				"op_preprocessor_ldemo_0_records_out_total":  int64(5),

				"op_project_0_exceptions_total":   int64(3),
				"op_project_0_process_latency_ms": int64(0),
				"op_project_0_records_in_total":   int64(5),
				"op_project_0_records_out_total":  int64(2),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(5),
				"sink_mockSink_0_records_out_total": int64(5),

				"source_ldemo_0_exceptions_total":  int64(0),
				"source_ldemo_0_records_in_total":  int64(5),
				"source_ldemo_0_records_out_total": int64(5),

				"op_window_0_exceptions_total":   int64(0),
				"op_window_0_process_latency_ms": int64(0),
				"op_window_0_records_in_total":   int64(5),
				"op_window_0_records_out_total":  int64(5),

				"op_aggregate_0_exceptions_total":   int64(0),
				"op_aggregate_0_process_latency_ms": int64(0),
				"op_aggregate_0_records_in_total":   int64(5),
				"op_aggregate_0_records_out_total":  int64(5),

				"op_having_0_exceptions_total":   int64(3),
				"op_having_0_process_latency_ms": int64(0),
				"op_having_0_records_in_total":   int64(5),
				"op_having_0_records_out_total":  int64(2),
			},
		}, {
			name: `rule5`,
			sql:  `SELECT color, size FROM ldemo GROUP BY tumblingwindow(ss, 1) ORDER BY size`,
			size: 5,
			r: [][]map[string]interface{}{
				{{
					"error": "run Order By error: incompatible types for comparison: int and string",
				}}, {{
					"size": float64(3),
				}}, {{
					"color": float64(49),
					"size":  float64(2),
				}},
			},
			m: map[string]interface{}{
				"op_preprocessor_ldemo_0_exceptions_total":   int64(0),
				"op_preprocessor_ldemo_0_process_latency_ms": int64(0),
				"op_preprocessor_ldemo_0_records_in_total":   int64(5),
				"op_preprocessor_ldemo_0_records_out_total":  int64(5),

				"op_project_0_exceptions_total":   int64(1),
				"op_project_0_process_latency_ms": int64(0),
				"op_project_0_records_in_total":   int64(3),
				"op_project_0_records_out_total":  int64(2),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),

				"source_ldemo_0_exceptions_total":  int64(0),
				"source_ldemo_0_records_in_total":  int64(5),
				"source_ldemo_0_records_out_total": int64(5),

				"op_window_0_exceptions_total":   int64(0),
				"op_window_0_process_latency_ms": int64(0),
				"op_window_0_records_in_total":   int64(5),
				"op_window_0_records_out_total":  int64(3),

				"op_order_0_exceptions_total":   int64(1),
				"op_order_0_process_latency_ms": int64(0),
				"op_order_0_records_in_total":   int64(3),
				"op_order_0_records_out_total":  int64(2),
			},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	createSchemalessStreams(t)
	defer dropSchemalessStreams(t)
	for i, tt := range tests {
		test.ResetClock(1541152486000)
		p := NewRuleProcessor(DbDir)
		parser := xsql.NewParser(strings.NewReader(tt.sql))
		var (
			sources []*nodes.SourceNode
			syncs   []chan int
		)
		if stmt, err := xsql.Language.Parse(parser); err != nil {
			t.Errorf("parse sql %s error: %s", tt.sql, err)
		} else {
			if selectStmt, ok := stmt.(*xsql.SelectStatement); !ok {
				t.Errorf("sql %s is not a select statement", tt.sql)
			} else {
				streams := xsql.GetStreams(selectStmt)
				for _, stream := range streams {
					next := make(chan int)
					syncs = append(syncs, next)
					source := getMockSourceL(stream, next, tt.size)
					sources = append(sources, source)
				}
			}
		}
		tp, inputs, err := p.createTopoWithSources(&api.Rule{Id: tt.name, Sql: tt.sql}, sources)
		if err != nil {
			t.Error(err)
		}
		mockSink := test.NewMockSink()
		sink := nodes.NewSinkNodeWithSink("mockSink", mockSink)
		tp.AddSink(inputs, sink)
		errCh := tp.Open()
		func() {
			for i := 0; i < tt.size*len(syncs); i++ {
				syncs[i%len(syncs)] <- i
				for {
					time.Sleep(1)
					if getMetric(tp, "op_window_0_records_in_total") == (i + 1) {
						break
					}
				}
				select {
				case err = <-errCh:
					t.Log(err)
					tp.Cancel()
					return
				default:
				}
			}
			retry := 100
			for ; retry > 0; retry-- {
				if err := compareMetrics(tp, tt.m, tt.sql); err == nil {
					break
				}
				t.Logf("wait to try another %d times", retry)
				time.Sleep(time.Duration(retry) * time.Millisecond)
			}
			if retry == 0 {
				err := compareMetrics(tp, tt.m, tt.sql)
				t.Errorf("could not get correct metrics: %v", err)
			}
		}()
		results := mockSink.GetResults()
		var maps [][]map[string]interface{}
		for _, v := range results {
			var mapRes []map[string]interface{}
			err := json.Unmarshal(v, &mapRes)
			if err != nil {
				t.Errorf("Failed to parse the input into map")
				continue
			}
			maps = append(maps, mapRes)
		}
		if !reflect.DeepEqual(tt.r, maps) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.r, maps)
		}
		if err := compareMetrics(tp, tt.m, tt.sql); err != nil {
			t.Errorf("%d. %q\n\n%v", i, tt.sql, err)
		}
		tp.Cancel()
	}
}

func createEventStreams(t *testing.T) {
	p := NewStreamProcessor(path.Join(DbDir, "stream"))
	demo := `CREATE STREAM demoE (
					color STRING,
					size BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="demoE", FORMAT="json", KEY="ts", TIMESTAMP="ts");`
	_, err := p.ExecStmt(demo)
	if err != nil {
		t.Log(err)
	}
	demo1 := `CREATE STREAM demo1E (
					temp FLOAT,
					hum BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="demo1E", FORMAT="json", KEY="ts", TIMESTAMP="ts");`
	_, err = p.ExecStmt(demo1)
	if err != nil {
		t.Log(err)
	}
	sessionDemo := `CREATE STREAM sessionDemoE (
					temp FLOAT,
					hum BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="sessionDemoE", FORMAT="json", KEY="ts", TIMESTAMP="ts");`
	_, err = p.ExecStmt(sessionDemo)
	if err != nil {
		t.Log(err)
	}
	demoErr := `CREATE STREAM demoErr (
					color STRING,
					size BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="demoErr", FORMAT="json", KEY="ts", TIMESTAMP="ts");`
	_, err = p.ExecStmt(demoErr)
	if err != nil {
		t.Log(err)
	}
}

func dropEventStreams(t *testing.T) {
	p := NewStreamProcessor(path.Join(DbDir, "stream"))
	demo := `DROP STREAM demoE`
	_, err := p.ExecStmt(demo)
	if err != nil {
		t.Log(err)
	}
	demo1 := `DROP STREAM demo1E`
	_, err = p.ExecStmt(demo1)
	if err != nil {
		t.Log(err)
	}
	sessionDemo := `DROP STREAM sessionDemoE`
	_, err = p.ExecStmt(sessionDemo)
	if err != nil {
		t.Log(err)
	}
	demoErr := `DROP STREAM demoErr`
	_, err = p.ExecStmt(demoErr)
	if err != nil {
		t.Log(err)
	}
}

func getEventMockSource(name string, done <-chan int, size int) *nodes.SourceNode {
	var data []*xsql.Tuple
	switch name {
	case "demoE":
		data = []*xsql.Tuple{
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "red",
					"size":  3,
					"ts":    1541152486013,
				},
				Timestamp: 1541152486013,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "blue",
					"size":  2,
					"ts":    1541152487632,
				},
				Timestamp: 1541152487632,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "red",
					"size":  1,
					"ts":    1541152489252,
				},
				Timestamp: 1541152489252,
			},
			{ //dropped item
				Emitter: name,
				Message: map[string]interface{}{
					"color": "blue",
					"size":  6,
					"ts":    1541152486822,
				},
				Timestamp: 1541152486822,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "yellow",
					"size":  4,
					"ts":    1541152488442,
				},
				Timestamp: 1541152488442,
			},
			{ //To lift the watermark and issue all windows
				Emitter: name,
				Message: map[string]interface{}{
					"color": "yellow",
					"size":  4,
					"ts":    1541152492342,
				},
				Timestamp: 1541152488442,
			},
		}
	case "demo1E":
		data = []*xsql.Tuple{
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 27.5,
					"hum":  59,
					"ts":   1541152486823,
				},
				Timestamp: 1541152486823,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 25.5,
					"hum":  65,
					"ts":   1541152486013,
				},
				Timestamp: 1541152486013,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 27.4,
					"hum":  80,
					"ts":   1541152488442,
				},
				Timestamp: 1541152488442,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 28.1,
					"hum":  75,
					"ts":   1541152487632,
				},
				Timestamp: 1541152487632,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 25.5,
					"hum":  62,
					"ts":   1541152489252,
				},
				Timestamp: 1541152489252,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 25.5,
					"hum":  62,
					"ts":   1541152499252,
				},
				Timestamp: 1541152499252,
			},
		}
	case "sessionDemoE":
		data = []*xsql.Tuple{
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 25.5,
					"hum":  65,
					"ts":   1541152486013,
				},
				Timestamp: 1541152486013,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 28.1,
					"hum":  75,
					"ts":   1541152487932,
				},
				Timestamp: 1541152487932,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 27.5,
					"hum":  59,
					"ts":   1541152486823,
				},
				Timestamp: 1541152486823,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 25.5,
					"hum":  62,
					"ts":   1541152489252,
				},
				Timestamp: 1541152489252,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 27.4,
					"hum":  80,
					"ts":   1541152488442,
				},
				Timestamp: 1541152488442,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 26.2,
					"hum":  63,
					"ts":   1541152490062,
				},
				Timestamp: 1541152490062,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 28.9,
					"hum":  85,
					"ts":   1541152491682,
				},
				Timestamp: 1541152491682,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 26.8,
					"hum":  71,
					"ts":   1541152490872,
				},
				Timestamp: 1541152490872,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 29.1,
					"hum":  92,
					"ts":   1541152492492,
				},
				Timestamp: 1541152492492,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 30.9,
					"hum":  87,
					"ts":   1541152494112,
				},
				Timestamp: 1541152494112,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 32.2,
					"hum":  99,
					"ts":   1541152493202,
				},
				Timestamp: 1541152493202,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 32.2,
					"hum":  99,
					"ts":   1541152499202,
				},
				Timestamp: 1541152499202,
			},
		}
	case "demoErr":
		data = []*xsql.Tuple{
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "red",
					"size":  3,
					"ts":    1541152486013,
				},
				Timestamp: 1541152486013,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": 2,
					"size":  "blue",
					"ts":    1541152487632,
				},
				Timestamp: 1541152487632,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "red",
					"size":  1,
					"ts":    1541152489252,
				},
				Timestamp: 1541152489252,
			},
			{ //dropped item
				Emitter: name,
				Message: map[string]interface{}{
					"color": "blue",
					"size":  6,
					"ts":    1541152486822,
				},
				Timestamp: 1541152486822,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "yellow",
					"size":  4,
					"ts":    1541152488442,
				},
				Timestamp: 1541152488442,
			},
			{ //To lift the watermark and issue all windows
				Emitter: name,
				Message: map[string]interface{}{
					"color": "yellow",
					"size":  4,
					"ts":    1541152492342,
				},
				Timestamp: 1541152488442,
			},
		}
	}
	return nodes.NewSourceNodeWithSource(name, test.NewMockSource(data[:size], done, true), map[string]string{
		"DATASOURCE": name,
	})
}

func TestEventWindow(t *testing.T) {
	var tests = []struct {
		name string
		sql  string
		size int
		r    [][]map[string]interface{}
		m    map[string]interface{}
	}{
		{
			name: `rule1`,
			sql:  `SELECT * FROM demoE GROUP BY HOPPINGWINDOW(ss, 2, 1)`,
			size: 6,
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
				"op_preprocessor_demoE_0_exceptions_total":   int64(0),
				"op_preprocessor_demoE_0_process_latency_ms": int64(0),
				"op_preprocessor_demoE_0_records_in_total":   int64(6),
				"op_preprocessor_demoE_0_records_out_total":  int64(6),

				"op_project_0_exceptions_total":   int64(0),
				"op_project_0_process_latency_ms": int64(0),
				"op_project_0_records_in_total":   int64(5),
				"op_project_0_records_out_total":  int64(5),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(5),
				"sink_mockSink_0_records_out_total": int64(5),

				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(6),
				"source_demoE_0_records_out_total": int64(6),

				"op_window_0_exceptions_total":   int64(0),
				"op_window_0_process_latency_ms": int64(0),
				"op_window_0_records_in_total":   int64(6),
				"op_window_0_records_out_total":  int64(5),
			},
		}, {
			name: `rule2`,
			sql:  `SELECT color, ts FROM demoE where size > 2 GROUP BY tumblingwindow(ss, 1)`,
			size: 6,
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
				"op_preprocessor_demoE_0_exceptions_total":   int64(0),
				"op_preprocessor_demoE_0_process_latency_ms": int64(0),
				"op_preprocessor_demoE_0_records_in_total":   int64(6),
				"op_preprocessor_demoE_0_records_out_total":  int64(6),

				"op_project_0_exceptions_total":   int64(0),
				"op_project_0_process_latency_ms": int64(0),
				"op_project_0_records_in_total":   int64(2),
				"op_project_0_records_out_total":  int64(2),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(2),
				"sink_mockSink_0_records_out_total": int64(2),

				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(6),
				"source_demoE_0_records_out_total": int64(6),

				"op_window_0_exceptions_total":   int64(0),
				"op_window_0_process_latency_ms": int64(0),
				"op_window_0_records_in_total":   int64(6),
				"op_window_0_records_out_total":  int64(4),

				"op_filter_0_exceptions_total":   int64(0),
				"op_filter_0_process_latency_ms": int64(0),
				"op_filter_0_records_in_total":   int64(4),
				"op_filter_0_records_out_total":  int64(2),
			},
		}, {
			name: `rule3`,
			sql:  `SELECT color, temp, ts FROM demoE INNER JOIN demo1E ON demoE.ts = demo1E.ts GROUP BY SlidingWindow(ss, 1)`,
			size: 6,
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
				"op_preprocessor_demoE_0_exceptions_total":   int64(0),
				"op_preprocessor_demoE_0_process_latency_ms": int64(0),
				"op_preprocessor_demoE_0_records_in_total":   int64(6),
				"op_preprocessor_demoE_0_records_out_total":  int64(6),

				"op_preprocessor_demo1E_0_exceptions_total":   int64(0),
				"op_preprocessor_demo1E_0_process_latency_ms": int64(0),
				"op_preprocessor_demo1E_0_records_in_total":   int64(6),
				"op_preprocessor_demo1E_0_records_out_total":  int64(6),

				"op_project_0_exceptions_total":   int64(0),
				"op_project_0_process_latency_ms": int64(0),
				"op_project_0_records_in_total":   int64(5),
				"op_project_0_records_out_total":  int64(5),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(5),
				"sink_mockSink_0_records_out_total": int64(5),

				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(6),
				"source_demoE_0_records_out_total": int64(6),

				"source_demo1E_0_exceptions_total":  int64(0),
				"source_demo1E_0_records_in_total":  int64(6),
				"source_demo1E_0_records_out_total": int64(6),

				"op_window_0_exceptions_total":   int64(0),
				"op_window_0_process_latency_ms": int64(0),
				"op_window_0_records_in_total":   int64(12),
				"op_window_0_records_out_total":  int64(5),

				"op_join_0_exceptions_total":   int64(0),
				"op_join_0_process_latency_ms": int64(0),
				"op_join_0_records_in_total":   int64(5),
				"op_join_0_records_out_total":  int64(5),
			},
		}, {
			name: `rule4`,
			sql:  `SELECT color FROM demoE GROUP BY SlidingWindow(ss, 2), color ORDER BY color`,
			size: 6,
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
				"op_preprocessor_demoE_0_exceptions_total":   int64(0),
				"op_preprocessor_demoE_0_process_latency_ms": int64(0),
				"op_preprocessor_demoE_0_records_in_total":   int64(6),
				"op_preprocessor_demoE_0_records_out_total":  int64(6),

				"op_project_0_exceptions_total":   int64(0),
				"op_project_0_process_latency_ms": int64(0),
				"op_project_0_records_in_total":   int64(4),
				"op_project_0_records_out_total":  int64(4),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(4),
				"sink_mockSink_0_records_out_total": int64(4),

				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(6),
				"source_demoE_0_records_out_total": int64(6),

				"op_window_0_exceptions_total":   int64(0),
				"op_window_0_process_latency_ms": int64(0),
				"op_window_0_records_in_total":   int64(6),
				"op_window_0_records_out_total":  int64(4),

				"op_aggregate_0_exceptions_total":   int64(0),
				"op_aggregate_0_process_latency_ms": int64(0),
				"op_aggregate_0_records_in_total":   int64(4),
				"op_aggregate_0_records_out_total":  int64(4),

				"op_order_0_exceptions_total":   int64(0),
				"op_order_0_process_latency_ms": int64(0),
				"op_order_0_records_in_total":   int64(4),
				"op_order_0_records_out_total":  int64(4),
			},
		}, {
			name: `rule5`,
			sql:  `SELECT temp FROM sessionDemoE GROUP BY SessionWindow(ss, 2, 1) `,
			size: 12,
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
				"op_preprocessor_sessionDemoE_0_exceptions_total":   int64(0),
				"op_preprocessor_sessionDemoE_0_process_latency_ms": int64(0),
				"op_preprocessor_sessionDemoE_0_records_in_total":   int64(12),
				"op_preprocessor_sessionDemoE_0_records_out_total":  int64(12),

				"op_project_0_exceptions_total":   int64(0),
				"op_project_0_process_latency_ms": int64(0),
				"op_project_0_records_in_total":   int64(4),
				"op_project_0_records_out_total":  int64(4),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(4),
				"sink_mockSink_0_records_out_total": int64(4),

				"source_sessionDemoE_0_exceptions_total":  int64(0),
				"source_sessionDemoE_0_records_in_total":  int64(12),
				"source_sessionDemoE_0_records_out_total": int64(12),

				"op_window_0_exceptions_total":   int64(0),
				"op_window_0_process_latency_ms": int64(0),
				"op_window_0_records_in_total":   int64(12),
				"op_window_0_records_out_total":  int64(4),
			},
		}, {
			name: `rule6`,
			sql:  `SELECT max(temp) as m, count(color) as c FROM demoE INNER JOIN demo1E ON demoE.ts = demo1E.ts GROUP BY SlidingWindow(ss, 1)`,
			size: 6,
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
				"op_preprocessor_demoE_0_exceptions_total":   int64(0),
				"op_preprocessor_demoE_0_process_latency_ms": int64(0),
				"op_preprocessor_demoE_0_records_in_total":   int64(6),
				"op_preprocessor_demoE_0_records_out_total":  int64(6),

				"op_preprocessor_demo1E_0_exceptions_total":   int64(0),
				"op_preprocessor_demo1E_0_process_latency_ms": int64(0),
				"op_preprocessor_demo1E_0_records_in_total":   int64(6),
				"op_preprocessor_demo1E_0_records_out_total":  int64(6),

				"op_project_0_exceptions_total":   int64(0),
				"op_project_0_process_latency_ms": int64(0),
				"op_project_0_records_in_total":   int64(5),
				"op_project_0_records_out_total":  int64(5),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(5),
				"sink_mockSink_0_records_out_total": int64(5),

				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(6),
				"source_demoE_0_records_out_total": int64(6),

				"source_demo1E_0_exceptions_total":  int64(0),
				"source_demo1E_0_records_in_total":  int64(6),
				"source_demo1E_0_records_out_total": int64(6),

				"op_window_0_exceptions_total":  int64(0),
				"op_window_0_records_in_total":  int64(12),
				"op_window_0_records_out_total": int64(5),

				"op_join_0_exceptions_total":   int64(0),
				"op_join_0_process_latency_ms": int64(0),
				"op_join_0_records_in_total":   int64(5),
				"op_join_0_records_out_total":  int64(5),
			},
		}, {
			name: `rule7`,
			sql:  `SELECT * FROM demoErr GROUP BY HOPPINGWINDOW(ss, 2, 1)`,
			size: 6,
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
				"op_preprocessor_demoErr_0_exceptions_total":   int64(1),
				"op_preprocessor_demoErr_0_process_latency_ms": int64(0),
				"op_preprocessor_demoErr_0_records_in_total":   int64(6),
				"op_preprocessor_demoErr_0_records_out_total":  int64(5),

				"op_project_0_exceptions_total":   int64(1),
				"op_project_0_process_latency_ms": int64(0),
				"op_project_0_records_in_total":   int64(6),
				"op_project_0_records_out_total":  int64(5),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(6),
				"sink_mockSink_0_records_out_total": int64(6),

				"source_demoErr_0_exceptions_total":  int64(0),
				"source_demoErr_0_records_in_total":  int64(6),
				"source_demoErr_0_records_out_total": int64(6),

				"op_window_0_exceptions_total":   int64(1),
				"op_window_0_process_latency_ms": int64(0),
				"op_window_0_records_in_total":   int64(6),
				"op_window_0_records_out_total":  int64(5),
			},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	createEventStreams(t)
	defer dropEventStreams(t)
	for i, tt := range tests {
		test.ResetClock(1541152486000)
		p := NewRuleProcessor(DbDir)
		parser := xsql.NewParser(strings.NewReader(tt.sql))
		var (
			sources []*nodes.SourceNode
			syncs   []chan int
		)
		if stmt, err := xsql.Language.Parse(parser); err != nil {
			t.Errorf("parse sql %s error: %s", tt.sql, err)
		} else {
			if selectStmt, ok := stmt.(*xsql.SelectStatement); !ok {
				t.Errorf("sql %s is not a select statement", tt.sql)
			} else {
				streams := xsql.GetStreams(selectStmt)
				for _, stream := range streams {
					next := make(chan int)
					syncs = append(syncs, next)
					source := getEventMockSource(stream, next, tt.size)
					sources = append(sources, source)
				}
			}
		}
		tp, inputs, err := p.createTopoWithSources(&api.Rule{
			Id: tt.name, Sql: tt.sql,
			Options: map[string]interface{}{
				"isEventTime":   true,
				"lateTolerance": float64(1000),
			},
		}, sources)
		if err != nil {
			t.Error(err)
		}
		mockSink := test.NewMockSink()
		sink := nodes.NewSinkNodeWithSink("mockSink", mockSink)
		tp.AddSink(inputs, sink)
		errCh := tp.Open()
		func() {
			for i := 0; i < tt.size*len(syncs); i++ {
				syncs[i%len(syncs)] <- i
				for {
					time.Sleep(1)
					if getMetric(tp, "op_window_0_records_in_total") == (i + 1) {
						break
					}
				}
				select {
				case err = <-errCh:
					t.Log(err)
					tp.Cancel()
					return
				default:
				}
			}
			mockClock := test.GetMockClock()
			mockClock.Add(1000 * time.Millisecond)
			retry := 100
			for ; retry > 0; retry-- {
				if err := compareMetrics(tp, tt.m, tt.sql); err == nil {
					break
				}
				t.Logf("wait to try another %d times", retry)
				time.Sleep(time.Duration(retry) * time.Millisecond)
			}
			if retry == 0 {
				err := compareMetrics(tp, tt.m, tt.sql)
				t.Errorf("could not get correct metrics: %v", err)
			}
		}()
		results := mockSink.GetResults()
		var maps [][]map[string]interface{}
		for _, v := range results {
			var mapRes []map[string]interface{}
			err := json.Unmarshal(v, &mapRes)
			if err != nil {
				t.Errorf("Failed to parse the input into map")
				continue
			}
			maps = append(maps, mapRes)
		}
		if !reflect.DeepEqual(tt.r, maps) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.r, maps)
		}
		if err := compareMetrics(tp, tt.m, tt.sql); err != nil {
			t.Errorf("%d. %q\n\n%v", i, tt.sql, err)
		}
		tp.Cancel()
	}
}

func getMetric(tp *xstream.TopologyNew, name string) int {
	keys, values := tp.GetMetrics()
	for index, key := range keys {
		if key == name {
			return int(values[index].(int64))
		}
	}
	fmt.Println("can't find " + name)
	return 0
}

func compareMetrics(tp *xstream.TopologyNew, m map[string]interface{}, sql string) (err error) {
	keys, values := tp.GetMetrics()
	for i, k := range keys {
		log.Printf("%s:%v", k, values[i])
	}
	for k, v := range m {
		var (
			index   int
			key     string
			matched bool
		)
		for index, key = range keys {
			if k == key {
				if strings.HasSuffix(k, "process_latency_ms") {
					if values[index].(int64) >= v.(int64) {
						matched = true
						continue
					} else {
						break
					}
				}
				if values[index] == v {
					matched = true
				}
				break
			}
		}
		if matched {
			continue
		}
		//do not find
		if index < len(values) {
			return fmt.Errorf("metrics mismatch for %s:\n\nexp=%#v(%t)\n\ngot=%#v(%t)\n\n", k, v, v, values[index], values[index])
		} else {
			return fmt.Errorf("metrics mismatch for %s:\n\nexp=%#v\n\ngot=nil\n\n", k, v)
		}
	}
	return nil
}

func errstring(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}
