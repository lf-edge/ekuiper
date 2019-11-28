package processors

import (
	"encoding/json"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/nodes"
	"github.com/emqx/kuiper/xstream/test"
	"fmt"
	"path"
	"reflect"
	"strings"
	"testing"
	"time"
)

var DbDir = getDbDir()

func getDbDir() string{
	dbDir, err := common.GetAndCreateDataLoc("test")
	if err != nil {
		log.Panic(err)
	}
	log.Infof("db location is %s", dbDir)
	return dbDir
}

func TestStreamCreateProcessor(t *testing.T) {
	var tests = []struct {
		s    string
		r    []string
		err  string
	}{
		{
			s: `SHOW STREAMS;`,
			r: []string{"No stream definitions are found."},
		},
		{
			s: `EXPLAIN STREAM topic1;`,
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
			s: `DESCRIBE STREAM topic1;`,
			err: "Stream topic1 is not found.",
		},
		{
			s: `DROP STREAM topic1;`,
			err: "Drop stream fails: topic1 is not found.",
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))

	streamDB := path.Join(getDbDir(), "streamTest")
	for i, tt := range tests {
		results, err := NewStreamProcessor(tt.s, streamDB).Exec()
		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" {
			if !reflect.DeepEqual(tt.r, results) {
				t.Errorf("%d. %q\n\nstmt mismatch:\n\ngot=%#v\n\n", i, tt.s, results)
			}
		}
	}
}

func createStreams(t *testing.T){
	demo := `CREATE STREAM demo (
					color STRING,
					size BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="demo", FORMAT="json", KEY="ts");`
	_, err := NewStreamProcessor(demo, path.Join(DbDir, "stream")).Exec()
	if err != nil{
		t.Log(err)
	}
	demo1 := `CREATE STREAM demo1 (
					temp FLOAT,
					hum BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="demo1", FORMAT="json", KEY="ts");`
	_, err = NewStreamProcessor(demo1, path.Join(DbDir, "stream")).Exec()
	if err != nil{
		t.Log(err)
	}
	sessionDemo := `CREATE STREAM sessionDemo (
					temp FLOAT,
					hum BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="sessionDemo", FORMAT="json", KEY="ts");`
	_, err = NewStreamProcessor(sessionDemo, path.Join(DbDir, "stream")).Exec()
	if err != nil{
		t.Log(err)
	}
}

func dropStreams(t *testing.T){
	demo := `DROP STREAM demo`
	_, err := NewStreamProcessor(demo, path.Join(DbDir, "stream")).Exec()
	if err != nil{
		t.Log(err)
	}
	demo1 := `DROP STREAM demo1`
	_, err = NewStreamProcessor(demo1, path.Join(DbDir, "stream")).Exec()
	if err != nil{
		t.Log(err)
	}
	sessionDemo := `DROP STREAM sessionDemo`
	_, err = NewStreamProcessor(sessionDemo, path.Join(DbDir, "stream")).Exec()
	if err != nil{
		t.Log(err)
	}
}

func getMockSource(name string, done chan<- struct{}, size int) *nodes.SourceNode{
	var data []*xsql.Tuple
	switch name{
	case "demo":
		data = []*xsql.Tuple{
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "red",
					"size": 3,
					"ts": 1541152486013,
				},
				Timestamp: 1541152486013,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "blue",
					"size": 6,
					"ts": 1541152486822,
				},
				Timestamp: 1541152486822,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "blue",
					"size": 2,
					"ts": 1541152487632,
				},
				Timestamp: 1541152487632,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "yellow",
					"size": 4,
					"ts": 1541152488442,
				},
				Timestamp: 1541152488442,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "red",
					"size": 1,
					"ts": 1541152489252,
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
					"hum": 65,
					"ts": 1541152486013,
				},
				Timestamp: 1541152486013,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 27.5,
					"hum": 59,
					"ts": 1541152486823,
				},
				Timestamp: 1541152486823,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 28.1,
					"hum": 75,
					"ts": 1541152487632,
				},
				Timestamp: 1541152487632,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 27.4,
					"hum": 80,
					"ts": 1541152488442,
				},
				Timestamp: 1541152488442,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 25.5,
					"hum": 62,
					"ts": 1541152489252,
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
					"hum": 65,
					"ts": 1541152486013,
				},
				Timestamp: 1541152486013,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 27.5,
					"hum": 59,
					"ts": 1541152486823,
				},
				Timestamp: 1541152486823,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 28.1,
					"hum": 75,
					"ts": 1541152487932,
				},
				Timestamp: 1541152487932,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 27.4,
					"hum": 80,
					"ts": 1541152488442,
				},
				Timestamp: 1541152488442,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 25.5,
					"hum": 62,
					"ts": 1541152489252,
				},
				Timestamp: 1541152489252,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 26.2,
					"hum": 63,
					"ts": 1541152490062,
				},
				Timestamp: 1541152490062,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 26.8,
					"hum": 71,
					"ts": 1541152490872,
				},
				Timestamp: 1541152490872,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 28.9,
					"hum": 85,
					"ts": 1541152491682,
				},
				Timestamp: 1541152491682,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 29.1,
					"hum": 92,
					"ts": 1541152492492,
				},
				Timestamp: 1541152492492,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 32.2,
					"hum": 99,
					"ts": 1541152493202,
				},
				Timestamp: 1541152493202,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 30.9,
					"hum": 87,
					"ts": 1541152494112,
				},
				Timestamp: 1541152494112,
			},
		}
	}
	return nodes.NewSourceNode(name, test.NewMockSource(data[:size], done, false),  map[string]string{
		"DATASOURCE": name,
	})
}

func TestSingleSQL(t *testing.T) {
	var tests = []struct {
		name    string
		sql 	string
		r    [][]map[string]interface{}
	}{
		{
			name: `rule1`,
			sql: `SELECT * FROM demo`,
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
		}, {
			name: `rule2`,
			sql: `SELECT color, ts FROM demo where size > 3`,
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
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	createStreams(t)
	defer dropStreams(t)
	done := make(chan struct{})
	defer close(done)
	for i, tt := range tests {
		p := NewRuleProcessor(DbDir)
		parser := xsql.NewParser(strings.NewReader(tt.sql))
		var sources []*nodes.SourceNode
		if stmt, err := xsql.Language.Parse(parser); err != nil{
			t.Errorf("parse sql %s error: %s", tt.sql , err)
		}else {
			if selectStmt, ok := stmt.(*xsql.SelectStatement); !ok {
				t.Errorf("sql %s is not a select statement", tt.sql)
			} else {
				streams := xsql.GetStreams(selectStmt)
				for _, stream := range streams{
					source := getMockSource(stream, done, 5)
					sources = append(sources, source)
				}
			}
		}
		tp, inputs, err := p.createTopoWithSources(&api.Rule{Id: tt.name, Sql: tt.sql}, sources)
		if err != nil{
			t.Error(err)
		}
		mockSink := test.NewMockSink()
		sink := nodes.NewSinkNode("MockSink", mockSink)
		tp.AddSink(inputs, sink)
		count := len(sources)
		errCh := tp.Open()
		func(){
			for{
				select{
				case err = <- errCh:
					t.Log(err)
					tp.Cancel()
					return
				case <- done:
					count--
					log.Infof("%d sources remaining", count)
					if count <= 0{
						log.Info("stream stopping")
						time.Sleep(1 * time.Second)
						tp.Cancel()
						return
					}
				default:
				}
			}
		}()
		results := mockSink.GetResults()
		var maps [][]map[string]interface{}
		for _, v := range results{
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
	}
}

func TestWindow(t *testing.T) {
	common.IsTesting = true
	var tests = []struct {
		name    string
		sql 	string
		size    int
		r    [][]map[string]interface{}
	}{
		{
			name: `rule1`,
			sql: `SELECT * FROM demo GROUP BY HOPPINGWINDOW(ss, 2, 1)`,
			size: 5,
			r: [][]map[string]interface{}{
				{{
					"color": "red",
					"size":  float64(3),
					"ts":    float64(1541152486013),
				},{
					"color": "blue",
					"size":  float64(6),
					"ts":    float64(1541152486822),
				}},
				{{
					"color": "red",
					"size":  float64(3),
					"ts":    float64(1541152486013),
				},{
					"color": "blue",
					"size":  float64(6),
					"ts":    float64(1541152486822),
				},{
					"color": "blue",
					"size":  float64(2),
					"ts":    float64(1541152487632),
				}},
				{{
					"color": "blue",
					"size":  float64(2),
					"ts":    float64(1541152487632),
				},{
					"color": "yellow",
					"size":  float64(4),
					"ts":    float64(1541152488442),
				}},
			},
		}, {
			name: `rule2`,
			sql: `SELECT color, ts FROM demo where size > 2 GROUP BY tumblingwindow(ss, 1)`,
			size: 5,
			r: [][]map[string]interface{}{
				{{
					"color": "red",
					"ts":    float64(1541152486013),
				},{
					"color": "blue",
					"ts":    float64(1541152486822),
				}},
				{{
					"color": "yellow",
					"ts":    float64(1541152488442),
				}},
			},
		}, {
			name: `rule3`,
			sql: `SELECT color, temp, ts FROM demo INNER JOIN demo1 ON demo.ts = demo1.ts GROUP BY SlidingWindow(ss, 1)`,
			size: 5,
			r: [][]map[string]interface{}{
				{{
					"color": "red",
					"temp": 25.5,
					"ts":    float64(1541152486013),
				}},{{
					"color": "red",
					"temp":  25.5,
					"ts":    float64(1541152486013),
				}},{{
					"color": "red",
					"temp":  25.5,
					"ts":    float64(1541152486013),
				}},{{
					"color": "blue",
					"temp": 28.1,
					"ts":    float64(1541152487632),
				}},{{
					"color": "blue",
					"temp":  28.1,
					"ts":    float64(1541152487632),
				}},{{
					"color": "blue",
					"temp":  28.1,
					"ts":    float64(1541152487632),
				},{
					"color": "yellow",
					"temp":  27.4,
					"ts":    float64(1541152488442),
				}},{{
					"color": "yellow",
					"temp":  27.4,
					"ts":    float64(1541152488442),
				}},{{
					"color": "yellow",
					"temp":  27.4,
					"ts":    float64(1541152488442),
				},{
					"color": "red",
					"temp":  25.5,
					"ts":    float64(1541152489252),
				}},
			},
		}, {
			name: `rule4`,
			sql: `SELECT color FROM demo GROUP BY SlidingWindow(ss, 2), color ORDER BY color`,
			size: 5,
			r: [][]map[string]interface{}{
				{{
					"color": "red",
				}},{{
					"color": "blue",
				},{
					"color": "red",
				}},{{
					"color": "blue",
				},{
					"color": "red",
				}},{{
					"color": "blue",
				},{
					"color": "yellow",
				}},{{
					"color": "blue",
				}, {
					"color": "red",
				},{
					"color": "yellow",
				}},
			},
		},{
			name: `rule5`,
			sql: `SELECT temp FROM sessionDemo GROUP BY SessionWindow(ss, 2, 1) `,
			size: 11,
			r: [][]map[string]interface{}{
				{{
					"temp": 25.5,
				},{
					"temp": 27.5,
				}},{{
					"temp": 28.1,
				},{
					"temp": 27.4,
				},{
					"temp": 25.5,
				}},{{
					"temp": 26.2,
				},{
					"temp": 26.8,
				},{
					"temp": 28.9,
				},{
					"temp": 29.1,
				},{
					"temp": 32.2,
				}},
			},
		},{
			name: `rule6`,
			sql: `SELECT max(temp) as m, count(color) as c FROM demo INNER JOIN demo1 ON demo.ts = demo1.ts GROUP BY SlidingWindow(ss, 1)`,
			size: 5,
			r: [][]map[string]interface{}{
				{{
					"m": 25.5,
					"c": float64(1),
				}},{{
					"m": 25.5,
					"c": float64(1),
				}},{{
					"m": 25.5,
					"c": float64(1),
				}},{{
					"m": 28.1,
					"c": float64(1),
				}},{{
					"m": 28.1,
					"c": float64(1),
				}},{{
					"m": 28.1,
					"c": float64(2),
				}},{{
					"m": 27.4,
					"c": float64(1),
				}},{{
					"m": 27.4,
					"c": float64(2),
				}},
			},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	createStreams(t)
	defer dropStreams(t)
	done := make(chan struct{})
	defer close(done)
	common.ResetMockTicker()
	for i, tt := range tests {
		p := NewRuleProcessor(DbDir)
		parser := xsql.NewParser(strings.NewReader(tt.sql))
		var sources []*nodes.SourceNode
		if stmt, err := xsql.Language.Parse(parser); err != nil{
			t.Errorf("parse sql %s error: %s", tt.sql , err)
		}else {
			if selectStmt, ok := stmt.(*xsql.SelectStatement); !ok {
				t.Errorf("sql %s is not a select statement", tt.sql)
			} else {
				streams := xsql.GetStreams(selectStmt)
				for _, stream := range streams{
					source := getMockSource(stream, done, tt.size)
					sources = append(sources, source)
				}
			}
		}
		tp, inputs, err := p.createTopoWithSources(&api.Rule{Id: tt.name, Sql: tt.sql}, sources)
		if err != nil{
			t.Error(err)
		}
		mockSink := test.NewMockSink()
		sink := nodes.NewSinkNode("mockSink", mockSink)
		tp.AddSink(inputs, sink)
		count := len(sources)
		errCh := tp.Open()
		func(){
			for{
				select{
				case err = <- errCh:
					t.Log(err)
					tp.Cancel()
					return
				case <- done:
					count--
					log.Infof("%d sources remaining", count)
					if count <= 0{
						log.Info("stream stopping")
						time.Sleep(1 * time.Second)
						tp.Cancel()
						return
					}
				default:
				}
			}
		}()
		results := mockSink.GetResults()
		var maps [][]map[string]interface{}
		for _, v := range results{
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
	}
}

func createEventStreams(t *testing.T){
	demo := `CREATE STREAM demoE (
					color STRING,
					size BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="demoE", FORMAT="json", KEY="ts", TIMESTAMP="ts");`
	_, err := NewStreamProcessor(demo, path.Join(DbDir, "stream")).Exec()
	if err != nil{
		t.Log(err)
	}
	demo1 := `CREATE STREAM demo1E (
					temp FLOAT,
					hum BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="demo1E", FORMAT="json", KEY="ts", TIMESTAMP="ts");`
	_, err = NewStreamProcessor(demo1, path.Join(DbDir, "stream")).Exec()
	if err != nil{
		t.Log(err)
	}
	sessionDemo := `CREATE STREAM sessionDemoE (
					temp FLOAT,
					hum BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="sessionDemoE", FORMAT="json", KEY="ts", TIMESTAMP="ts");`
	_, err = NewStreamProcessor(sessionDemo, path.Join(DbDir, "stream")).Exec()
	if err != nil{
		t.Log(err)
	}
}

func dropEventStreams(t *testing.T){
	demo := `DROP STREAM demoE`
	_, err := NewStreamProcessor(demo, path.Join(DbDir, "stream")).Exec()
	if err != nil{
		t.Log(err)
	}
	demo1 := `DROP STREAM demo1E`
	_, err = NewStreamProcessor(demo1, path.Join(DbDir, "stream")).Exec()
	if err != nil{
		t.Log(err)
	}
	sessionDemo := `DROP STREAM sessionDemoE`
	_, err = NewStreamProcessor(sessionDemo, path.Join(DbDir, "stream")).Exec()
	if err != nil{
		t.Log(err)
	}
}

func getEventMockSource(name string, done chan<- struct{}, size int) *nodes.SourceNode{
	var data []*xsql.Tuple
	switch name{
	case "demoE":
		data = []*xsql.Tuple{
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "red",
					"size": 3,
					"ts": 1541152486013,
				},
				Timestamp: 1541152486013,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "blue",
					"size": 2,
					"ts": 1541152487632,
				},
				Timestamp: 1541152487632,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "red",
					"size": 1,
					"ts": 1541152489252,
				},
				Timestamp: 1541152489252,
			},
			{ //dropped item
				Emitter: name,
				Message: map[string]interface{}{
					"color": "blue",
					"size": 6,
					"ts": 1541152486822,
				},
				Timestamp: 1541152486822,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "yellow",
					"size": 4,
					"ts": 1541152488442,
				},
				Timestamp: 1541152488442,
			},
			{ //To lift the watermark and issue all windows
				Emitter: name,
				Message: map[string]interface{}{
					"color": "yellow",
					"size": 4,
					"ts": 1541152492342,
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
					"hum": 59,
					"ts": 1541152486823,
				},
				Timestamp: 1541152486823,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 25.5,
					"hum": 65,
					"ts": 1541152486013,
				},
				Timestamp: 1541152486013,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 27.4,
					"hum": 80,
					"ts": 1541152488442,
				},
				Timestamp: 1541152488442,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 28.1,
					"hum": 75,
					"ts": 1541152487632,
				},
				Timestamp: 1541152487632,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 25.5,
					"hum": 62,
					"ts": 1541152489252,
				},
				Timestamp: 1541152489252,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 25.5,
					"hum": 62,
					"ts": 1541152499252,
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
					"hum": 65,
					"ts": 1541152486013,
				},
				Timestamp: 1541152486013,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 28.1,
					"hum": 75,
					"ts": 1541152487932,
				},
				Timestamp: 1541152487932,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 27.5,
					"hum": 59,
					"ts": 1541152486823,
				},
				Timestamp: 1541152486823,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 25.5,
					"hum": 62,
					"ts": 1541152489252,
				},
				Timestamp: 1541152489252,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 27.4,
					"hum": 80,
					"ts": 1541152488442,
				},
				Timestamp: 1541152488442,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 26.2,
					"hum": 63,
					"ts": 1541152490062,
				},
				Timestamp: 1541152490062,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 28.9,
					"hum": 85,
					"ts": 1541152491682,
				},
				Timestamp: 1541152491682,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 26.8,
					"hum": 71,
					"ts": 1541152490872,
				},
				Timestamp: 1541152490872,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 29.1,
					"hum": 92,
					"ts": 1541152492492,
				},
				Timestamp: 1541152492492,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 30.9,
					"hum": 87,
					"ts": 1541152494112,
				},
				Timestamp: 1541152494112,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 32.2,
					"hum": 99,
					"ts": 1541152493202,
				},
				Timestamp: 1541152493202,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"temp": 32.2,
					"hum": 99,
					"ts": 1541152499202,
				},
				Timestamp: 1541152499202,
			},
		}
	}
	return nodes.NewSourceNode(name, test.NewMockSource(data[:size], done, true), map[string]string{
		"DATASOURCE": name,
	})
}

func TestEventWindow(t *testing.T) {
	common.IsTesting = true
	var tests = []struct {
		name    string
		sql 	string
		size    int
		r    [][]map[string]interface{}
	}{
		{
			name: `rule1`,
			sql: `SELECT * FROM demoE GROUP BY HOPPINGWINDOW(ss, 2, 1)`,
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
				},{
					"color": "blue",
					"size":  float64(2),
					"ts":    float64(1541152487632),
				}},
				{{
					"color": "blue",
					"size":  float64(2),
					"ts":    float64(1541152487632),
				},{
					"color": "yellow",
					"size":  float64(4),
					"ts":    float64(1541152488442),
				}},{{
					"color": "yellow",
					"size":  float64(4),
					"ts":    float64(1541152488442),
				},{
					"color": "red",
					"size": float64(1),
					"ts": float64(1541152489252),
				}},{{
					"color": "red",
					"size": float64(1),
					"ts": float64(1541152489252),
				}},
			},
		}, {
			name: `rule2`,
			sql: `SELECT color, ts FROM demoE where size > 2 GROUP BY tumblingwindow(ss, 1)`,
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
		}, {
			name: `rule3`,
			sql: `SELECT color, temp, ts FROM demoE INNER JOIN demo1E ON demoE.ts = demo1E.ts GROUP BY SlidingWindow(ss, 1)`,
			size: 6,
			r: [][]map[string]interface{}{
				{{
					"color": "red",
					"temp": 25.5,
					"ts":    float64(1541152486013),
				}},{{
					"color": "red",
					"temp":  25.5,
					"ts":    float64(1541152486013),
				}},{{
					"color": "blue",
					"temp": 28.1,
					"ts":    float64(1541152487632),
				}},{{
					"color": "blue",
					"temp":  28.1,
					"ts":    float64(1541152487632),
				},{
					"color": "yellow",
					"temp":  27.4,
					"ts":    float64(1541152488442),
				}},{{
					"color": "yellow",
					"temp":  27.4,
					"ts":    float64(1541152488442),
				},{
					"color": "red",
					"temp":  25.5,
					"ts":    float64(1541152489252),
				}},
			},
		}, {
			name: `rule4`,
			sql: `SELECT color FROM demoE GROUP BY SlidingWindow(ss, 2), color ORDER BY color`,
			size: 6,
			r: [][]map[string]interface{}{
				{{
					"color": "red",
				}},{{
					"color": "blue",
				},{
					"color": "red",
				}},{{
					"color": "blue",
				},{
					"color": "yellow",
				}},{{
					"color": "blue",
				}, {
					"color": "red",
				},{
					"color": "yellow",
				}},
			},
		},{
			name: `rule5`,
			sql: `SELECT temp FROM sessionDemoE GROUP BY SessionWindow(ss, 2, 1) `,
			size: 12,
			r: [][]map[string]interface{}{
				{{
					"temp": 25.5,
				}},{{
					"temp": 28.1,
				},{
					"temp": 27.4,
				},{
					"temp": 25.5,
				}},{{
					"temp": 26.2,
				},{
					"temp": 26.8,
				},{
					"temp": 28.9,
				},{
					"temp": 29.1,
				},{
					"temp": 32.2,
				}},{{
					"temp": 30.9,
				}},
			},
		},{
			name: `rule6`,
			sql: `SELECT max(temp) as m, count(color) as c FROM demoE INNER JOIN demo1E ON demoE.ts = demo1E.ts GROUP BY SlidingWindow(ss, 1)`,
			size: 6,
			r: [][]map[string]interface{}{
				{{
					"m": 25.5,
					"c": float64(1),
				}},{{
					"m": 25.5,
					"c": float64(1),
				}},{{
					"m": 28.1,
					"c": float64(1),
				}},{{
					"m": 28.1,
					"c": float64(2),
				}},{{
					"m": 27.4,
					"c": float64(2),
				}},
			},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	createEventStreams(t)
	defer dropEventStreams(t)
	done := make(chan struct{})
	defer close(done)
	common.ResetMockTicker()
	//mock ticker
	realTicker := time.NewTicker(500 * time.Millisecond)
	tickerDone := make(chan bool)
	go func(){
		ticker := common.GetTicker(1000).(*common.MockTicker)
		timer := common.GetTimer(1000).(*common.MockTimer)
		for {
			select {
			case <-tickerDone:
				log.Infof("real ticker exiting...")
				return
			case t := <-realTicker.C:
				ts := common.TimeToUnixMilli(t)
				if ticker != nil {
					go ticker.DoTick(ts)
				}
				if timer != nil {
					go timer.DoTick(ts)
				}
			}
		}

	}()
	for i, tt := range tests {
		p := NewRuleProcessor(DbDir)
		parser := xsql.NewParser(strings.NewReader(tt.sql))
		var sources []*nodes.SourceNode
		if stmt, err := xsql.Language.Parse(parser); err != nil{
			t.Errorf("parse sql %s error: %s", tt.sql , err)
		}else {
			if selectStmt, ok := stmt.(*xsql.SelectStatement); !ok {
				t.Errorf("sql %s is not a select statement", tt.sql)
			} else {
				streams := xsql.GetStreams(selectStmt)
				for _, stream := range streams{
					source := getEventMockSource(stream, done, tt.size)
					sources = append(sources, source)
				}
			}
		}
		tp, inputs, err := p.createTopoWithSources(&api.Rule{
			Id:tt.name, Sql: tt.sql,
			Options: map[string]interface{}{
				"isEventTime": true,
				"lateTolerance": float64(1000),
			},
		}, sources)
		if err != nil{
			t.Error(err)
		}
		mockSink := test.NewMockSink()
		sink := nodes.NewSinkNode("MockSink", mockSink)
		tp.AddSink(inputs, sink)
		count := len(sources)
		errCh := tp.Open()
		func(){
			for{
				select{
				case err = <- errCh:
					t.Log(err)
					tp.Cancel()
					return
				case <- done:
					count--
					log.Infof("%d sources remaining", count)
					if count <= 0{
						log.Info("stream stopping")
						time.Sleep(1 * time.Second)
						tp.Cancel()
						return
					}
				default:
				}
			}
		}()
		results := mockSink.GetResults()
		var maps [][]map[string]interface{}
		for _, v := range results{
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
	}
	realTicker.Stop()
	tickerDone <- true
	close(tickerDone)
}

func errstring(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

