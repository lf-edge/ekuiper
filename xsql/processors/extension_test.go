package processors

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/nodes"
	"github.com/emqx/kuiper/xstream/test"
	"os"
	"path"
	"reflect"
	"strings"
	"testing"
	"time"
)

//This cannot be run in Windows. And the plugins must be built to so before running this
//For Windows, run it in wsl with go test xsql/processors/extension_test.go xsql/processors/xsql_processor.go
func setup() *RuleProcessor {
	log := common.Log

	os.Remove(CACHE_FILE)

	dbDir, err := common.GetAndCreateDataLoc("test")
	if err != nil {
		log.Panic(err)
	}
	log.Infof("db location is %s", dbDir)

	p := NewStreamProcessor(path.Join(dbDir, "stream"))
	demo := `DROP STREAM ext`
	p.ExecStmt(demo)

	demo = "CREATE STREAM ext (count bigint) WITH (DATASOURCE=\"users\", FORMAT=\"JSON\", TYPE=\"random\", CONF_KEY=\"ext\")"
	_, err = p.ExecStmt(demo)
	if err != nil {
		panic(err)
	}

	demo = `DROP STREAM ext2`
	p.ExecStmt(demo)

	demo = "CREATE STREAM ext2 (count bigint) WITH (DATASOURCE=\"users\", FORMAT=\"JSON\", TYPE=\"random\", CONF_KEY=\"dedup\")"
	_, err = p.ExecStmt(demo)
	if err != nil {
		panic(err)
	}

	rp := NewRuleProcessor(dbDir)
	return rp
}

var CACHE_FILE = "cache"

//Test for source, sink, func and agg func extensions
//The .so files must be in the plugins folder
func TestExtensions(t *testing.T) {
	log := common.Log
	var tests = []struct {
		name      string
		rj        string
		minLength int
		maxLength int
	}{
		{
			name:      `$$test1`,
			rj:        "{\"sql\": \"SELECT count(echo(count)) as c, echo(count) as e, countPlusOne(count) as p FROM ext where count > 49\",\"actions\": [{\"file\":  {\"path\":\"" + CACHE_FILE + "\"}}]}",
			minLength: 5,
		}, {
			name:      `$$test2`,
			rj:        "{\"sql\": \"SELECT count(echo(count)) as c, echo(count) as e, countPlusOne(count) as p FROM ext2\",\"actions\": [{\"file\":  {\"path\":\"" + CACHE_FILE + "\"}}]}",
			maxLength: 2,
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	rp := setup()
	done := make(chan struct{})
	defer close(done)
	for i, tt := range tests {
		rp.ExecDrop(tt.name)
		rs, err := rp.ExecCreate(tt.name, tt.rj)
		if err != nil {
			t.Errorf("failed to create rule: %s.", err)
			continue
		}
		os.Create(CACHE_FILE)
		tp, err := rp.ExecInitRule(rs)
		if err != nil {
			t.Errorf("fail to init rule: %v", err)
			continue
		}

		go func() {
			select {
			case err := <-tp.Open():
				log.Println(err)
				tp.Cancel()
			case <-time.After(900 * time.Millisecond):
				tp.Cancel()
			}
		}()
		time.Sleep(1000 * time.Millisecond)
		log.Printf("exit main program after a second")
		results := getResults()
		log.Infof("get results %v", results)
		os.Remove(CACHE_FILE)
		var maps [][]map[string]interface{}
		for _, v := range results {
			var mapRes []map[string]interface{}
			err := json.Unmarshal([]byte(v), &mapRes)
			if err != nil {
				t.Errorf("Failed to parse the input into map")
				continue
			}
			maps = append(maps, mapRes)
		}

		if tt.minLength > 0 {
			if len(maps) < tt.minLength {
				t.Errorf("%d. %q\n\nresult length is smaller than minlength:\n\ngot=%#v\n\n", i, tt.rj, maps)
				break
			}
		}

		if tt.maxLength > 0 {
			if len(maps) > tt.maxLength {
				t.Errorf("%d. %q\n\nresult length is bigger than maxLength:\n\ngot=%#v\n\n", i, tt.rj, maps)
				break
			}
		}

		for _, r := range maps {
			if len(r) != 1 {
				t.Errorf("%d. %q\n\nresult mismatch:\n\ngot=%#v\n\n", i, tt.rj, maps)
				break
			}
			r := r[0]
			c := int((r["c"]).(float64))
			if c != 1 {
				t.Errorf("%d. %q\n\nresult mismatch:\n\ngot=%#v\n\n", i, tt.rj, maps)
				break
			}
			e := int((r["e"]).(float64))
			if e != 50 && e != 51 {
				t.Errorf("%d. %q\n\nresult mismatch:\n\ngot=%#v\n\n", i, tt.rj, maps)
				break
			}
			p := int(r["p"].(float64))
			if p != 2 {
				t.Errorf("%d. %q\n\nresult mismatch:\n\ngot=%#v\n\n", i, tt.rj, maps)
				break
			}
		}
	}
}

func getResults() []string {
	f, err := os.Open(CACHE_FILE)
	if err != nil {
		panic(err)
	}
	result := make([]string, 0)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		result = append(result, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
	f.Close()
	return result
}

func getExtMockSource(name string, done <-chan int, size int) *nodes.SourceNode {
	var data []*xsql.Tuple
	switch name {
	case "text":
		data = []*xsql.Tuple{
			{
				Emitter: name,
				Message: map[string]interface{}{
					"slogan": "Impossible is nothing",
					"brand":  "Adidas",
				},
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"slogan": "Stronger than dirt",
					"brand":  "Ajax",
				},
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"slogan": "Belong anywhere",
					"brand":  "Airbnb",
				},
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"slogan": "I can't believe I ate the whole thing",
					"brand":  "Alka Seltzer",
				},
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"slogan": "You're in good hands",
					"brand":  "Allstate",
				},
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"slogan": "Don't leave home without it",
					"brand":  "American Express",
				},
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"slogan": "Think different",
					"brand":  "Apple",
				},
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"slogan": "We try harder",
					"brand":  "Avis",
				},
			},
		}

	}
	return nodes.NewSourceNodeWithSource(name, test.NewMockSource(data[:size], done, false), map[string]string{
		"DATASOURCE": name,
	})
}

func setup2() *RuleProcessor {
	log := common.Log

	dbDir, err := common.GetAndCreateDataLoc("test")
	if err != nil {
		log.Panic(err)
	}
	log.Infof("db location is %s", dbDir)

	p := NewStreamProcessor(path.Join(dbDir, "stream"))
	demo := `DROP STREAM text`
	p.ExecStmt(demo)

	demo = "CREATE STREAM text (slogan string, brand string) WITH (DATASOURCE=\"users\", FORMAT=\"JSON\")"
	_, err = p.ExecStmt(demo)
	if err != nil {
		panic(err)
	}

	rp := NewRuleProcessor(dbDir)
	return rp
}

func TestFuncState(t *testing.T) {
	var tests = []struct {
		name string
		sql  string
		r    [][]map[string]interface{}
		s    string
		m    map[string]interface{}
	}{
		{
			name: `rule1`,
			sql:  `SELECT accumulateWordCount(slogan, " ") as wc FROM text`,
			r: [][]map[string]interface{}{
				{{
					"wc": float64(3),
				}},
				{{
					"wc": float64(6),
				}},
				{{
					"wc": float64(8),
				}},
				{{
					"wc": float64(16),
				}},
				{{
					"wc": float64(20),
				}},
				{{
					"wc": float64(25),
				}},
				{{
					"wc": float64(27),
				}},
				{{
					"wc": float64(30),
				}},
			},
			m: map[string]interface{}{
				"op_preprocessor_text_0_exceptions_total":   int64(0),
				"op_preprocessor_text_0_process_latency_ms": int64(0),
				"op_preprocessor_text_0_records_in_total":   int64(8),
				"op_preprocessor_text_0_records_out_total":  int64(8),

				"op_project_0_exceptions_total":   int64(0),
				"op_project_0_process_latency_ms": int64(0),
				"op_project_0_records_in_total":   int64(8),
				"op_project_0_records_out_total":  int64(8),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(8),
				"sink_mockSink_0_records_out_total": int64(8),

				"source_text_0_exceptions_total":  int64(0),
				"source_text_0_records_in_total":  int64(8),
				"source_text_0_records_out_total": int64(8),
			},
			s: "sink_mockSink_0_records_out_total",
		},
	}
	p := setup2()
	for i, tt := range tests {
		p.ExecDrop(tt.name)
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
					source := getExtMockSource(stream, next, 8)
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
		sink := nodes.NewSinkNodeWithSink("mockSink", mockSink, nil)
		tp.AddSink(inputs, sink)
		errCh := tp.Open()
		func() {
			for i := 0; i < 8; i++ {
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
				if err := compareMetrics2(tp, tt.m, tt.sql); err == nil {
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
		if err := compareMetrics2(tp, tt.m, tt.sql); err != nil {
			t.Errorf("%d. %q\n\n%v", i, tt.sql, err)
		}
		tp.Cancel()
	}
}

func compareMetrics2(tp *xstream.TopologyNew, m map[string]interface{}, sql string) (err error) {
	keys, values := tp.GetMetrics()
	//for i, k := range keys {
	//	log.Printf("%s:%v", k, values[i])
	//}
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
