package topotest

import (
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/internal/conf"
	"github.com/emqx/kuiper/internal/processor"
	"github.com/emqx/kuiper/internal/testx"
	"github.com/emqx/kuiper/internal/topo"
	"github.com/emqx/kuiper/internal/topo/node"
	"github.com/emqx/kuiper/internal/topo/planner"
	"github.com/emqx/kuiper/internal/topo/topotest/mockclock"
	"github.com/emqx/kuiper/internal/topo/topotest/mocknode"
	"github.com/emqx/kuiper/internal/xsql"
	"github.com/emqx/kuiper/pkg/api"
	"github.com/emqx/kuiper/pkg/ast"
	"github.com/emqx/kuiper/pkg/cast"
	"path"
	"reflect"
	"strings"
	"testing"
	"time"
)

const POSTLEAP = 1000 // Time change after all data sends out
type RuleTest struct {
	Name string
	Sql  string
	R    interface{}            // The result
	M    map[string]interface{} // final metrics
	T    *topo.PrintableTopo    // printable topo, an optional field
	W    int                    // wait time for each data sending, in milli
}

var (
	DbDir = testx.GetDbDir()
)

func compareMetrics(tp *topo.Topo, m map[string]interface{}) (err error) {
	keys, values := tp.GetMetrics()
	for k, v := range m {
		var (
			index   int
			key     string
			matched bool
		)
		for index, key = range keys {
			if k == key {
				if strings.HasSuffix(k, "process_latency_us") {
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
		if conf.Config.Basic.Debug == true {
			for i, k := range keys {
				conf.Log.Printf("%s:%v", k, values[i])
			}
		}
		//do not find
		if index < len(values) {
			return fmt.Errorf("metrics mismatch for %s:\n\nexp=%#v(%T)\n\ngot=%#v(%T)\n\n", k, v, v, values[index], values[index])
		} else {
			return fmt.Errorf("metrics mismatch for %s:\n\nexp=%#v\n\ngot=nil\n\n", k, v)
		}
	}
	return nil
}

func commonResultFunc(result [][]byte) interface{} {
	var maps [][]map[string]interface{}
	for _, v := range result {
		var mapRes []map[string]interface{}
		err := json.Unmarshal(v, &mapRes)
		if err != nil {
			panic("Failed to parse the input into map")
		}
		maps = append(maps, mapRes)
	}
	return maps
}

func DoRuleTest(t *testing.T, tests []RuleTest, j int, opt *api.RuleOption, wait int) {
	doRuleTestBySinkProps(t, tests, j, opt, wait, nil, commonResultFunc)
}

func doRuleTestBySinkProps(t *testing.T, tests []RuleTest, j int, opt *api.RuleOption, w int, sinkProps map[string]interface{}, resultFunc func(result [][]byte) interface{}) {
	fmt.Printf("The test bucket for option %d size is %d.\n\n", j, len(tests))
	for i, tt := range tests {
		datas, dataLength, tp, mockSink, errCh := createStream(t, tt, j, opt, sinkProps)
		if tp == nil {
			t.Errorf("topo is not created successfully")
			break
		}
		wait := tt.W
		if wait == 0 {
			if w > 0 {
				wait = w
			} else {
				wait = 5
			}
		}
		switch opt.Qos {
		case api.ExactlyOnce:
			wait *= 10
		case api.AtLeastOnce:
			wait *= 3
		}
		var retry int
		if opt.Qos > api.AtMostOnce {
			for retry = 3; retry > 0; retry-- {
				if tp.GetCoordinator() == nil || !tp.GetCoordinator().IsActivated() {
					conf.Log.Debugf("waiting for coordinator ready %d\n", retry)
					time.Sleep(10 * time.Millisecond)
				} else {
					break
				}
			}
			if retry < 0 {
				t.Error("coordinator timeout")
				t.FailNow()
			}
		}
		if err := sendData(t, dataLength, tt.M, datas, errCh, tp, POSTLEAP, wait); err != nil {
			t.Errorf("send data error %s", err)
			break
		}
		compareResult(t, mockSink, resultFunc, tt, i, tp)
	}
}

func compareResult(t *testing.T, mockSink *mocknode.MockSink, resultFunc func(result [][]byte) interface{}, tt RuleTest, i int, tp *topo.Topo) {
	// Check results
	results := mockSink.GetResults()
	maps := resultFunc(results)

	if !reflect.DeepEqual(tt.R, maps) {
		t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.Sql, tt.R, maps)
	}
	if err := compareMetrics(tp, tt.M); err != nil {
		t.Errorf("%d. %q\n\nmetrics mismatch:\n\n%s\n\n", i, tt.Sql, err)
	}
	if tt.T != nil {
		topo := tp.GetTopo()
		if !reflect.DeepEqual(tt.T, topo) {
			t.Errorf("%d. %q\n\ntopo mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.Sql, tt.T, topo)
		}
	}
	tp.Cancel()
}

func sendData(t *testing.T, dataLength int, metrics map[string]interface{}, datas [][]*xsql.Tuple, errCh <-chan error, tp *topo.Topo, postleap int, wait int) error {
	// Send data and move time
	mockClock := mockclock.GetMockClock()
	// Set the current time
	mockClock.Add(0)
	// TODO assume multiple data source send the data in order and has the same length
	for i := 0; i < dataLength; i++ {
		for _, d := range datas {
			time.Sleep(time.Duration(wait) * time.Millisecond)
			// Make sure time is going forward only
			// gradually add up time to ensure checkpoint is triggered before the data send
			for n := conf.GetNowInMilli() + 100; d[i].Timestamp+100 > n; n += 100 {
				if d[i].Timestamp < n {
					n = d[i].Timestamp
				}
				mockClock.Set(cast.TimeFromUnixMilli(n))
				conf.Log.Debugf("Clock set to %d", conf.GetNowInMilli())
				time.Sleep(1)
			}
			select {
			case err := <-errCh:
				t.Log(err)
				tp.Cancel()
				return err
			default:
			}
		}
	}
	mockClock.Add(time.Duration(postleap) * time.Millisecond)
	conf.Log.Debugf("Clock add to %d", conf.GetNowInMilli())
	// Check if stream done. Poll for metrics,
	time.Sleep(10 * time.Millisecond)
	var retry int
	for retry = 4; retry > 0; retry-- {
		if err := compareMetrics(tp, metrics); err == nil {
			break
		} else {
			conf.Log.Errorf("check metrics error at %d: %s", retry, err)
		}
		time.Sleep(1000 * time.Millisecond)
	}
	if retry == 0 {
		t.Error("send data timeout")
	} else if retry < 2 {
		conf.Log.Debugf("try %d for metric comparison\n", 2-retry)
	}
	return nil
}

func createStream(t *testing.T, tt RuleTest, j int, opt *api.RuleOption, sinkProps map[string]interface{}) ([][]*xsql.Tuple, int, *topo.Topo, *mocknode.MockSink, <-chan error) {
	mockclock.ResetClock(1541152486000)
	// Create stream
	var (
		sources    []*node.SourceNode
		datas      [][]*xsql.Tuple
		dataLength int
	)

	parser := xsql.NewParser(strings.NewReader(tt.Sql))
	if stmt, err := xsql.Language.Parse(parser); err != nil {
		t.Errorf("parse sql %s error: %s", tt.Sql, err)
	} else {
		if selectStmt, ok := stmt.(*ast.SelectStatement); !ok {
			t.Errorf("sql %s is not a select statement", tt.Sql)
		} else {
			streams := xsql.GetStreams(selectStmt)
			for _, stream := range streams {
				data, ok := mocknode.TestData[stream]
				if !ok {
					continue
				}
				dataLength = len(data)
				datas = append(datas, data)
			}
		}
	}
	mockSink := mocknode.NewMockSink()
	sink := node.NewSinkNodeWithSink("mockSink", mockSink, sinkProps)
	tp, err := planner.PlanWithSourcesAndSinks(&api.Rule{Id: fmt.Sprintf("%s_%d", tt.Name, j), Sql: tt.Sql, Options: opt}, DbDir, sources, []*node.SinkNode{sink})
	if err != nil {
		t.Error(err)
		return nil, 0, nil, nil, nil
	}
	errCh := tp.Open()
	return datas, dataLength, tp, mockSink, errCh
}

// Create or drop streams
func HandleStream(createOrDrop bool, names []string, t *testing.T) {
	p := processor.NewStreamProcessor(path.Join(DbDir, "stream"))
	for _, name := range names {
		var sql string
		if createOrDrop {
			switch name {
			case "demo":
				sql = `CREATE STREAM demo (
					color STRING,
					size BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="demo", TYPE="mock", FORMAT="json", KEY="ts");`
			case "demoError":
				sql = `CREATE STREAM demoError (
					color STRING,
					size BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="demoError", TYPE="mock", FORMAT="json", KEY="ts");`
			case "demo1":
				sql = `CREATE STREAM demo1 (
					temp FLOAT,
					hum BIGINT,` +
					"`from`" + ` STRING,
					ts BIGINT
				) WITH (DATASOURCE="demo1", TYPE="mock", FORMAT="json", KEY="ts");`
			case "demoTable":
				sql = `CREATE TABLE demoTable (
					device STRING,
					ts BIGINT
				) WITH (DATASOURCE="demoTable", TYPE="mock", RETAIN_SIZE="3");`
			case "sessionDemo":
				sql = `CREATE STREAM sessionDemo (
					temp FLOAT,
					hum BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="sessionDemo", TYPE="mock", FORMAT="json", KEY="ts");`
			case "demoE":
				sql = `CREATE STREAM demoE (
					color STRING,
					size BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="demoE", TYPE="mock", FORMAT="json", KEY="ts", TIMESTAMP="ts");`
			case "demo1E":
				sql = `CREATE STREAM demo1E (
					temp FLOAT,
					hum BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="demo1E", TYPE="mock", FORMAT="json", KEY="ts", TIMESTAMP="ts");`
			case "sessionDemoE":
				sql = `CREATE STREAM sessionDemoE (
					temp FLOAT,
					hum BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="sessionDemoE", TYPE="mock", FORMAT="json", KEY="ts", TIMESTAMP="ts");`
			case "demoErr":
				sql = `CREATE STREAM demoErr (
					color STRING,
					size BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="demoErr", TYPE="mock", FORMAT="json", KEY="ts", TIMESTAMP="ts");`
			case "ldemo":
				sql = `CREATE STREAM ldemo (					
				) WITH (DATASOURCE="ldemo", TYPE="mock", FORMAT="json");`
			case "ldemo1":
				sql = `CREATE STREAM ldemo1 (
				) WITH (DATASOURCE="ldemo1", TYPE="mock", FORMAT="json");`
			case "lsessionDemo":
				sql = `CREATE STREAM lsessionDemo (
				) WITH (DATASOURCE="lsessionDemo", TYPE="mock", FORMAT="json");`
			case "ext":
				sql = "CREATE STREAM ext (count bigint) WITH (DATASOURCE=\"ext\", TYPE=\"mock\", FORMAT=\"JSON\", TYPE=\"random\", CONF_KEY=\"ext\")"
			case "ext2":
				sql = "CREATE STREAM ext2 (count bigint) WITH (DATASOURCE=\"ext2\", TYPE=\"mock\", FORMAT=\"JSON\", TYPE=\"random\", CONF_KEY=\"dedup\")"
			case "text":
				sql = "CREATE STREAM text (slogan string, brand string) WITH (DATASOURCE=\"text\", TYPE=\"mock\", FORMAT=\"JSON\")"
			case "binDemo":
				sql = "CREATE STREAM binDemo () WITH (DATASOURCE=\"binDemo\", TYPE=\"mock\", FORMAT=\"BINARY\")"
			case "table1":
				sql = `CREATE TABLE table1 (
					name STRING,
					size BIGINT,
					id BIGINT
				) WITH (DATASOURCE="lookup.json", FORMAT="json", CONF_KEY="test");`
			case "helloStr":
				sql = `CREATE STREAM helloStr (name string) WITH (DATASOURCE="helloStr", TYPE="mock", FORMAT="JSON")`
			case "commands":
				sql = `CREATE STREAM commands (cmd string, base64_img string, encoded_json string) WITH (DATASOURCE="commands", FORMAT="JSON", TYPE="mock")`
			case "fakeBin":
				sql = "CREATE STREAM fakeBin () WITH (DATASOURCE=\"fakeBin\", TYPE=\"mock\", FORMAT=\"BINARY\")"
			case "shelves":
				sql = `CREATE STREAM shelves (
					name string,
					size BIGINT,
					shelf STRUCT(theme STRING,id BIGINT, subfield STRING)
				) WITH (DATASOURCE="shelves", TYPE="mock", FORMAT="json");`
			case "mes":
				sql = `CREATE STREAM mes (message_id string, text string) WITH (DATASOURCE="mes", TYPE="mock", FORMAT="JSON")`
			default:
				t.Errorf("create stream %s fail", name)
			}
		} else {
			if strings.Index(name, "table") == 0 {
				sql = `DROP TABLE ` + name
			} else {
				sql = `DROP STREAM ` + name
			}
		}

		_, err := p.ExecStmt(sql)
		if err != nil {
			t.Log(err)
		}
	}
}

type RuleCheckpointTest struct {
	RuleTest
	PauseSize   int                    // Stop stream after sending pauseSize source to test checkpoint resume
	Cc          int                    // checkpoint count when paused
	PauseMetric map[string]interface{} // The metric to check when paused
}

func DoCheckpointRuleTest(t *testing.T, tests []RuleCheckpointTest, j int, opt *api.RuleOption) {
	fmt.Printf("The test bucket for option %d size is %d.\n\n", j, len(tests))
	for i, tt := range tests {
		datas, dataLength, tp, mockSink, errCh := createStream(t, tt.RuleTest, j, opt, nil)
		if tp == nil {
			t.Errorf("topo is not created successfully")
			break
		}
		var retry int
		for retry = 10; retry > 0; retry-- {
			if tp.GetCoordinator() == nil || !tp.GetCoordinator().IsActivated() {
				conf.Log.Debugf("waiting for coordinator ready %d\n", retry)
				time.Sleep(10 * time.Millisecond)
			} else {
				break
			}
		}
		if retry == 0 {
			t.Error("coordinator timeout")
			t.FailNow()
		}
		conf.Log.Debugf("Start sending first phase data done at %d", conf.GetNowInMilli())
		if err := sendData(t, tt.PauseSize, tt.PauseMetric, datas, errCh, tp, 100, 100); err != nil {
			t.Errorf("first phase send data error %s", err)
			break
		}
		conf.Log.Debugf("Send first phase data done at %d", conf.GetNowInMilli())
		// compare checkpoint count
		time.Sleep(10 * time.Millisecond)
		for retry = 3; retry > 0; retry-- {
			actual := tp.GetCoordinator().GetCompleteCount()
			if tt.Cc == actual {
				break
			} else {
				conf.Log.Debugf("check checkpointCount error at %d: %d\n", retry, actual)
			}
			time.Sleep(200 * time.Millisecond)
		}
		cc := tp.GetCoordinator().GetCompleteCount()
		tp.Cancel()
		if retry == 0 {
			t.Errorf("%d-%d. checkpoint count\n\nresult mismatch:\n\nexp=%#v\n\ngot=%d\n\n", i, j, tt.Cc, cc)
			return
		} else if retry < 3 {
			conf.Log.Debugf("try %d for checkpoint count\n", 4-retry)
		}
		tp.Cancel()
		time.Sleep(10 * time.Millisecond)
		// resume stream
		conf.Log.Debugf("Resume stream at %d", conf.GetNowInMilli())
		errCh = tp.Open()
		conf.Log.Debugf("After open stream at %d", conf.GetNowInMilli())
		if err := sendData(t, dataLength, tt.M, datas, errCh, tp, POSTLEAP, 10); err != nil {
			t.Errorf("second phase send data error %s", err)
			break
		}
		compareResult(t, mockSink, commonResultFunc, tt.RuleTest, i, tp)
	}
}

func CreateRule(name, sql string) (*api.Rule, error) {
	p := processor.NewRuleProcessor(DbDir)
	p.ExecDrop(name)
	return p.ExecCreate(name, sql)
}
