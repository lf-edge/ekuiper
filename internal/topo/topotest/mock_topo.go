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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
	"github.com/lf-edge/ekuiper/v2/internal/processor"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/internal/topo"
	"github.com/lf-edge/ekuiper/v2/internal/topo/planner"
	"github.com/lf-edge/ekuiper/v2/internal/topo/topotest/mockclock"
	"github.com/lf-edge/ekuiper/v2/internal/topo/topotest/mocknode"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func init() {
	testx.InitEnv("topotest")
}

const POSTLEAP = 1000 // Time change after all data sends out
type RuleTest struct {
	Name string
	Sql  string
	R    [][]map[string]any // The result
	M    map[string]any     // final metrics
	T    *api.PrintableTopo // printable topo, an optional field
	W    int                // wait time for each data sending, in milli
}

// CommonResultFunc A function to convert memory sink result to map slice
func CommonResultFunc(result []any) [][]map[string]any {
	maps := make([][]map[string]any, 0, len(result))
	for _, v := range result {
		switch rt := v.(type) {
		case api.ReadonlyMessage:
			maps = append(maps, []map[string]any{rt.ToMap()})
		case []api.ReadonlyMessage:
			nm := make([]map[string]any, 0, len(rt))
			for _, mm := range rt {
				nm = append(nm, mm.ToMap())
			}
			maps = append(maps, nm)
		}
	}
	return maps
}

func DoRuleTest(t *testing.T, tests []RuleTest, j int, opt *api.RuleOption, w int) {
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			id := fmt.Sprintf("%s_%d", tt.Name, j)
			// Create the rule which sink to memory topic
			datas, dataLength, tp, errCh := createTestRule(t, id, tt, opt)
			if tp == nil {
				t.Errorf("topo is not created successfully")
				return
			}
			// Send data with leaps
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
			default:
				// do nothing
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
			// Send async
			go sendData(dataLength, datas, tp, POSTLEAP, wait)
			// Receive data
			limit := len(tt.R)
			consumer := pubsub.CreateSub(id, nil, "", limit)
			ticker := time.After(1000 * time.Second)
			sinkResult := make([]any, 0, limit)
		outerloop:
			for {
				select {
				case sg := <-errCh:
					switch et := sg.(type) {
					case error:
						tp.Cancel()
						assert.Fail(t, et.Error())
						break outerloop
					default:
						fmt.Println("ctrlCh", et)
					}
				case tuple := <-consumer:
					sinkResult = append(sinkResult, tuple)
					limit--
					if limit <= 0 {
						break outerloop
					}
				case <-ticker:
					tp.Cancel()
					assert.Fail(t, "timeout")
					break outerloop
				}
			}
			assert.Equal(t, tt.R, CommonResultFunc(sinkResult))
			err := CompareMetrics(tp, tt.M)
			assert.NoError(t, err)
		})
	}
}

func CompareMetrics(tp *topo.Topo, m map[string]interface{}) (err error) {
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
					}
					break
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
		// do not find
		if index < len(values) {
			return fmt.Errorf("metrics mismatch for %s:\n\nexp=%#v(%T)\n\ngot=%#v(%T)\n\n", k, v, v, values[index], values[index])
		} else {
			return fmt.Errorf("metrics mismatch for %s:\n\nexp=%#v\n\ngot=nil\n\n", k, v)
		}
	}
	return nil
}

func sendData(dataLength int, datas [][]*xsql.Tuple, tp *topo.Topo, postleap int, wait int) {
	// Send data and move time
	mockClock := mockclock.GetMockClock()
	// Set the current time
	mockClock.Add(0)
	// TODO assume multiple data source send the data in order and has the same length
	for i := 0; i < dataLength; i++ {
		// wait for table to load
		time.Sleep(100 * time.Millisecond)
		for _, d := range datas {
			time.Sleep(time.Duration(wait) * time.Millisecond)
			// Make sure time is going forward only
			// gradually add uptime to ensure checkpoint is triggered before the data send
			for n := timex.GetNowInMilli() + 100; d[i].Timestamp+100 > n; n += 100 {
				if d[i].Timestamp < n {
					n = d[i].Timestamp
				}
				mockClock.Set(cast.TimeFromUnixMilli(n))
				conf.Log.Debugf("Clock set to %d", timex.GetNowInMilli())
				time.Sleep(1 * time.Millisecond)
			}
			select {
			case <-tp.GetContext().Done():
				return
			default:
			}
		}
	}
	mockClock.Add(time.Duration(postleap) * time.Millisecond)
	conf.Log.Debugf("Clock add to %d", timex.GetNowInMilli())
	//// Check if stream done. Poll for metrics,
	//time.Sleep(10 * time.Millisecond)
	//var retry int
	//for retry = 4; retry > 0; retry-- {
	//	var err error
	//	if err = CompareMetrics(tp, metrics); err == nil {
	//		break
	//	}
	//	conf.Log.Errorf("check metrics error at %d: %s", retry, err)
	//	time.Sleep(1000 * time.Millisecond)
	//}
	//if retry == 0 {
	//	t.Error("send data timeout")
	//} else if retry < 2 {
	//	conf.Log.Debugf("try %d for metric comparison\n", 2-retry)
	//}
	//return nil
}

// create a test rule with memory sink
func createTestRule(t *testing.T, id string, tt RuleTest, opt *api.RuleOption) ([][]*xsql.Tuple, int, *topo.Topo, <-chan error) {
	mockclock.ResetClock(1541152486000)
	// Create stream
	var (
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
	tp, err := planner.Plan(&api.Rule{
		Id:  id,
		Sql: tt.Sql,
		Actions: []map[string]any{
			{
				"memory": map[string]any{
					"topic":      id,
					"sendSingle": true,
				},
			},
		},
		Options: opt,
	})
	if err != nil {
		t.Error(err)
		return nil, 0, nil, nil
	}
	errCh := tp.Open()
	return datas, dataLength, tp, errCh
}

type RuleCheckpointTest struct {
	RuleTest
	PauseSize   int                    // Stop stream after sending pauseSize source to test checkpoint resume
	Cc          int                    // checkpoint count when paused
	PauseMetric map[string]interface{} // The metric to check when paused
}

//func DoCheckpointRuleTest(t *testing.T, tests []RuleCheckpointTest, j int, opt *api.RuleOption) {
//	fmt.Printf("The test bucket for option %d size is %d.\n\n", j, len(tests))
//	for i, tt := range tests {
//		datas, dataLength, tp, errCh := createTestRule(t, tt.RuleTest, j, opt)
//		if tp == nil {
//			t.Errorf("topo is not created successfully")
//			break
//		}
//		var retry int
//		for retry = 10; retry > 0; retry-- {
//			if tp.GetCoordinator() == nil || !tp.GetCoordinator().IsActivated() {
//				conf.Log.Debugf("waiting for coordinator ready %d\n", retry)
//				time.Sleep(10 * time.Millisecond)
//			} else {
//				break
//			}
//		}
//		if retry == 0 {
//			t.Error("coordinator timeout")
//			t.FailNow()
//		}
//		conf.Log.Debugf("Start sending first phase data done at %d", timex.GetNowInMilli())
//		if err := sendData(t, tt.PauseSize, tt.PauseMetric, datas, errCh, tp, 100, 100); err != nil {
//			t.Errorf("first phase send data error %s", err)
//			break
//		}
//		conf.Log.Debugf("Send first phase data done at %d", timex.GetNowInMilli())
//		// compare checkpoint count
//		time.Sleep(10 * time.Millisecond)
//		for retry = 3; retry > 0; retry-- {
//			actual := tp.GetCoordinator().GetCompleteCount()
//			if tt.Cc == actual {
//				break
//			}
//			conf.Log.Debugf("check checkpointCount error at %d: %d\n", retry, actual)
//			time.Sleep(200 * time.Millisecond)
//		}
//		cc := tp.GetCoordinator().GetCompleteCount()
//		tp.Cancel()
//		if retry == 0 {
//			t.Errorf("%d-%d. checkpoint count\n\nresult mismatch:\n\nexp=%#v\n\ngot=%d\n\n", i, j, tt.Cc, cc)
//			return
//		} else if retry < 3 {
//			conf.Log.Debugf("try %d for checkpoint count\n", 4-retry)
//		}
//		tp.Cancel()
//		time.Sleep(10 * time.Millisecond)
//		// resume stream
//		conf.Log.Debugf("Resume stream at %d", timex.GetNowInMilli())
//		errCh = tp.Open()
//		conf.Log.Debugf("After open stream at %d", timex.GetNowInMilli())
//		if err := sendData(t, dataLength, tt.M, datas, errCh, tp, POSTLEAP, 10); err != nil {
//			t.Errorf("second phase send data error %s", err)
//			break
//		}
//		compareResult(t, mockSink, CommonResultFunc, tt.RuleTest, i, tp)
//	}
//}

//func CreateRule(name, sql string) (*api.Rule, error) {
//	p := processor.NewRuleProcessor()
//	p.ExecDrop(name)
//	return p.ExecCreateWithValidation(name, sql)
//}

// HandleStream Create or drop streams
func HandleStream(createOrDrop bool, names []string, t *testing.T) {
	p := processor.NewStreamProcessor()
	for _, name := range names {
		var sql string
		if createOrDrop {
			switch name {
			case "sharedDemo":
				sql = `CREATE STREAM sharedDemo () WITH (DATASOURCE="sharedDemo", TYPE="mock", FORMAT="json", SHARED="true");`
			case "demoE3":
				sql = `CREATE STREAM demoE3 () WITH (DATASOURCE="demoE3", TYPE="mock", FORMAT="json", KEY="ts", TIMESTAMP="ts");`
			case "demoE2":
				sql = `CREATE STREAM demoE2 () WITH (DATASOURCE="demoE2", TYPE="mock", FORMAT="json", KEY="ts", TIMESTAMP="ts");`
			case "demoArr2":
				sql = `CREATE STREAM demoArr2 () WITH (DATASOURCE="demoArr2", TYPE="mock", FORMAT="json", KEY="ts");`
			case "demoArr":
				sql = `CREATE STREAM demoArr () WITH (DATASOURCE="demoArr", TYPE="mock", FORMAT="json", KEY="ts");`
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
				) WITH (DATASOURCE="demoError", TYPE="mock", FORMAT="json", KEY="ts",STRICT_VALIDATION="true");`
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
				) WITH (DATASOURCE="demoErr", TYPE="mock", FORMAT="json", KEY="ts", TIMESTAMP="ts",STRICT_VALIDATION="true");`
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
				sql = "CREATE STREAM ext (count bigint) WITH (DATASOURCE=\"ext\", FORMAT=\"JSON\", TYPE=\"random\", CONF_KEY=\"ext\",STRICT_VALIDATION=\"true\")"
			case "ext2":
				sql = "CREATE STREAM ext2 (count bigint) WITH (DATASOURCE=\"ext2\", FORMAT=\"JSON\", TYPE=\"random\", CONF_KEY=\"dedup\")"
			case "extpy":
				sql = "CREATE STREAM extpy (name string, value bigint) WITH (FORMAT=\"JSON\", TYPE=\"pyjson\", CONF_KEY=\"ext\")"
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
			case "optional_commands":
				sql = `CREATE STREAM optional_commands (base64_img string) WITH (DATASOURCE="optional_commands", FORMAT="JSON", TYPE="mock")`
			case "schemaless_commands":
				sql = `CREATE STREAM schemaless_commands (cmd string, base64_img string, encoded_json STRUCT(name STRING, size BIGINT)) WITH (DATASOURCE="schemaless_commands", FORMAT="JSON", TYPE="mock")`
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
