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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	pkgstore "github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/processor"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/internal/topo"
	"github.com/lf-edge/ekuiper/v2/internal/topo/planner"
	"github.com/lf-edge/ekuiper/v2/internal/topo/topotest/mocknode"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
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
	T    *def.PrintableTopo // printable topo, an optional field
	W    int                // wait time for each data sending, in milli
	TL   int                // table load wait time before first data, in milli (for tests using lookup tables)
}

// CommonResultFunc A function to convert memory sink result to map slice
func CommonResultFunc(result []any) [][]map[string]any {
	maps := make([][]map[string]any, 0, len(result))
	for _, v := range result {
		switch rt := v.(type) {
		case pubsub.MemTuple:
			m := rt.ToMap()
			maps = append(maps, []map[string]any{m})
		case []pubsub.MemTuple:
			nm := make([]map[string]any, 0, len(rt))
			for _, mm := range rt {
				nm = append(nm, mm.ToMap())
			}
			maps = append(maps, nm)
		default:
			conf.Log.Errorf("receive wrong tuple %v", rt)
		}
	}
	return maps
}

func DoRuleTest(t *testing.T, tests []RuleTest, opt *def.RuleOption, w int) {
	DoRuleTestWithResultFunc(t, tests, opt, w, CommonResultFunc)
}

func DoRuleTestWithResultFunc(t *testing.T, tests []RuleTest, opt *def.RuleOption, w int, resultFunc func(result []any) [][]map[string]any) {
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			id := strings.ReplaceAll(strings.ReplaceAll(t.Name(), "#", "_"), "/", "_")
			conf.Log.Debugf("run test %s", id)
			// Drop any stale checkpoint state from a previous run of the same rule ID
			if opt != nil && opt.Qos >= def.AtLeastOnce {
				_ = pkgstore.DropTS(id)
			}
			// Create the rule which sink to memory topic
			datas, dataLength, tp, errCh := createTestRule(t, id, tt, opt)
			if tp == nil {
				t.Errorf("topo is not created successfully")
				return
			}
			defer tp.Cancel()
			// Send data with leaps
			wait := tt.W
			if wait == 0 {
				if w > 0 {
					wait = w
				} else {
					wait = 50
				}
			}
			switch opt.Qos {
			case def.ExactlyOnce:
				wait *= 10
			case def.AtLeastOnce:
				wait *= 3
			default:
				// do nothing
			}
			var retry int
			if opt.Qos > def.AtMostOnce {
				for retry = 20; retry > 0; retry-- {
					if tp.GetCoordinator() == nil || !tp.GetCoordinator().IsActivated() {
						conf.Log.Debugf("waiting for coordinator ready %d\n", retry)
						time.Sleep(50 * time.Millisecond)
					} else {
						break
					}
				}
				if retry == 0 {
					t.Error("coordinator timeout")
					t.FailNow()
				}
			}
			waitTopoReady(t, tp, id)
			// Receive data
			limit := len(tt.R)
			consumer := pubsub.CreateSub(id, nil, id, limit+5)
			conf.Log.Debugf("test create memory sub %s", id)
			ticker := time.After(30 * time.Second)
			sinkResult := make([]any, 0, limit+5)
			// Signal sendData to start only after the main loop's select is set up
			ready := make(chan struct{})
			go func() {
				<-ready
				sendData(dataLength, datas, tp, POSTLEAP, wait, tt.TL)
			}()
			close(ready)
		outerloop:
			for {
				select {
				// the done signal is sent when all sources are EOF. The mock source is bounded so this will be triggered.
				case err := <-errCh:
					conf.Log.Debugf("test %s receive error signal: %v", id, err)
					_ = tp.GracefulStop(0)
					break outerloop
				case tuple := <-consumer:
					sinkResult = append(sinkResult, tuple)
					conf.Log.Debugf("test %s append result %v", id, tuple)
					if dataLength == 0 && len(sinkResult) == limit {
						break outerloop
					}
				case <-ticker:
					_ = tp.GracefulStop(0)
					assert.Fail(t, "timeout")
					break outerloop
				}
			}
		drainloop:
			for {
				select {
				case tuple := <-consumer:
					sinkResult = append(sinkResult, tuple)
					conf.Log.Debugf("test %s append result %v", id, tuple)
				case <-time.After(100 * time.Millisecond):
					break drainloop
				}
			}
			conf.Log.Debugf("test %s receive %d result", id, len(sinkResult))
			actual := resultFunc(sinkResult)
			if len(actual) > len(tt.R) && len(tt.R) > 0 {
				allEmpty := true
				for i := len(tt.R); i < len(actual); i++ {
					if len(actual[i]) > 0 {
						allEmpty = false
						break
					}
				}
				if allEmpty {
					actual = actual[:len(tt.R)]
				}
			}
			assert.Equal(t, tt.R, actual)
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
		if conf.Config.Basic.Debug {
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

func sendData(dataLength int, datas [][]*xsql.Tuple, tp *topo.Topo, postleap int, wait int, tableLoadWait int) {
	// TODO assume multiple data source send the data in order and has the same length
	conf.Log.Infof("Send clock init to %d", timex.GetNowInMilli())
	// wait for table to load (only for tests using lookup tables)
	if tableLoadWait > 0 {
		time.Sleep(time.Duration(tableLoadWait) * time.Millisecond)
	}
	for i := 0; i < dataLength; i++ {
		for _, d := range datas {
			time.Sleep(time.Duration(wait) * time.Millisecond)
			// Make sure time is going forward only
			// gradually add uptime to ensure checkpoint is triggered before the data send
			for n := timex.GetNow().Add(100 * time.Millisecond); d[i].Timestamp.Add(100 * time.Millisecond).After(n); n = n.Add(100 * time.Millisecond) {
				if d[i].Timestamp.Before(n) {
					n = d[i].Timestamp
				}
				timex.Set(n.UnixMilli())
				conf.Log.Infof("Clock set to %d", timex.GetNowInMilli())
				time.Sleep(1 * time.Millisecond)
			}
			select {
			case <-tp.GetContext().Done():
				return
			default:
			}
		}
	}
	timex.Add(time.Duration(postleap) * time.Millisecond)
	conf.Log.Infof("Clock add to %d", timex.GetNowInMilli())
}

// create a test rule with memory sink
func createTestRule(t *testing.T, id string, tt RuleTest, opt *def.RuleOption) ([][]*xsql.Tuple, int, *topo.Topo, <-chan error) {
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
	if len(datas) > 0 {
		startTs := datas[0][0].Timestamp.UnixMilli() / 1000 * 1000
		for startTs != timex.GetNowInMilli() {
			timex.Set(startTs)
			conf.Log.Infof("Init Clock to %d, and now is %d", startTs, timex.GetNowInMilli())
		}
	}
	rule := &def.Rule{
		Id:  id,
		Sql: tt.Sql,
		Actions: []map[string]any{
			{
				"memory": map[string]any{
					"topic":      id,
					"sendSingle": false,
				},
			},
		},
		Options: opt,
	}
	tp, err := planner.Plan(rule)
	if err != nil {
		t.Error(err)
		return nil, 0, nil, nil
	}
	errCh := tp.Open()
	return datas, dataLength, tp, errCh
}

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
				sql = "CREATE STREAM ext (count bigint) WITH (DATASOURCE=\"ext\", FORMAT=\"JSON\", TYPE=\"random\", CONF_KEY=\"ext\",STRICT_VALIDATION=\"true\", INTERVAL=\"10ms\")"
			case "ext2":
				sql = "CREATE STREAM ext2 (count bigint) WITH (DATASOURCE=\"ext2\", FORMAT=\"JSON\", TYPE=\"random\", CONF_KEY=\"dedup\", INTERVAL=\"10ms\")"
			case "text":
				sql = "CREATE STREAM text (slogan string, brand string) WITH (DATASOURCE=\"text\", TYPE=\"mock\", FORMAT=\"JSON\")"
			case "binDemo":
				sql = "CREATE STREAM binDemo () WITH (DATASOURCE=\"binDemo\", TYPE=\"mock\", FORMAT=\"BINARY\")"
			case "table1":
				sql = `CREATE TABLE table1 (
					name STRING,
					size BIGINT,
					id BIGINT
				) WITH (DATASOURCE="lookup.json", FORMAT="json", CONF_KEY="test", STRICT_VALIDATION="true");`
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

type RuleCheckpointTest struct {
	RuleTest
	PauseSize int // Stop stream after sending pauseSize source to test checkpoint resume
	Cc        int // checkpoint count when paused
	// PauseMetric map[string]interface{} // The metric to check when paused
}

func DoCheckpointRuleTest(t *testing.T, tests []RuleCheckpointTest, opt *def.RuleOption, w int) {
	fmt.Printf("The test bucket for option %d size is %d.\n\n", w, len(tests))
	for i, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			id := strings.ReplaceAll(strings.ReplaceAll(t.Name(), "#", "_"), "/", "_")
			conf.Log.Debugf("run test %s", id)
			// Drop any stale checkpoint state from a previous run (but NOT during restart after checkpoint)
			if opt != nil && opt.Qos >= def.AtLeastOnce {
				_ = pkgstore.DropTS(id)
			}
			// Create the rule which sink to memory topic
			datas, dataLength, tp, _ := createTestRule(t, id, tt.RuleTest, opt)
			if tp == nil {
				t.Errorf("topo is not created successfully")
				return
			}
			defer tp.Cancel()
			// Send data with leaps
			wait := tt.W
			if wait == 0 {
				if w > 0 {
					wait = w
				} else {
					wait = 50
				}
			}
			switch opt.Qos {
			case def.ExactlyOnce:
				wait *= 10
			case def.AtLeastOnce:
				wait *= 3
			default:
				// do nothing
			}
			var retry int
			if opt.Qos > def.AtMostOnce {
				for retry = 20; retry > 0; retry-- {
					if tp.GetCoordinator() == nil || !tp.GetCoordinator().IsActivated() {
						conf.Log.Debugf("waiting for coordinator ready %d\n", retry)
						time.Sleep(50 * time.Millisecond)
					} else {
						break
					}
				}
				if retry == 0 {
					t.Error("coordinator timeout")
					t.FailNow()
				}
			}
			waitTopoReady(t, tp, id)
			if opt.Qos == def.ExactlyOnce {
				time.Sleep(100 * time.Millisecond)
			}
			// Send async
			go sendData(tt.PauseSize, datas, tp, 100, wait, 0)
			conf.Log.Debugf("Send first phase data done at %d", timex.GetNowInMilli())
			// compare checkpoint count
			// Wait longer than checkpoint interval (2s) to ensure at least one checkpoint completes
			time.Sleep(3 * time.Second)
			for retry = 10; retry > 0; retry-- {
				actual := tp.GetCoordinator().GetCompleteCount()
				if tt.Cc == actual {
					break
				}
				conf.Log.Debugf("check checkpointCount error at %d: %d\n", retry, actual)
				time.Sleep(500 * time.Millisecond)
			}
			cc := tp.GetCoordinator().GetCompleteCount()
			_ = tp.GracefulStop(0)
			if retry == 0 {
				t.Errorf("%d-%d. checkpoint count\n\nresult mismatch:\n\nexp=%#v\n\ngot=%d\n\n", i, w, tt.Cc, cc)
				return
			} else if retry < 3 {
				conf.Log.Debugf("try %d for checkpoint count\n", 4-retry)
			}
			time.Sleep(50 * time.Millisecond)
			// resume stream
			conf.Log.Debugf("Resume stream at %d", timex.GetNowInMilli())
			_, _, tp, errCh := createTestRule(t, id, tt.RuleTest, opt)
			if tp == nil {
				t.Errorf("topo is not created successfully")
				return
			}

			conf.Log.Debugf("After open stream at %d", timex.GetNowInMilli())
			waitTopoReady(t, tp, id)
			// Receive data
			limit := len(tt.R)
			consumer := pubsub.CreateSub(id, nil, id, limit+5)
			conf.Log.Debugf("test create memory sub %s", id)
			time.Sleep(20 * time.Millisecond)
			go sendData(dataLength-tt.PauseSize, [][]*xsql.Tuple{datas[0][tt.PauseSize:]}, tp, POSTLEAP, 10, 0)
			ticker := time.After(1000 * time.Second)
			sinkResult := make([]any, 0, limit+5)
		outerloop:
			for {
				select {
				case <-errCh:
					conf.Log.Debugf("test %s receive error signal", id)
					_ = tp.GracefulStop(0)
					break outerloop
				case tuple := <-consumer:
					sinkResult = append(sinkResult, tuple)
					conf.Log.Debugf("test %s append result %v", id, tuple)
				case <-ticker:
					_ = tp.GracefulStop(0)
					assert.Fail(t, "timeout")
					break outerloop
				}
			}
		outloop:
			for {
				select {
				case tuple := <-consumer:
					sinkResult = append(sinkResult, tuple)
					conf.Log.Debugf("test %s append result %v", id, tuple)
				case <-time.After(100 * time.Millisecond):
					break outloop
				}
			}
			conf.Log.Debugf("test %s receive %d result", id, len(sinkResult))
			actual := CommonResultFunc(sinkResult)
			if len(actual) > len(tt.R) && len(tt.R) > 0 {
				allEmpty := true
				for i := len(tt.R); i < len(actual); i++ {
					if len(actual[i]) > 0 {
						allEmpty = false
						break
					}
				}
				if allEmpty {
					actual = actual[:len(tt.R)]
				}
			}
			assert.Equal(t, tt.R, actual)
			err := CompareMetrics(tp, tt.M)
			assert.NoError(t, err)
		})
	}
}

func waitTopoReady(t *testing.T, tp *topo.Topo, id string) {
	for retry := 100; retry > 0; retry-- {
		allReady := true
		keys, values := tp.GetMetrics()
		for i, key := range keys {
			if strings.HasSuffix(key, "_connection_status") {
				if values[i] != 1 && values[i] != "connected" {
					allReady = false
					break
				}
			}
		}
		if allReady && pubsub.GetPubCount(id) > 0 {
			if tp.GetCoordinator() != nil {
				if tp.GetCoordinator().IsActivated() {
					return
				}
			} else {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}
