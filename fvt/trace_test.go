// Copyright 2024 EMQ Technologies Co., Ltd.
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

package fvt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type TraceTestSuite struct {
	suite.Suite
}

func TestTraceTestSuite(t *testing.T) {
	suite.Run(t, new(TraceTestSuite))
}

// Cover multiple sink, memory, batch, window
func (s *TraceTestSuite) TestComplexTrace() {
	s.Run("init rule1", func() {
		streamSql := `{"sql": "create stream pushStream() WITH (TYPE=\"httppush\", DATASOURCE=\"/test/sim\", FORMAT=\"json\", SHARED=\"true\")"}`
		resp, err := client.CreateStream(streamSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		ruleSql := `{
  "id": "rule1",
  "name": "http push to multiple sinks including memory",
  "sql": "SELECT a + b as c FROM pushStream",
  "actions": [
    {
      "log": {
        "format": "delimited",
        "sendSingle": false,
        "batchSize": 2
      },
      "memory": {
        "topic": "fvt/mem1",
        "sendSingle": true
      }
    }
  ],
  "options": {
    "sendError": false
  }
}`
		resp, err = client.CreateRule(ruleSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)
	})
	s.Run("init rule2", func() {
		streamSql := `{"sql": "create stream memStream() WITH (TYPE=\"memory\", DATASOURCE=\"fvt/mem1\", FORMAT=\"json\")"}`
		resp, err := client.CreateStream(streamSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		ruleSql := `{
  "id": "rule2",
  "name": "use window from memory source with cache",
  "sql": "SELECT count(*) FROM memStream GROUP BY SlidingWindow(ms, 100)",
  "actions": [
    {
      "log": {
        "sendSingle": true,
        "enableCache": true
      }
    }
  ],
  "options": {
    "sendError": false
  }
}`
		resp, err = client.CreateRule(ruleSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)
	})
	time.Sleep(ConstantInterval)
	s.Run("enable trace", func() {
		resp, err := client.Post("rules/rule1/trace/start", `{"strategy": "always"}`)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)

		resp, err = client.Post("rules/rule2/trace/start", `{"strategy": "always"}`)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
	})
	time.Sleep(ConstantInterval)
	s.Run("send data by http", func() {
		resp, err := http.Post("http://127.0.0.1:10081/test/sim", ContentTypeJson, bytes.NewBufferString("{\"a\": 12,\"b\": 21}"))
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
		resp, err = http.Post("http://127.0.0.1:10081/test/sim", ContentTypeJson, bytes.NewBufferString("{\"a\": 22,\"b\": 41}"))
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
		time.Sleep(500 * time.Millisecond)
		resp, err = http.Post("http://127.0.0.1:10081/test/sim", ContentTypeJson, bytes.NewBufferString("{\"a\": 32,\"b\": 61}"))
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
	})
	s.Run("assert rule1 trace", func() {
		var rule1Ids []string
		// Assert rule1 traces
		r := TryAssert(10, time.Second, func() bool {
			resp, e := client.Get("trace/rule/rule1")
			s.Require().NoError(e)
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			s.Require().NoError(err)
			err = json.Unmarshal(body, &rule1Ids)
			s.Require().NoError(err)
			return len(rule1Ids) == 4
		})
		fmt.Println(len(rule1Ids))
		s.Require().True(r)
		// assert each trace, just check 1/2/3
		for i := 1; i < 4; i++ {
			tid := rule1Ids[i]
			resp, e := client.Get(path.Join("trace", tid))
			s.NoError(e)
			s.Equal(http.StatusOK, resp.StatusCode)
			act, resultMap, err := GetResponseResultTextAndMap(resp)
			s.NoError(err)
			all, err := os.ReadFile(filepath.Join("result", "trace", fmt.Sprintf("complex%d.json", i)))
			s.NoError(err)
			exps := make([]map[string]any, 0)
			err = json.Unmarshal(all, &exps)
			s.NoError(err)
			find := false
			for _, exp := range exps {
				if s.compareTrace(exp, resultMap) {
					find = true
					break
				}
			}
			if !find {
				fmt.Println(fmt.Sprintf("complex%d.json", i))
				fmt.Println(string(act))
				s.Fail(fmt.Sprintf("trace 1 file %d compares fail", i))
			}
		}
	})
	s.Run("assert rule2 trace", func() {
		var (
			rule2Ids []string
			checkMap = map[int]int{
				1: 2,
				2: 3,
				4: 4,
				5: 5,
			}
		)
		// Assert rule2 traces
		r := TryAssert(10, 100*time.Millisecond, func() bool {
			resp, e := client.Get("trace/rule/rule2")
			s.Require().NoError(e)
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			s.Require().NoError(err)
			err = json.Unmarshal(body, &rule2Ids)
			s.Require().NoError(err)
			return len(rule2Ids) == 6
		})
		s.Require().True(r)
		for i, tid := range rule2Ids {
			eid, ok := checkMap[i]
			if !ok {
				continue
			}
			resp, e := client.Get(path.Join("trace", tid))
			s.NoError(e)
			s.Equal(http.StatusOK, resp.StatusCode)
			act, resultMap, err := GetResponseResultTextAndMap(resp)
			s.NoError(err)

			all, err := os.ReadFile(filepath.Join("result", "trace", fmt.Sprintf("complex%d.json", eid)))
			s.NoError(err)
			exps := make([]map[string]any, 0)
			err = json.Unmarshal(all, &exps)
			s.NoError(err)
			find := false
			for _, exp := range exps {
				if s.compareTrace(exp, resultMap) {
					find = true
					break
				}
			}
			if !find {
				fmt.Println(fmt.Sprintf("complex%d.json", eid))
				fmt.Println(string(act))
				s.Fail(fmt.Sprintf("trace 2 file %d compares fail", eid))
			}
		}
	})
	s.Run("clean", func() {
		res, e := client.Delete("rules/rule2")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)

		res, e = client.Delete("rules/rule1")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)

		res, e = client.Delete("streams/memStream")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)

		res, e = client.Delete("streams/pushStream")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)
	})
}

// Cover ratelimit, lookup table
func (s *TraceTestSuite) TestLookup() {
	s.Run("init mem table", func() {
		streamSql := `{"sql":"CREATE TABLE memTable() WITH (DATASOURCE=\"memtable\", TYPE=\"memory\", KIND=\"lookup\", KEY=\"id\")"}`
		resp, err := client.CreateStream(streamSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		streamSql = `{"sql":"CREATE STREAM permanent() WITH (TYPE=\"httppush\", DATASOURCE=\"/test/table\", FORMAT=\"json\")"}`
		resp, err = client.CreateStream(streamSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		ruleSql := `{
  "id": "ruleMemTable",
  "sql": "SELECT * FROM permanent ",
  "actions": [{
    "memory": {
      "topic": "memtable",
      "rowkindField": "action",
      "keyField": "id",
      "sendSingle": true
    }
  }]
}`
		resp, err = client.CreateRule(ruleSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)
	})
	time.Sleep(ConstantInterval)
	s.Run("send data to table", func() {
		resp, err := http.Post("http://127.0.0.1:10081/test/table", ContentTypeJson, bytes.NewBufferString("{\"action\":\"upsert\",\"id\":1,\"name\":\"John\",\"address\":34,\"mobile\":\"334433\"}"))
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)

		resp, err = http.Post("http://127.0.0.1:10081/test/table", ContentTypeJson, bytes.NewBufferString("{\"action\":\"upsert\",\"id\":2,\"name\":\"Jon\",\"address\":54,\"mobile\":\"534433\"}"))
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
	})

	s.Run("init rate limit and lookup rule", func() {
		conf := map[string]any{
			"interval": "100ms",
		}
		resp, err := client.CreateConf("sources/httppush/confKeys/onesec", conf)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)

		streamSql := `{"sql":"CREATE STREAM pushStream2() WITH (TYPE=\"httppush\", DATASOURCE=\"/test/push2\", CONF_KEY=\"onesec\", FORMAT=\"json\")"}`
		resp, err = client.CreateStream(streamSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		ruleSql := `{
  "id": "ruleLookupMem1",
  "sql": "SELECT * FROM pushStream2 INNER JOIN memTable ON pushStream2.id = memTable.id",
  "actions": [{
    "log": {
    }
  }]
}`
		resp, err = client.CreateRule(ruleSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)
	})
	s.Run("enable trace", func() {
		resp, err := client.Post("rules/ruleLookupMem1/trace/start", `{"strategy": "always"}`)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
	})
	time.Sleep(ConstantInterval)
	s.Run("send data to rule", func() {
		resp, err := http.Post("http://127.0.0.1:10081/test/push2", ContentTypeJson, bytes.NewBufferString("{\"id\":1}"))
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)

		resp, err = http.Post("http://127.0.0.1:10081/test/push2", ContentTypeJson, bytes.NewBufferString("{\"id\":2}"))
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
	})
	s.Run("assert trace", func() {
		var ruleIds []string
		// Assert rule1 traces
		r := TryAssert(10, time.Second, func() bool {
			resp, e := client.Get("trace/rule/ruleLookupMem1")
			s.Require().NoError(e)
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			s.Require().NoError(err)
			err = json.Unmarshal(body, &ruleIds)
			s.Require().NoError(err)
			return len(ruleIds) == 2
		})
		s.Require().True(r)
		// assert each trace, just check 1/2/3
		for i, tid := range ruleIds {
			resp, e := client.Get(path.Join("trace", tid))
			s.NoError(e)
			s.Equal(http.StatusOK, resp.StatusCode)
			act, resultMap, err := GetResponseResultTextAndMap(resp)
			s.NoError(err)
			all, err := os.ReadFile(filepath.Join("result", "trace", fmt.Sprintf("lookup%d.json", i+1)))
			s.NoError(err)
			exp := make(map[string]any)
			err = json.Unmarshal(all, &exp)
			s.NoError(err)
			if s.compareTrace(exp, resultMap) == false {
				fmt.Println(fmt.Sprintf("lookup%d.json", i+1))
				fmt.Println(string(act))
				s.Fail(fmt.Sprintf("trace lookup %d compares fail", i+1))
			}
		}
	})
	s.Run("clean", func() {
		res, e := client.Delete("rules/ruleLookupMem1")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)

		res, e = client.Delete("rules/ruleMemTable")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)

		res, e = client.Delete("streams/pushStream2")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)

		res, e = client.Delete("streams/permanent")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)

		res, e = client.Delete("tables/memTable")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)
	})
}

// Cover eventtime, watermark, rest sink (with error)
func (s *TraceTestSuite) TestEventTime() {
	s.Run("init rule3", func() {
		streamSql := `{"sql":"CREATE STREAM pushStream3() WITH (TYPE=\"httppush\", DATASOURCE=\"/test/push3\", CONF_KEY=\"onesec\", FORMAT=\"json\", TIMESTAMP=\"ts\")"}`
		resp, err := client.CreateStream(streamSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		ruleSql := `{
  "id": "rule3",
  "name": "use event time window",
  "sql": "SELECT count(*) FROM pushStream3 GROUP BY TumblingWindow(ss, 1)",
  "actions": [
    {
      "rest": {
        "url": "https://www.githubstatus.com/test",
        "sendSingle": true
      }
    }
  ],
  "options": {
    "sendError": false,
    "isEventTime": true,
    "lateTolerance" : 0
  }
}`
		resp, err = client.CreateRule(ruleSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)
	})
	s.Run("enable trace", func() {
		resp, err := client.Post("rules/rule3/trace/start", `{"strategy": "always"}`)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
	})
	time.Sleep(ConstantInterval)
	s.Run("send data to test", func() {
		resp, err := http.Post("http://127.0.0.1:10081/test/push3", ContentTypeJson, bytes.NewBufferString("{\"id\":1, \"ts\": 1111}"))
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)

		resp, err = http.Post("http://127.0.0.1:10081/test/push3", ContentTypeJson, bytes.NewBufferString("{\"id\":1, \"ts\": 1901}"))
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)

		resp, err = http.Post("http://127.0.0.1:10081/test/push3", ContentTypeJson, bytes.NewBufferString("{\"id\":3, \"ts\": 2431}"))
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
	})
	s.Run("assert trace", func() {
		var (
			ruleIds  []string
			checkMap = map[int]int{
				0: 1,
				2: 2,
				3: 3,
			}
		)
		// Assert rule1 traces
		r := TryAssert(10, time.Second, func() bool {
			resp, e := client.Get("trace/rule/rule3")
			s.Require().NoError(e)
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			s.Require().NoError(err)
			err = json.Unmarshal(body, &ruleIds)
			s.Require().NoError(err)
			return len(ruleIds) == 5
		})
		s.Require().True(r)
		// assert each trace, just check 0/1/2
		for i, cid := range checkMap {
			tid := ruleIds[i]
			resp, e := client.Get(path.Join("trace", tid))
			s.NoError(e)
			s.Equal(http.StatusOK, resp.StatusCode)
			act, resultMap, err := GetResponseResultTextAndMap(resp)
			s.NoError(err)
			all, err := os.ReadFile(filepath.Join("result", "trace", fmt.Sprintf("event%d.json", cid)))
			s.NoError(err)
			exp := make(map[string]any)
			err = json.Unmarshal(all, &exp)
			s.NoError(err)
			if s.compareTrace(exp, resultMap) == false {
				fmt.Println(fmt.Sprintf("event%d.json", cid))
				fmt.Println(string(act))
				s.Fail(fmt.Sprintf("trace event %d compares fail", cid))
			}
		}
	})
	s.Run("clean", func() {
		res, e := client.Delete("rules/rule3")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)

		res, e = client.Delete("streams/pushStream3")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)
	})
}

func (s *TraceTestSuite) compareTrace(exp map[string]any, act map[string]any) bool {
	if len(exp) != len(act) {
		fmt.Println("map length different")
		return false
	}
	for k, v := range act {
		switch k {
		case "name", "attribute":
			if reflect.DeepEqual(exp[k], v) == false {
				fmt.Printf("compare %s, expect %s, actual %s\n", k, exp[k], v)
				return false
			}
		case "ChildSpan":
			ex, ok := exp[k]
			if !ok {
				fmt.Println("missing child span")
				return false
			}
			exs, ok := ex.([]any)
			if !ok {
				fmt.Println("exp child span not slice")
				return false
			}
			vs, ok := v.([]any)
			if !ok {
				fmt.Println("act child span not slice")
				return false
			}
			if len(vs) != len(exs) {
				fmt.Println("child span not equal")
				return false
			}
			childsMap := make(map[string]map[string]any)
			for _, vss := range vs {
				vm, ok := vss.(map[string]any)
				if !ok {
					fmt.Println("act child span not map")
					return false
				}
				name, ok := vm["name"]
				if !ok {
					fmt.Println("act child span does not have name")
					return false
				}
				childsMap[name.(string)] = vm
			}
			for _, exss := range exs {
				exm, ok := exss.(map[string]any)
				if !ok {
					fmt.Println("exp child span not map")
					return false
				}
				name, ok := exm["name"]
				if !ok {
					fmt.Println("exp child span does not have name")
					return false
				}
				vm, ok := childsMap[name.(string)]
				if !ok {
					fmt.Printf("act child span does not have %s", name)
					return false
				}
				r := s.compareTrace(exm, vm)
				if !r {
					return r
				}
			}
		case "links":
			ec, ok := exp[k]
			if !ok {
				fmt.Println("exp missing links")
				return false
			}
			ecl, ok := ec.([]any)
			if !ok {
				fmt.Println("exp links not slice")
				return false
			}
			vl, ok := v.([]any)
			if !ok {
				fmt.Println("act links not slice")
				return false
			}
			if len(ecl) != len(vl) {
				fmt.Println("links count not equal")
				return false
			}
		default:
			_, ok := exp[k]
			if !ok {
				fmt.Printf("exp missing key %s\n", k)
				return false
			}
		}
	}
	return true
}
