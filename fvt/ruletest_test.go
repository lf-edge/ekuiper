// Copyright 2026 EMQ Technologies Co., Ltd.
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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type RuletestTestSuite struct {
	suite.Suite
}

func TestRuletestSuite(t *testing.T) {
	suite.Run(t, new(RuletestTestSuite))
}

func (s *RuletestTestSuite) TestRuletestMockSourceUnnestKeepProjectedFields() {
	streamName := "demoRuletest5501"
	ruleID := "rule_ruletest_5501"

	_, _ = client.DeleteStream(streamName)
	_, _ = client.Delete(fmt.Sprintf("ruletest/%s", ruleID))

	s.T().Cleanup(func() {
		_, _ = client.Delete(fmt.Sprintf("ruletest/%s", ruleID))
		_, _ = client.DeleteStream(streamName)
	})

	streamSQL := fmt.Sprintf(`{"sql":"CREATE STREAM %s (id STRING, time STRING, type STRING, data ARRAY(STRUCT(k BIGINT))) WITH (DATASOURCE=\"%s\", FORMAT=\"json\", TYPE=\"mqtt\")"}`, streamName, streamName)
	resp, err := client.CreateStream(streamSQL)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, resp.StatusCode)

	ruleDef := fmt.Sprintf(`{
  "id": "%s",
  "sql": "SELECT id, time, type, unnest(data) FROM %s",
  "mockSource": {
    "%s": {
      "loop": false,
      "data": [
        {
          "id": "id1",
          "time": "2023-05-30T15:23:23.123+08:00",
          "type": "1",
          "data": [
            {"k": 1},
            {"k": 2}
          ]
        }
      ]
    }
  },
  "sinkProps": {
    "sendSingle": true
  }
}`, ruleID, streamName, streamName)

	resp, err = client.Post("ruletest", ruleDef)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode)

	result, err := GetResponseResultMap(resp)
	s.Require().NoError(err)
	s.Require().Equal(ruleID, result["id"])
	port, ok := result["port"].(float64)
	s.Require().True(ok)

	sseURL := fmt.Sprintf("http://127.0.0.1:%d/test/%s", int(port), ruleID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sseURL, nil)
	s.Require().NoError(err)
	req.Header.Set("Accept", "text/event-stream")
	sseResp, err := http.DefaultClient.Do(req)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, sseResp.StatusCode)
	defer sseResp.Body.Close()

	// Start rule after SSE connected to avoid missing data.
	resp, err = client.Post(fmt.Sprintf("ruletest/%s/start", ruleID), "any")
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode)

	scanner := bufio.NewScanner(sseResp.Body)
	var got map[string]any
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := strings.TrimPrefix(line, "data: ")
		s.T().Log(payload)
		s.Require().NoError(json.Unmarshal([]byte(payload), &got))
		break
	}
	s.Require().NoError(scanner.Err())
	s.Require().NotEmpty(got)

	// Regression guard: in mockSource+unnest ruletest, projected fields should not be dropped.
	s.Require().Equal("id1", got["id"])
	s.Require().Equal("2023-05-30T15:23:23.123+08:00", got["time"])
	s.Require().Equal("1", got["type"])
	_, hasK := got["k"]
	s.Require().True(hasK)
}

func (s *RuletestTestSuite) TestAccCollectChargeCycle() {
	streamName := "test_stream"
	ruleID := "rule_acc_collect_charge"

	_, _ = client.DeleteStream(streamName)
	_, _ = client.Delete(fmt.Sprintf("ruletest/%s", ruleID))

	s.T().Cleanup(func() {
		_, _ = client.Delete(fmt.Sprintf("ruletest/%s", ruleID))
		_, _ = client.DeleteStream(streamName)
	})

	streamSQL := fmt.Sprintf(`{"sql":"CREATE STREAM %s (field_a STRING, field_b STRING, metric BIGINT, et BIGINT) WITH (DATASOURCE=\"%s\", FORMAT=\"json\", TYPE=\"mqtt\")"}`, streamName, streamName)
	resp, err := client.CreateStream(streamSQL)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, resp.StatusCode)

	ruleDef := fmt.Sprintf(`{
  "id": "%s",
  "sql": "SELECT CASE WHEN field_a = 'val_on' AND lag(field_a) != 'val_on' THEN et END AS flag_a, CASE WHEN field_b = 'val_x' AND lag(field_b) != 'val_x' THEN et END AS flag_b, CASE WHEN field_b != 'val_x' AND lag(field_b) = 'val_x' THEN et END AS flag_c, CASE WHEN mod(metric,10) = 0 AND mod(lag(metric),10) != 0 THEN OBJECT_CONSTRUCT('metric', metric, 'et', et) END AS trigger_obj, acc_collect(trigger_obj, latest(flag_b) > 0, flag_b > 0) AS acc_collected FROM %s WHERE flag_c > 0",
  "mockSource": {
    "%s": {
      "loop": false,
      "interval": "10ms",
      "data": [
        {"field_a":"val_off","field_b":"val_y","metric":49,"et":1754100100},
        {"field_a":"val_on","field_b":"val_x","metric":49,"et":1754100100},
        {"field_a":"val_on","field_b":"val_x","metric":50,"et":1754100101},
        {"field_a":"val_on","field_b":"val_x","metric":51,"et":1754100102},
        {"field_a":"val_on","field_b":"val_x","metric":55,"et":1754100103},
        {"field_a":"val_on","field_b":"val_x","metric":59,"et":1754100104},
        {"field_a":"val_on","field_b":"val_x","metric":60,"et":1754100105},
        {"field_a":"val_on","field_b":"val_x","metric":61,"et":1754100106},
        {"field_a":"val_on","field_b":"val_y","metric":60,"et":1754100107},
        {"field_a":"val_on","field_b":"val_y","metric":40,"et":1754100108},
        {"field_a":"val_on","field_b":"val_y","metric":20,"et":1754100109},
        {"field_a":"val_on","field_b":"val_x","metric":25,"et":1754100110},
        {"field_a":"val_on","field_b":"val_x","metric":29,"et":1754100111},
        {"field_a":"val_on","field_b":"val_x","metric":30,"et":1754100112},
        {"field_a":"val_on","field_b":"val_x","metric":31,"et":1754100113},
        {"field_a":"val_on","field_b":"val_x","metric":35,"et":1754100114},
        {"field_a":"val_on","field_b":"val_x","metric":39,"et":1754100115},
        {"field_a":"val_on","field_b":"val_x","metric":40,"et":1754100116},
        {"field_a":"val_on","field_b":"val_x","metric":41,"et":1754100117},
        {"field_a":"val_off","field_b":"val_y","metric":41,"et":1754100118}
      ]
    }
  },
  "sinkProps": {
    "sendSingle": true
  }
}`, ruleID, streamName, streamName)

	resp, err = client.Post("ruletest", ruleDef)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode)

	result, err := GetResponseResultMap(resp)
	s.Require().NoError(err)
	s.Require().Equal(ruleID, result["id"])
	port, ok := result["port"].(float64)
	s.Require().True(ok)

	sseURL := fmt.Sprintf("http://127.0.0.1:%d/test/%s", int(port), ruleID)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sseURL, nil)
	s.Require().NoError(err)
	req.Header.Set("Accept", "text/event-stream")
	sseResp, err := http.DefaultClient.Do(req)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, sseResp.StatusCode)
	defer sseResp.Body.Close()

	resp, err = client.Post(fmt.Sprintf("ruletest/%s/start", ruleID), "any")
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode)

	scanner := bufio.NewScanner(sseResp.Body)
	var results []map[string]any
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := strings.TrimPrefix(line, "data: ")
		s.T().Logf("SSE output: %s", payload)
		var got map[string]any
		s.Require().NoError(json.Unmarshal([]byte(payload), &got))
		results = append(results, got)
	}
	// scanner exits when SSE stream is closed (context deadline / no more data); this is expected
	if scanner.Err() != nil {
		s.T().Logf("scanner stopped (expected after data consumed): %v", scanner.Err())
	}

	// For now, log results since we're observing actual output
	s.T().Logf("Total results: %d", len(results))
	for i, r := range results {
		formatted, _ := json.MarshalIndent(r, "", "  ")
		s.T().Logf("Result[%d]: %s", i, string(formatted))
	}

	// Basic sanity checks
	s.Require().NotEmpty(results, "should have at least one output (cycle end)")
	for _, r := range results {
		s.Require().NotEmpty(r["acc_collected"], "acc_collected should not be empty")
	}

	// Assert concrete expected output
	s.Require().Len(results, 2, "should have exactly 2 charge cycle outputs")

	// Cycle 1: charge ends at ts=1754100107
	s.Require().Equal(float64(1754100107), results[0]["flag_c"])
	expectedCollected1 := []interface{}{
		map[string]interface{}{"metric": float64(50), "et": float64(1754100101)},
		map[string]interface{}{"metric": float64(60), "et": float64(1754100105)},
		map[string]interface{}{"metric": float64(60), "et": float64(1754100107)},
	}
	s.Require().Equal(expectedCollected1, results[0]["acc_collected"])

	// Cycle 2: charge ends at ts=1754100118
	s.Require().Equal(float64(1754100118), results[1]["flag_c"])
	expectedCollected2 := []interface{}{
		map[string]interface{}{"metric": float64(30), "et": float64(1754100112)},
		map[string]interface{}{"metric": float64(40), "et": float64(1754100116)},
	}
	s.Require().Equal(expectedCollected2, results[1]["acc_collected"])
}
