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
