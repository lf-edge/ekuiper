// Copyright 2025 EMQ Technologies Co., Ltd.
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
	"net/http"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
)

type RuleTestSuite struct {
	suite.Suite
}

func TestRuleSuite(t *testing.T) {
	suite.Run(t, new(RuleTestSuite))
}

func (s *RuleTestSuite) TestRuleDisableBufferFullDiscard() {
	topic := "test1"
	subCh := pubsub.CreateSub(topic, nil, topic, 1024)
	defer pubsub.CloseSourceConsumerChannel(topic, topic)
	data := []map[string]any{
		{
			"a": float64(1),
		},
		{
			"a": float64(2),
		},
		{
			"a": float64(3),
		},
		{
			"a": float64(4),
		},
		{
			"a": float64(5),
		},
		{
			"a": float64(6),
		},
	}
	conf := map[string]any{
		"data":     data,
		"interval": "1ms",
		"loop":     false,
	}
	resp, err := client.CreateConf("sources/simulator/confKeys/sim1", conf)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode)
	streamSql := `{"sql": "create stream sim1() WITH (TYPE=\"simulator\", CONF_KEY=\"sim1\")"}`
	resp, err = client.CreateStream(streamSql)
	s.Require().NoError(err)
	s.T().Log(GetResponseText(resp))
	s.Require().Equal(http.StatusCreated, resp.StatusCode)
	ruleSql := `{
  "id": "ruleSim1",
  "sql": "SELECT * FROM sim1",
  "actions": [
    {
      "memory":{
        "topic": "test1",
        "bufferLength": 1
      }
    }
  ],
  "options": {
    "disableBufferFullDiscard": true,
    "bufferLength": 1
  }
}`
	resp, err = client.CreateRule(ruleSql)
	s.Require().NoError(err)
	s.T().Log(GetResponseText(resp))
	s.Require().Equal(http.StatusCreated, resp.StatusCode)
	s.assertRecvMemTuple(subCh, data)
}

func (s *RuleTestSuite) assertRecvMemTuple(subCh chan any, expect []map[string]any) {
	for _, e := range expect {
		d := <-subCh
		mt, ok := d.([]pubsub.MemTuple)
		s.Require().True(ok)
		s.Require().Len(mt, 1)
		s.Require().Equal(e, mt[0].ToMap())
	}
}
