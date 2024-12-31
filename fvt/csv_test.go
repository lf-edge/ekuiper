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
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
)

type CsvTestSuite struct {
	suite.Suite
}

func TestCsvTestSuite(t *testing.T) {
	suite.Run(t, new(CsvTestSuite))
}

func (s *CsvTestSuite) TestDifferentFields() {
	s.Run("init rules", func() {
		conf := map[string]any{
			"interval": "2ms",
			"loop":     false,
			"data": []map[string]any{
				{
					"humidity": 20,
				},
				{
					"temperature": 30,
				},
				{
					"humidity":    40,
					"temperature": 60,
				},
				{
					"humidity": 80,
				},
			},
		}
		resp, err := client.CreateConf("sources/simulator/confKeys/test", conf)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)

		streamSql := `{"sql":"CREATE STREAM sim() WITH (TYPE=\"simulator\", CONF_KEY=\"test\", FORMAT=\"json\")"}`
		resp, err = client.CreateStream(streamSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		ruleSql := `{
  "id": "ruleSim1",
  "sql": "SELECT temperature, humidity FROM sim",
  "actions": [{
    "file": {
		"path": "test.csv",
		"format": "delimited",
        "fileType": "csv",
        "hasHeader": true,
        "sendSingle": true
    }
  }],
  "options":{
  	"sendNilField": true
	}
}`
		resp, err = client.CreateRule(ruleSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)
	})
	s.Run("compare results", func() {
		// Assert rule2 metrics
		r := TryAssert(10, ConstantInterval, func() bool {
			metrics, e := client.GetRuleStatus("ruleSim1")
			s.Require().NoError(e)
			fmt.Println(metrics)
			return metrics["status"] == "stopped"
		})
		s.Require().True(r)
		// read the file
		file, err := os.ReadFile("test.csv")
		s.Require().NoError(err)
		result1 := "humidity,temperature\n20,\n,30\n40,60\n80,"
		result2 := "temperature,humidity\n,20\n30,\n60,40\n,80"
		if !s.Equal(string(file), result1) && !s.Equal(string(file), result2) {
			s.Require().Equal(result2, string(file))
		}
	})
	s.Run("clean up", func() {
		res, e := client.Delete("rules/ruleSim1")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)

		res, e = client.Delete("streams/sim")
		s.NoError(e)
		s.Equal(http.StatusOK, res.StatusCode)
	})
}
