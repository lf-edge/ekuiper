// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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

package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/processor"
	"github.com/lf-edge/ekuiper/v2/internal/trial"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func (suite *RestTestSuite) TestRuleTest() {
	dataDir, err := conf.GetDataLoc()
	require.NoError(suite.T(), err)
	require.NoError(suite.T(), store.SetupDefault(dataDir))

	rd := &trial.RunDef{
		Id:  "mock1",
		Sql: "select * from trialDemo",
		Mock: map[string]map[string]any{
			"trialDemo": {
				"data": []map[string]any{
					{
						"a": 1,
					},
				},
				"loop":     true,
				"interval": 100,
			},
		},
		SinkProps: map[string]any{
			"sendSingle": true,
		},
	}
	b, err := json.Marshal(rd)
	require.NoError(suite.T(), err)
	req2, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/ruletest", bytes.NewBuffer(b))
	w2 := httptest.NewRecorder()
	suite.r.ServeHTTP(w2, req2)
	require.NotEqual(suite.T(), http.StatusOK, w2.Code)

	p := processor.NewStreamProcessor()
	p.ExecStmt("DROP STREAM trialDemo")
	_, err = p.ExecStmt("CREATE STREAM trialDemo () WITH (DATASOURCE=\"trialDemo\", TYPE=\"simulator\", FORMAT=\"json\", KEY=\"ts\")")
	require.NoError(suite.T(), err)
	defer p.ExecStmt("DROP STREAM trialDemo")

	req2, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/ruletest", bytes.NewBuffer(b))
	w2 = httptest.NewRecorder()
	suite.r.ServeHTTP(w2, req2)
	require.Equal(suite.T(), http.StatusOK, w2.Code)
	u := url.URL{Scheme: "ws", Host: "localhost:10087", Path: "/test/mock1"}
	c1, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	assert.NoError(suite.T(), err)
	defer c1.Close()

	recvCh := make(chan []byte, 10)
	closeCh := make(chan struct{}, 10)
	go func() {
		_, data, err := c1.ReadMessage()
		require.NoError(suite.T(), err)
		recvCh <- data
	}()
	go func() {
		for {
			select {
			case <-closeCh:
				return
			default:
				timex.Add(100 * time.Millisecond)
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	time.Sleep(100 * time.Millisecond)
	req2, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/ruletest/mock1/start", bytes.NewBuffer([]byte{}))
	w2 = httptest.NewRecorder()
	suite.r.ServeHTTP(w2, req2)
	require.Equal(suite.T(), http.StatusOK, w2.Code)
	require.Equal(suite.T(), `{"a":1}`, string(<-recvCh))
	req2, _ = http.NewRequest(http.MethodDelete, "http://localhost:8080/ruletest/mock1", bytes.NewBuffer([]byte{}))
	w2 = httptest.NewRecorder()
	suite.r.ServeHTTP(w2, req2)
	require.Equal(suite.T(), http.StatusOK, w2.Code)
	closeCh <- struct{}{}
}
