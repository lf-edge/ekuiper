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

package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func init() {
	modules.RegisterConnection("mock", connection.CreateMockConnection)
}

func (suite *RestTestSuite) TestGetConnectionStatus() {
	connection.InitConnectionManager4Test()
	connJson := `
{
  "id": "conn1",
  "typ":"mock",
  "props": {
    "method": "post",
	"datasource": "/test1"
  }
}
`
	buf := bytes.NewBuffer([]byte(connJson))
	req, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/connections", buf)
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	require.Equal(suite.T(), http.StatusCreated, w.Code)

	// test invalid id
	connInvalid := `{"id": "inv/conn", "typ":"mock", "props": {"method": "post", "datasource": "/test1"}}`
	buf = bytes.NewBuffer([]byte(connInvalid))
	req, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/connections", buf)
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	require.Equal(suite.T(), http.StatusBadRequest, w.Code)

	time.Sleep(100 * time.Millisecond)
	req, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/connections?forceAll=true", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	require.Equal(suite.T(), http.StatusOK, w.Code)
	var returnVal []byte
	returnVal, _ = io.ReadAll(w.Result().Body)
	var m []map[string]interface{}
	require.NoError(suite.T(), json.Unmarshal(returnVal, &m))

	req, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/connections/conn1", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	require.Equal(suite.T(), http.StatusOK, w.Code)
	returnVal, _ = io.ReadAll(w.Result().Body)
	require.Equal(suite.T(), `{"id":"conn1","typ":"mock","props":{"datasource":"/test1","method":"post"},"isNamed":true,"status":"connected"}`, string(returnVal))
	require.Equal(suite.T(), w.Header().Get("Content-Type"), "application/json")

	connJson = `
{
  "id": "conn1",
  "typ":"mock",
  "props": {
    "method": "post",
	"datasource": "/test2"
  }
}
`
	buf = bytes.NewBuffer([]byte(connJson))
	req, _ = http.NewRequest(http.MethodPut, "http://localhost:8080/connections/conn1", buf)
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	require.Equal(suite.T(), http.StatusOK, w.Code)
	time.Sleep(100 * time.Millisecond)
	req, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/connections/conn1", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	require.Equal(suite.T(), http.StatusOK, w.Code)
	returnVal, _ = io.ReadAll(w.Result().Body)
	require.Equal(suite.T(), `{"id":"conn1","typ":"mock","props":{"datasource":"/test2","method":"post"},"isNamed":true,"status":"connected"}`, string(returnVal))
	require.Equal(suite.T(), w.Header().Get("Content-Type"), "application/json")
}

func (suite *RestTestSuite) TestEditInternalConn() {
	connection.InitConnectionManager4Test()
	// create stream
	buf := bytes.NewBuffer([]byte(`{"sql":"CREATE stream connTest() WITH (DATASOURCE=\"0\", TYPE=\"mqtt\")"}`))
	req, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/streams", buf)
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	require.Equal(suite.T(), http.StatusCreated, w.Code)

	// create rule with trigger false
	ruleJson := `{"id": "connTest","sql": "select * from connTest","actions": [{"log": {}}]}`
	buf = bytes.NewBuffer([]byte(ruleJson))
	req, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/rules", buf)
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	require.Equal(suite.T(), http.StatusCreated, w.Code)
	time.Sleep(100 * time.Millisecond)

	req, _ = http.NewRequest(http.MethodDelete, "http://localhost:8080/connections/connTest-connTest-0-mqtt-source", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	require.Equal(suite.T(), http.StatusBadRequest, w.Code)

	updateJson := `
{
  "id": "connTest-connTest-0-mqtt-source",
  "typ":"mqtt",
  "props": {
    "method": "post",
	"datasource": "/test1"
  }
}
`
	buf = bytes.NewBuffer([]byte(updateJson))
	req, _ = http.NewRequest(http.MethodPut, "http://localhost:8080/connections/connTest-connTest-0-mqtt-source", buf)
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	require.Equal(suite.T(), http.StatusBadRequest, w.Code)
}
