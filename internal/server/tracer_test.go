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
)

func (suite *RestTestSuite) TestTraceRule() {
	buf1 := bytes.NewBuffer([]byte(`{"sql":"CREATE stream demo4321() WITH (DATASOURCE=\"0\", TYPE=\"mqtt\")"}`))
	req1, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/streams", buf1)
	w1 := httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)

	ruleJson2 := `{"id":"test54321","triggered":true,"sql":"select * from demo4321","actions":[{"log":{}}]}`
	buf2 := bytes.NewBuffer([]byte(ruleJson2))
	req2, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/rules", buf2)
	w2 := httptest.NewRecorder()
	suite.r.ServeHTTP(w2, req2)
	require.Equal(suite.T(), http.StatusCreated, w2.Code)

	req2, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/rules/test54321/trace/start", bytes.NewBufferString("any"))
	w2 = httptest.NewRecorder()
	suite.r.ServeHTTP(w2, req2)
	require.Equal(suite.T(), http.StatusOK, w2.Code)
	time.Sleep(10 * time.Millisecond)

	req2, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/rules/test54321/trace/stop", bytes.NewBufferString("any"))
	w2 = httptest.NewRecorder()
	suite.r.ServeHTTP(w2, req2)
	require.Equal(suite.T(), http.StatusOK, w2.Code)
	time.Sleep(10 * time.Millisecond)
	req2, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/rules", bytes.NewBufferString("any"))
	w2 = httptest.NewRecorder()
	suite.r.ServeHTTP(w2, req2)
	require.Equal(suite.T(), http.StatusOK, w2.Code)
	var returnVal []byte
	returnVal, _ = io.ReadAll(w2.Result().Body)
	v := make([]map[string]interface{}, 0)
	require.NoError(suite.T(), json.Unmarshal(returnVal, &v))
	for _, vv := range v {
		if vv["id"] == "test54321" {
			require.Equal(suite.T(), "running", v[0]["status"])
			require.Equal(suite.T(), false, v[0]["trace"])
		}
	}
}
