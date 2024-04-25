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
	"net/http"
	"net/http/httptest"

	"github.com/stretchr/testify/require"
)

func (suite *RestTestSuite) TestSinkPasswordPing() {
	buf1 := bytes.NewBuffer([]byte(`{"sql":"CREATE stream demo98() WITH (DATASOURCE=\"0\", TYPE=\"mqtt\")"}`))
	req1, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/streams", buf1)
	w1 := httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)

	ruleJson2 := `{"triggered":false,"id":"rule321","sql":"select * from demo98;","actions":[{"mqtt":{"server":"tcp://broker.emqx.io:1883","topic":"devices/demo_001/messages/events/","qos":0,"clientId":"demo_001","username":"xyz.azure-devices.net/demo_001/?api-version=2018-06-30","password":"12345"}}]}`
	buf2 := bytes.NewBuffer([]byte(ruleJson2))
	req2, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/rules", buf2)
	w2 := httptest.NewRecorder()
	suite.r.ServeHTTP(w2, req2)
	require.Equal(suite.T(), http.StatusCreated, w2.Code)

	c := `{"server":"tcp://broker.emqx.io:1883","topic":"devices/demo_001/messages/events/","qos":0,"clientId":"demo_001","username":"xyz.azure-devices.net/demo_001/?api-version=2018-06-30","password":"******"}`
	m := make(map[string]interface{})
	require.NoError(suite.T(), json.Unmarshal([]byte(c), &m))
	r := replacePasswdByRuleID("rule321", 0, "mqtt", m)
	require.Equal(suite.T(), "12345", r["password"])
	m["password"] = "12345"
	require.Equal(suite.T(), m, r)
}
