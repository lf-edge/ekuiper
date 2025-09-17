// Copyright 2023-2024 EMQ Technologies Co., Ltd.
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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/io/http/httpserver"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/processor"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/internal/topo/rule"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
)

func init() {
	testx.InitEnv("server")
	streamProcessor = processor.NewStreamProcessor()
	ruleProcessor = processor.NewRuleProcessor()
	rulesetProcessor = processor.NewRulesetProcessor(ruleProcessor, streamProcessor)
	registry = &RuleRegistry{internal: make(map[string]*rule.State)}
	uploadsDb, _ = store.GetKV("uploads")
	uploadsStatusDb, _ = store.GetKV("uploadsStatusDb")
	sysMetrics = NewMetrics()
}

type RestTestSuite struct {
	suite.Suite
	r *mux.Router
}

func (suite *RestTestSuite) SetupTest() {
	dataDir, err := conf.GetDataLoc()
	if err != nil {
		panic(err)
	}
	uploadDir = filepath.Join(dataDir, "uploads")
	if _, err = os.Stat(uploadDir); os.IsNotExist(err) {
		os.MkdirAll(uploadDir, 0o755)
	}

	r := mux.NewRouter()
	r.HandleFunc("/", rootHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/ping", pingHandler).Methods(http.MethodGet)
	r.HandleFunc("/streams", streamsHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/streamdetails", streamDetailsHandler).Methods(http.MethodGet)
	r.HandleFunc("/streams/{name}", streamHandler).Methods(http.MethodGet, http.MethodDelete, http.MethodPut)
	r.HandleFunc("/streams/{name}/schema", streamSchemaHandler).Methods(http.MethodGet)
	r.HandleFunc("/tables", tablesHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/tabledetails", tableDetailsHandler).Methods(http.MethodGet)
	r.HandleFunc("/tables/{name}", tableHandler).Methods(http.MethodGet, http.MethodDelete, http.MethodPut)
	r.HandleFunc("/tables/{name}/schema", tableSchemaHandler).Methods(http.MethodGet)
	r.HandleFunc("/rules", rulesHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/rules/{name}", ruleHandler).Methods(http.MethodDelete, http.MethodGet, http.MethodPut)
	r.HandleFunc("/rules/{name}/status", getStatusRuleHandler).Methods(http.MethodGet)
	r.HandleFunc("/v2/rules/{name}/status", getStatusV2RulHandler).Methods(http.MethodGet)
	r.HandleFunc("/rules/{name}/start", startRuleHandler).Methods(http.MethodPost)
	r.HandleFunc("/rules/{name}/stop", stopRuleHandler).Methods(http.MethodPost)
	r.HandleFunc("/rules/{name}/restart", restartRuleHandler).Methods(http.MethodPost)
	r.HandleFunc("/rules/{name}/topo", getTopoRuleHandler).Methods(http.MethodGet)
	r.HandleFunc("/rules/{name}/reset_state", ruleStateHandler).Methods(http.MethodPut)
	r.HandleFunc("/rules/{name}/explain", explainRuleHandler).Methods(http.MethodGet)
	r.HandleFunc("/rules/{name}/trace/start", enableRuleTraceHandler).Methods(http.MethodPost)
	r.HandleFunc("/rules/{name}/trace/stop", disableRuleTraceHandler).Methods(http.MethodPost)
	r.HandleFunc("/rules/validate", validateRuleHandler).Methods(http.MethodPost)
	r.HandleFunc("/rules/status/all", getAllRuleStatusHandler).Methods(http.MethodGet)
	r.HandleFunc("/ruleset/export", exportHandler).Methods(http.MethodPost)
	r.HandleFunc("/ruleset/import", importHandler).Methods(http.MethodPost)
	r.HandleFunc("/configs", configurationUpdateHandler).Methods(http.MethodPatch)
	r.HandleFunc("/config/uploads", fileUploadHandler).Methods(http.MethodPost, http.MethodGet)
	r.HandleFunc("/config/uploads/{name}", fileDeleteHandler).Methods(http.MethodDelete)
	r.HandleFunc("/data/export", configurationExportHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/data/import", configurationImportHandler).Methods(http.MethodPost)
	r.HandleFunc("/data/import/status", configurationStatusHandler).Methods(http.MethodGet)
	r.HandleFunc("/connections", connectionsHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/connections/{id}", connectionHandler).Methods(http.MethodGet, http.MethodDelete, http.MethodPut)
	r.HandleFunc("/ruletest", testRuleHandler).Methods(http.MethodPost)
	r.HandleFunc("/ruletest/{name}/start", testRuleStartHandler).Methods(http.MethodPost)
	r.HandleFunc("/ruletest/{name}", testRuleStopHandler).Methods(http.MethodDelete)
	// r.HandleFunc("/connection/websocket", connectionHandler).Methods(http.MethodGet, http.MethodPost, http.MethodDelete)
	r.HandleFunc("/metadata/sinks/{name}/confKeys/{confKey}", sinkConfKeyHandler).Methods(http.MethodDelete, http.MethodPut)
	suite.r = r
}

func (suite *RestTestSuite) Test_rootHandler() {
	req, _ := http.NewRequest(http.MethodPost, "/", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
}

func (suite *RestTestSuite) Test_sourcesManageHandler() {
	req, _ := http.NewRequest(http.MethodGet, "/", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)

	// get scan table
	req, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/streams?kind=scan", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)

	// get lookup table
	req, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/streams?kind=lookup", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)

	// create table
	buf := bytes.NewBuffer([]byte(` {"sql":"CREATE TABLE alertTable() WITH (DATASOURCE=\"0\", TYPE=\"memory\", KEY=\"id\", KIND=\"lookup\")"}`))
	req, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/streams?kind=lookup", buf)
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusCreated, w.Code)
	var returnVal []byte
	returnVal, _ = io.ReadAll(w.Result().Body)
	fmt.Printf("returnVal %s\n", string(returnVal))

	// create stream
	buf = bytes.NewBuffer([]byte(`{"sql":"CREATE stream alert() WITH (DATASOURCE=\"0\", TYPE=\"mqtt\")"}`))
	req, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/streams", buf)
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusCreated, w.Code)
	returnVal, _ = io.ReadAll(w.Result().Body)
	fmt.Printf("returnVal %s\n", string(returnVal))

	// get stream
	req, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/streams/alert", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
	expect := []byte(`{"Name":"alert","Options":{"datasource":"0","type":"mqtt"},"Statement":null,"StreamFields":null,"StreamType":0}`)
	exp := map[string]interface{}{}
	_ = json.NewDecoder(bytes.NewBuffer(expect)).Decode(&exp)

	res := map[string]interface{}{}
	_ = json.NewDecoder(w.Result().Body).Decode(&res)
	assert.Equal(suite.T(), exp, res)
	req, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/streams/alert/schema", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)

	// get table
	req, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/tables/alertTable", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
	expect = []byte(`{"Name":"alertTable","Options":{"datasource":"0","type":"memory", "key":"id","kind":"lookup"},"Statement":null,"StreamFields":null,"StreamType":1}`)
	exp = map[string]interface{}{}
	_ = json.NewDecoder(bytes.NewBuffer(expect)).Decode(&exp)
	res = map[string]interface{}{}
	_ = json.NewDecoder(w.Result().Body).Decode(&res)
	assert.Equal(suite.T(), exp, res)

	req, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/tables/alertTable/schema", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)

	// put table
	buf = bytes.NewBuffer([]byte(` {"sql":"CREATE TABLE alertTable() WITH (DATASOURCE=\"0\", TYPE=\"memory\", KEY=\"id\", KIND=\"lookup\")"}`))
	req, _ = http.NewRequest(http.MethodPut, "http://localhost:8080/tables/alertTable", buf)
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)

	// put stream
	buf = bytes.NewBuffer([]byte(`{"sql":"CREATE stream alert() WITH (DATASOURCE=\"0\", TYPE=\"httppull\")"}`))
	req, _ = http.NewRequest(http.MethodPut, "http://localhost:8080/streams/alert", buf)
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)

	// drop table
	req, _ = http.NewRequest(http.MethodDelete, "http://localhost:8080/tables/alertTable", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)

	// drop stream
	req, _ = http.NewRequest(http.MethodDelete, "http://localhost:8080/streams/alert", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
}

func (suite *RestTestSuite) TestRecoverRule() {
	// drop stream
	req, _ := http.NewRequest(http.MethodDelete, "http://localhost:8080/streams/recoverTest", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	req1, _ := http.NewRequest(http.MethodDelete, "http://localhost:8080/rules/recoverTest", bytes.NewBufferString("any"))
	w1 := httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)

	buf1 := bytes.NewBuffer([]byte(`{"sql":"CREATE stream recoverTest() WITH (DATASOURCE=\"0\", TYPE=\"mqtt\")"}`))
	req1, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/streams", buf1)
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	// create rule with trigger false
	ruleJson := `{"id": "recoverTest","triggered": false,"sql": "select * from recoverTest","actions": [{"log": {}}]}`

	buf2 := bytes.NewBuffer([]byte(ruleJson))
	req2, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/rules", buf2)
	w2 := httptest.NewRecorder()
	suite.r.ServeHTTP(w2, req2)

	req3, _ := http.NewRequest(http.MethodGet, "http://localhost:8080/rules", bytes.NewBufferString("any"))
	w3 := httptest.NewRecorder()
	suite.r.ServeHTTP(w3, req3)

	b, err := io.ReadAll(w3.Result().Body)
	require.NoError(suite.T(), err)
	got := make([]map[string]interface{}, 0)
	require.NoError(suite.T(), json.Unmarshal(b, &got))
	find := false
	for _, s := range got {
		if s["id"] == "recoverTest" {
			find = true
			require.Equal(suite.T(), "stopped", s["status"])
		}
	}
	require.True(suite.T(), find)
}

func (suite *RestTestSuite) TestRulesGetStateWrapper() {
	buf1 := bytes.NewBuffer([]byte(`{"sql":"CREATE stream qwe12() WITH (DATASOURCE=\"0\", TYPE=\"mqtt\")"}`))
	req1, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/streams", buf1)
	w1 := httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)

	// create rule with trigger false
	ruleJson := `{"id": "rule4441","triggered": false,"sql": "select * from qwe12","actions": [{"log": {}}]}`

	buf2 := bytes.NewBuffer([]byte(ruleJson))
	req2, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/rules", buf2)
	w2 := httptest.NewRecorder()
	suite.r.ServeHTTP(w2, req2)

	w, err := getAllRulesWithState()
	require.NoError(suite.T(), err)
	require.True(suite.T(), len(w) > 0)
}

func (suite *RestTestSuite) Test_rulesManageHandler() {
	connection.InitConnectionManager4Test()
	// Start rules
	if rules, err := ruleProcessor.GetAllRules(); err != nil {
		logger.Infof("Start rules error: %s", err)
	} else {
		logger.Info("Starting rules")
		var reply string
		for _, name := range rules {
			rule, err := ruleProcessor.GetRuleById(name)
			if err != nil {
				logger.Error(err)
				continue
			}
			// err = server.StartRule(rule, &reply)
			reply = registry.RecoverRule(rule)
			if 0 != len(reply) {
				logger.Info(reply)
			}
		}
	}

	all, err := streamProcessor.GetAll()
	require.NoError(suite.T(), err)
	for key := range all["streams"] {
		_, err := streamProcessor.DropStream(key, ast.TypeStream)
		require.NoError(suite.T(), err)
	}
	for key := range all["tables"] {
		_, err := streamProcessor.DropStream(key, ast.TypeTable)
		require.NoError(suite.T(), err)
	}

	buf1 := bytes.NewBuffer([]byte(`{"sql":"CREATE stream alert() WITH (DATASOURCE=\"0\", TYPE=\"mqtt\")"}`))
	req1, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/streams", buf1)
	w1 := httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)

	buf1 = bytes.NewBuffer([]byte(`{"sql":"create table hello() WITH (DATASOURCE=\"/hello\", FORMAT=\"JSON\", TYPE=\"httppull\")"}`))
	req1, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/tables", buf1)
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)

	req1, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/streamdetails", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ := io.ReadAll(w1.Result().Body)
	require.Equal(suite.T(), `[{"name":"alert","type":"mqtt","format":"json"}]`, string(returnVal))

	req1, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/tabledetails", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body)
	require.Equal(suite.T(), `[{"name":"hello","type":"httppull","format":"json"}]`, string(returnVal))

	suite.assertGetRuleHiddenPassword()

	// validate a rule
	ruleJson := `{"id": "rule1","triggered": false,"sql": "select * from alert","actions": [{"log": {}}]}`
	buf2 := bytes.NewBuffer([]byte(ruleJson))
	req2, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/rules/validate", buf2)
	w2 := httptest.NewRecorder()
	suite.r.ServeHTTP(w2, req2)
	returnVal, _ = io.ReadAll(w2.Result().Body)
	expect := `{"sources":["alert"],"valid":true}`
	assert.Equal(suite.T(), http.StatusOK, w2.Code)
	assert.Equal(suite.T(), expect, string(returnVal))

	// validate a wrong rule
	ruleJson = `{"id": "rule321", "sql": "select * from alert"}`
	buf2 = bytes.NewBuffer([]byte(ruleJson))
	req2, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/rules/validate", buf2)
	w2 = httptest.NewRecorder()
	suite.r.ServeHTTP(w2, req2)
	returnVal, _ = io.ReadAll(w2.Result().Body)
	expect = `invalid rule json: Missing rule actions.`
	assert.Equal(suite.T(), http.StatusUnprocessableEntity, w2.Code)
	assert.Equal(suite.T(), expect, string(returnVal))

	// delete rule
	req1, _ = http.NewRequest(http.MethodDelete, "http://localhost:8080/rules/rule3442551", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)

	// create rule with trigger false
	ruleJson = `{"id": "rule3/21","triggered": false,"sql": "select * from alert","actions": [{"log": {}}]}`
	buf2 = bytes.NewBuffer([]byte(ruleJson))
	req2, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/rules", buf2)
	w2 = httptest.NewRecorder()
	suite.r.ServeHTTP(w2, req2)
	returnVal, _ = io.ReadAll(w2.Result().Body)
	expect = `{"error":1000,"message":"invalid rule json: ruleID:rule3/21 contains invalidChar:/"}`
	expect = expect + "\n"
	assert.Equal(suite.T(), expect, string(returnVal))

	// create rule with trigger false
	ruleJson = `{"id": "rule321","triggered": false,"sql": "select * from alert","actions": [{"log": {}}]}`
	buf2 = bytes.NewBuffer([]byte(ruleJson))
	req2, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/rules", buf2)
	w2 = httptest.NewRecorder()
	suite.r.ServeHTTP(w2, req2)

	// get all rules
	req3, _ := http.NewRequest(http.MethodGet, "http://localhost:8080/rules", bytes.NewBufferString("any"))
	w3 := httptest.NewRecorder()
	suite.r.ServeHTTP(w3, req3)
	_, _ = io.ReadAll(w3.Result().Body)

	// update rule, will set rule to triggered
	ruleJson = `{"id": "rule321","triggered": true,"sql": "select * from alert","actions": [{"nop": {}}]}`
	buf2 = bytes.NewBuffer([]byte(ruleJson))
	req1, _ = http.NewRequest(http.MethodPut, "http://localhost:8080/rules/rule321", buf2)
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	assert.Equal(suite.T(), http.StatusOK, w1.Code)

	time.Sleep(10 * time.Millisecond)

	// update wrong rule
	ruleJson = `{"id": "rule321","sql": "select * from alert1","actions": [{"nop": {}}]}`
	buf2 = bytes.NewBuffer([]byte(ruleJson))
	req1, _ = http.NewRequest(http.MethodPut, "http://localhost:8080/rules/rule321", buf2)
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	assert.Equal(suite.T(), http.StatusBadRequest, w1.Code)

	// get rule
	req1, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/rules/rule321", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body)
	expect = `{"id": "rule321","triggered": true,"sql": "select * from alert","actions": [{"nop": {}}]}`
	assert.Equal(suite.T(), expect, string(returnVal))

	// get rule status
	req1, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/rules/rule1/status", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body) //nolint

	// get rule status v2
	req1, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/v2/rules/rule321/status", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body) //nolint
	m := make(map[string]any)
	require.NoError(suite.T(), json.Unmarshal(returnVal, &m))
	require.Equal(suite.T(), http.StatusOK, w1.Code)

	req1, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/rules/rule32211/explain", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body) //nolint
	returnStr := string(returnVal)
	expect = "{\"error\":1002,\"message\":\"explain rules error: Rule rule32211 is not found.\"}\n"
	assert.Equal(suite.T(), expect, returnStr)

	// get rule topo
	req1, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/rules/rule321/topo", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body)
	expect = "{\"sources\":[\"source_alert\"],\"edges\":{\"op_2_decoder\":[\"op_3_project\"],\"op_3_project\":[\"op_nop_0_0_transform\"],\"op_nop_0_0_transform\":[\"op_nop_0_1_encode\"],\"op_nop_0_1_encode\":[\"sink_nop_0\"],\"source_alert\":[\"op_2_decoder\"]}}"
	assert.Equal(suite.T(), expect, string(returnVal))

	// start rule
	req1, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/rules/rule321/start", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body)
	expect = `Rule rule321 was started`
	assert.Equal(suite.T(), expect, string(returnVal))

	// start non-existence rule
	req1, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/rules/non-existence-rule/start", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body)
	equal, err := assertErrorCode(errorx.NOT_FOUND, returnVal)
	require.NoError(suite.T(), err)
	require.True(suite.T(), equal)
	assert.Equal(suite.T(), http.StatusNotFound, w1.Code)

	// stop rule
	req1, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/rules/rule321/stop", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body)
	expect = `Rule rule321 was stopped.`
	assert.Equal(suite.T(), expect, string(returnVal))

	// stop non-existence rule
	req1, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/rules/non-existence-rule/stop", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body)
	equal, err = assertErrorCode(errorx.NOT_FOUND, returnVal)
	require.NoError(suite.T(), err)
	require.True(suite.T(), equal)
	assert.Equal(suite.T(), http.StatusNotFound, w1.Code)

	// update rule, will set rule to triggered
	ruleJson = `{"id": "rule321","triggered": false,"sql": "select * from alert","actions": [{"nop": {}}]}`
	buf2 = bytes.NewBuffer([]byte(ruleJson))
	req1, _ = http.NewRequest(http.MethodPut, "http://localhost:8080/rules/rule321", buf2)
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	assert.Equal(suite.T(), http.StatusOK, w1.Code)

	// restart rule
	req1, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/rules/rule321/restart", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body)
	expect = `Rule rule321 was restarted`
	assert.Equal(suite.T(), expect, string(returnVal))

	// get rule
	req1, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/rules/rule321", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body)
	expect = "{\"actions\":[{\"nop\":{}}],\"id\":\"rule321\",\"sql\":\"select * from alert\",\"triggered\":true}"
	assert.Equal(suite.T(), expect, string(returnVal))

	// delete rule
	req1, _ = http.NewRequest(http.MethodDelete, "http://localhost:8080/rules/rule321", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)

	// drop stream
	req, _ := http.NewRequest(http.MethodDelete, "http://localhost:8080/streams/alert", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
}

func (suite *RestTestSuite) assertGetRuleHiddenPassword() {
	ruleJson2 := `{"id":"rule3442551","triggered":false,"sql":"select * from alert","actions":[{"mqtt":{"password":"123","topic":"123","server":"123"}}]}`
	buf2 := bytes.NewBuffer([]byte(ruleJson2))
	req2, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/rules", buf2)
	w2 := httptest.NewRecorder()
	suite.r.ServeHTTP(w2, req2)
	require.Equal(suite.T(), http.StatusCreated, w2.Code)

	// get rule
	req1, _ := http.NewRequest(http.MethodGet, "http://localhost:8080/rules/rule3442551", bytes.NewBufferString("any"))
	w1 := httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	require.Equal(suite.T(), http.StatusOK, w1.Code)

	returnVal, _ := io.ReadAll(w1.Result().Body)
	rule2 := &def.Rule{}
	require.NoError(suite.T(), json.Unmarshal(returnVal, rule2))
	require.Len(suite.T(), rule2.Actions, 1)
	mqttAction := rule2.Actions[0]["mqtt"]
	require.NotNil(suite.T(), mqttAction)
	mqttOption, ok := mqttAction.(map[string]interface{})
	require.True(suite.T(), ok)
	require.Equal(suite.T(), "123", mqttOption["password"])

	// delete rule
	req1, _ = http.NewRequest(http.MethodDelete, "http://localhost:8080/rules/rule3442551", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
}

func assertErrorCode(code errorx.ErrorCode, resp []byte) (bool, error) {
	m := struct {
		ErrorCode int `json:"error"`
	}{}
	err := json.Unmarshal(resp, &m)
	if err != nil {
		return false, err
	}
	return m.ErrorCode == int(code), nil
}

func (suite *RestTestSuite) Test_ruleTestHandler() {
	suite.T().Skip()
	buf1 := bytes.NewBuffer([]byte(`{"sql":"CREATE stream alert() WITH (DATASOURCE=\"0\", TYPE=\"mqtt\")"}`))
	req1, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/streams", buf1)
	w1 := httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)

	// create rule with trigger false
	ruleJson := `{"id":"rule1","sql":"select * from alert","mockSource":{"alert":{"data":[{"name":"demo","value":1},{"name":"demo","value":2}],"interval":1,"loop":false}},"sinkProps":{"sendSingle":true}}`

	buf2 := bytes.NewBuffer([]byte(ruleJson))
	req2, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/ruletest", buf2)
	w2 := httptest.NewRecorder()
	suite.r.ServeHTTP(w2, req2)

	assert.Equal(suite.T(), http.StatusOK, w2.Code)
	assert.Equal(suite.T(), "{\"id\":\"rule1\",\"port\":10081}", w2.Body.String())

	// start rule
	req1, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/ruletest/rule1/start", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ := io.ReadAll(w1.Result().Body)

	expect := `Test rule rule1 was started`
	assert.Equal(suite.T(), expect, string(returnVal))

	// delete rule
	req1, _ = http.NewRequest(http.MethodDelete, "http://localhost:8080/ruletest/rule1", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	assert.Equal(suite.T(), http.StatusOK, w1.Code)

	// drop stream
	req, _ := http.NewRequest(http.MethodDelete, "http://localhost:8080/streams/alert", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
}

func (suite *RestTestSuite) Test_configUpdate() {
	req, _ := http.NewRequest(http.MethodPatch, "http://localhost:8080/configs", bytes.NewBufferString(""))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)

	b, _ := json.Marshal(map[string]any{
		"debug":    true,
		"timezone": "",
	})
	req, _ = http.NewRequest(http.MethodPatch, "http://localhost:8080/configs", bytes.NewBuffer(b))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusNoContent, w.Code)

	b, _ = json.Marshal(map[string]any{
		"debug":    true,
		"timezone": "unknown",
	})
	req, _ = http.NewRequest(http.MethodPatch, "http://localhost:8080/configs", bytes.NewBuffer(b))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)

	b, _ = json.Marshal(map[string]any{
		"debug":   true,
		"fileLog": true,
	})
	req, _ = http.NewRequest(http.MethodPatch, "http://localhost:8080/configs", bytes.NewBuffer(b))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusNoContent, w.Code)

	b, _ = json.Marshal(map[string]any{
		"debug":      true,
		"consoleLog": true,
	})
	req, _ = http.NewRequest(http.MethodPatch, "http://localhost:8080/configs", bytes.NewBuffer(b))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusNoContent, w.Code)
}

func (suite *RestTestSuite) Test_ruleSetImport() {
	ruleJson := `{"streams":{"plugin":"\n              CREATE STREAM plugin\n              ()\n              WITH (FORMAT=\"json\", CONF_KEY=\"default\", TYPE=\"mqtt\", SHARED=\"false\", );\n          "},"tables":{},"rules":{"rule1":"{\"id\":\"rule1\",\"name\":\"\",\"sql\":\"select name from plugin\",\"actions\":[{\"log\":{\"runAsync\":false,\"omitIfEmpty\":false,\"sendSingle\":true,\"bufferLength\":1024,\"enableCache\":false,\"format\":\"json\"}}],\"options\":{\"restartStrategy\":{}}}"}}`
	ruleSetJson := map[string]string{
		"content": ruleJson,
	}
	buf, _ := json.Marshal(ruleSetJson)
	buf2 := bytes.NewBuffer(buf)
	req1, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/ruleset/import", buf2)
	w1 := httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	assert.Equal(suite.T(), http.StatusOK, w1.Code)

	req1, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/ruleset/export", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	assert.Equal(suite.T(), http.StatusOK, w1.Code)
}

func (suite *RestTestSuite) Test_dataImport() {
	file := "rpc_test_data/data/import_configuration.json"
	f, err := os.Open(file)
	if err != nil {
		fmt.Printf("fail to open file %s: %v", file, err)
		return
	}
	defer f.Close()
	buffer := new(bytes.Buffer)
	_, err = io.Copy(buffer, f)
	if err != nil {
		fmt.Printf("fail to convert file %s: %v", file, err)
		return
	}
	content := buffer.Bytes()
	ruleSetJson := map[string]string{
		"content": string(content),
	}
	buf, _ := json.Marshal(ruleSetJson)
	buf2 := bytes.NewBuffer(buf)
	req, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/data/import", buf2)
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)

	req, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/data/import/status", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)

	req, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/data/export", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)

	req, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/data/import?partial=1", bytes.NewBuffer(buf))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)

	issueData := `{"content":"{\"streams\": {\"issueTest\": \"CREATE STREAM issueTest () WITH (DATASOURCE=\\\"users\\\", CONF_KEY=\\\"issueTest_1\\\",TYPE=\\\"none\\\", FORMAT=\\\"JSON\\\")\"},\"sourceConfig\":{\n    \"mqtt\":\"{\\\"issueTest_1\\\":{\\\"insecureSkipVerify\\\":false,\\\"password\\\":\\\"public\\\",\\\"protocolVersion\\\":\\\"3.1.1\\\",\\\"qos\\\":1,\\\"server\\\":\\\"tcp://broker.emqx.io:1883\\\",\\\"username\\\":\\\"admin\\\"},\\\"issueTest_2\\\":{\\\"insecureSkipVerify\\\":false,\\\"password\\\":\\\"public\\\",\\\"protocolVersion\\\":\\\"3.1.1\\\",\\\"qos\\\":1,\\\"server\\\":\\\"tcp://127.0.0.1:1883\\\",\\\"username\\\":\\\"admin\\\"}}\"\n  }}"}`
	req, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/data/import?partial=1", bytes.NewBuffer([]byte(issueData)))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
}

func (suite *RestTestSuite) Test_fileUpload() {
	fileJson := `{"Name": "test.txt", "Content": "test"}`
	req, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/config/uploads", bytes.NewBufferString(fileJson))
	req.Header["Content-Type"] = []string{"application/json"}
	os.Mkdir(uploadDir, 0o777)
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusCreated, w.Code)

	postContent := map[string]string{}
	postContent["Name"] = "test1.txt"
	postContent["file"] = "file://" + uploadDir + "/test.txt"

	bdy, _ := json.Marshal(postContent)
	req, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/config/uploads", bytes.NewBuffer(bdy))
	req.Header["Content-Type"] = []string{"application/json"}
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusCreated, w.Code)

	req, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/config/uploads", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)

	req, _ = http.NewRequest(http.MethodDelete, "http://localhost:8080/config/uploads/test.txt", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)

	req, _ = http.NewRequest(http.MethodDelete, "http://localhost:8080/config/uploads/test1.txt", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
	os.Remove(uploadDir)
}

func (suite *RestTestSuite) Test_fileUploadValidate() {
	fileJson := `{"Name": "test.txt", "Content": test}`
	req, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/config/uploads", bytes.NewBufferString(fileJson))
	req.Header["Content-Type"] = []string{"application/json"}
	os.Mkdir(uploadDir, 0o777)
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)

	fileJson = `{"Name": "test.txt", "Contents": "test"}`
	req, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/config/uploads", bytes.NewBufferString(fileJson))
	req.Header["Content-Type"] = []string{"application/json"}
	os.Mkdir(uploadDir, 0o777)
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)

	fileJson = `{"Content": "test"}`
	req, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/config/uploads", bytes.NewBufferString(fileJson))
	req.Header["Content-Type"] = []string{"application/json"}
	os.Mkdir(uploadDir, 0o777)
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)

	postContent := map[string]string{}
	postContent["Name"] = "test1.txt"
	postContent["file"] = "file://" + uploadDir + "/test.txt"

	bdy, _ := json.Marshal(postContent)
	req, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/config/uploads", bytes.NewBuffer(bdy))
	req.Header["Content-Type"] = []string{"application/json"}
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)

	os.Remove(uploadDir)
}

func TestRestTestSuite(t *testing.T) {
	suite.Run(t, new(RestTestSuite))
}

//func (suite *ServerTestSuite) TestCreateInValidSinkRule() {
//	var sql, reply string
//	var err error
//	sql = `Create Stream test543 (a bigint) WITH (DATASOURCE="../internal/server/rpc_test_data/test.json", FORMAT="JSON", type="file");`
//	err = suite.s.Stream(sql, &reply)
//	require.NoError(suite.T(), err)
//	reply = ""
//	rule := `{"id":"rule","sql":"select * from test543","actions":[{"mqtt":{"server":"tcp://docker.for.mac.host.internal:1883","topic":"collect/labels","qos":100,"clientId":"center","sendSingle":true}}]}`
//	ruleId := "rule"
//	args := &model.RPCArgDesc{Name: ruleId, Json: rule}
//	err = suite.s.CreateRule(args, &reply)
//	require.Error(suite.T(), err)
//}
//
//func (suite *ServerTestSuite) TestStartRuleAfterSchemaChange() {
//	reply := ""
//	sql := `Create Stream test (a bigint) WITH (DATASOURCE="../internal/server/rpc_test_data/test.json", FORMAT="JSON", type="file");`
//	err := suite.s.Stream(sql, &reply)
//	assert.Nil(suite.T(), err)
//	assert.Equal(suite.T(), "Stream test is created.\n", reply)
//
//	reply = ""
//	rule := `{
//			  "sql": "SELECT a from test;",
//			  "actions": [{
//				"file": {
//				  "path": "../internal/server/rpc_test_data/data/result.txt",
//				  "interval": 5000,
//				  "fileType": "lines",
//				  "format": "json"
//				}
//			  }]
//	}`
//	ruleId := "myRule"
//	args := &model.RPCArgDesc{Name: ruleId, Json: rule}
//	err = suite.s.CreateRule(args, &reply)
//	assert.Nil(suite.T(), err)
//	assert.Equal(suite.T(), "Rule myRule was created successfully, please use 'bin/kuiper getstatus rule myRule' command to get rule status.", reply)
//
//	reply = ""
//	err = suite.s.GetStatusRule(ruleId, &reply)
//	assert.Nil(suite.T(), err)
//
//	reply = ""
//	err = suite.s.StopRule(ruleId, &reply)
//	assert.Nil(suite.T(), err)
//	assert.Equal(suite.T(), "Rule myRule was stopped.", reply)
//
//	reply = ""
//	sql = `drop stream test`
//	err = suite.s.Stream(sql, &reply)
//	assert.Nil(suite.T(), err)
//	assert.Equal(suite.T(), "Stream test is dropped.\n", reply)
//
//	reply = ""
//	sql = `Create Stream test (b bigint) WITH (DATASOURCE="../internal/server/rpc_test_data/test.json", FORMAT="JSON", type="file");`
//	err = suite.s.Stream(sql, &reply)
//	assert.Nil(suite.T(), err)
//	assert.Equal(suite.T(), "Stream test is created.\n", reply)
//
//	reply = ""
//	err = suite.s.StartRule(ruleId, &reply)
//	assert.Error(suite.T(), err)
//	assert.Equal(suite.T(), err.Error(), "unknown field a")
//}

func (suite *RestTestSuite) TestUpdateRuleOffset() {
	req1, _ := http.NewRequest(http.MethodPut, "http://localhost:8080/rules/rule344421/reset_state", bytes.NewBufferString(`123`))
	w1 := httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ := io.ReadAll(w1.Result().Body) //nolint
	returnStr := string(returnVal)
	require.Equal(suite.T(), `{"error":1000,"message":"json: cannot unmarshal number into Go value of type server.ruleStateUpdateRequest"}`+"\n", returnStr)

	req1, _ = http.NewRequest(http.MethodPut, "http://localhost:8080/rules/rule344421/reset_state", bytes.NewBufferString(`{"type":0,"params":{"streamName":"demo","input":{"a":1}}}`))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body) //nolint
	returnStr = string(returnVal)
	require.Equal(suite.T(), `{"error":1000,"message":"unknown stateType:0"}`+"\n", returnStr)

	req1, _ = http.NewRequest(http.MethodPut, "http://localhost:8080/rules/rule344421/reset_state", bytes.NewBufferString(`{"type":1,"params":{"streamName":"demo","input":{"a":1}}}`))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body) //nolint
	returnStr = string(returnVal)
	require.Equal(suite.T(), `{"error":1000,"message":"Rule rule344421 is not found in registry"}`+"\n", returnStr)
	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/server/updateOffset", "return(1)")
	defer func() {
		failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/server/updateOffset")
	}()

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/server/updateOffset", "return(2)")
	req1, _ = http.NewRequest(http.MethodPut, "http://localhost:8080/rules/rule344421/reset_state", bytes.NewBufferString(`{"type":1,"params":{"streamName":"demo","input":{"a":1}}}`))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body) //nolint
	returnStr = string(returnVal)
	require.Equal(suite.T(), `{"error":1000,"message":"rule rule344421 should be running when modify state"}`+"\n", returnStr)

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/server/updateOffset", "return(3)")
	req1, _ = http.NewRequest(http.MethodPut, "http://localhost:8080/rules/rule344421/reset_state", bytes.NewBufferString(`{"type":1,"params":{"streamName":"demo","input":{"a":1}}}`))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body) //nolint
	returnStr = string(returnVal)
	require.Equal(suite.T(), `success`, returnStr)
}

func (suite *RestTestSuite) TestCreateDuplicateRule() {
	buf1 := bytes.NewBuffer([]byte(`{"sql":"CREATE stream demo123() WITH (DATASOURCE=\"0\", TYPE=\"mqtt\")"}`))
	req1, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/streams", buf1)
	w1 := httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)

	ruleJson2 := `{"id":"test12345","triggered":false,"sql":"select * from demo123","actions":[{"log":{}}]}`
	buf2 := bytes.NewBuffer([]byte(ruleJson2))
	req2, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/rules", buf2)
	w2 := httptest.NewRecorder()
	suite.r.ServeHTTP(w2, req2)
	require.Equal(suite.T(), http.StatusCreated, w2.Code)

	buf2 = bytes.NewBuffer([]byte(ruleJson2))
	req2, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/rules", buf2)
	w2 = httptest.NewRecorder()
	suite.r.ServeHTTP(w2, req2)
	require.Equal(suite.T(), http.StatusBadRequest, w2.Code)
	var returnVal []byte
	returnVal, _ = io.ReadAll(w2.Result().Body)
	require.Equal(suite.T(), `{"error":1000,"message":"rule test12345 already exists"}`+"\n", string(returnVal))
}

func (suite *RestTestSuite) TestGetAllRuleStatus() {
	buf1 := bytes.NewBuffer([]byte(`{"sql":"CREATE stream demo456() WITH (DATASOURCE=\"0\", TYPE=\"mqtt\")"}`))
	req1, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/streams", buf1)
	w1 := httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)

	ruleJson2 := `{"id":"allRule1","sql":"select * from demo456","actions":[{"log":{}}]}`
	buf2 := bytes.NewBuffer([]byte(ruleJson2))
	req2, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/rules", buf2)
	w2 := httptest.NewRecorder()
	suite.r.ServeHTTP(w2, req2)
	require.Equal(suite.T(), http.StatusCreated, w2.Code)

	ruleJson2 = `{"id":"allRule2","sql":"select * from demo456","actions":[{"log":{}}]}`
	buf2 = bytes.NewBuffer([]byte(ruleJson2))
	req2, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/rules", buf2)
	w2 = httptest.NewRecorder()
	suite.r.ServeHTTP(w2, req2)
	require.Equal(suite.T(), http.StatusCreated, w2.Code)

	req, _ := http.NewRequest(http.MethodGet, "http://localhost:8080/rules/status/all", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
	var returnVal []byte
	returnVal, _ = io.ReadAll(w.Result().Body)
	var m map[string]interface{}
	require.NoError(suite.T(), json.Unmarshal(returnVal, &m))
	_, ok := m["allRule1"]
	require.True(suite.T(), ok)
	_, ok = m["allRule2"]
	require.True(suite.T(), ok)
}

func (suite *RestTestSuite) TestWaitStopRule() {
	ip := "127.0.0.1"
	port := 10085
	httpserver.InitGlobalServerManager(ip, port, nil)
	connection.InitConnectionManager4Test()

	// delete create
	req, _ := http.NewRequest(http.MethodDelete, "http://localhost:8080/streams/demo221", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)

	// delete rule
	req, _ = http.NewRequest(http.MethodDelete, "http://localhost:8080/rules/rule221", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)

	// create stream
	buf := bytes.NewBuffer([]byte(`{"sql":"CREATE stream demo221() WITH (DATASOURCE=\"/data1\", TYPE=\"websocket\")"}`))
	req, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/streams", buf)
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	require.Equal(suite.T(), http.StatusCreated, w.Code)

	// create rule
	ruleJson := `{"id": "rule221","sql": "select a,b from demo221","actions": [{"log": {}}]}`
	buf = bytes.NewBuffer([]byte(ruleJson))
	req, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/rules", buf)
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	require.Equal(suite.T(), http.StatusCreated, w.Code)

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/topo/node/mockTimeConsumingClose", "return(true)")
	defer failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/topo/node/mockTimeConsumingClose")
	now := time.Now()
	// delete rule
	req, _ = http.NewRequest(http.MethodDelete, "http://localhost:8080/rules/rule221", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	end := time.Now()
	require.True(suite.T(), end.Sub(now) >= 300*time.Millisecond)

	// create rule
	buf = bytes.NewBuffer([]byte(ruleJson))
	req, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/rules", buf)
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	require.Equal(suite.T(), http.StatusCreated, w.Code)

	now = time.Now()
	// stop rule
	req, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/rules/rule221/stop", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	end = time.Now()
	require.True(suite.T(), end.Sub(now) >= 300*time.Millisecond)
	waitAllRuleStop()
}
