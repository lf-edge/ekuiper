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

	"github.com/gorilla/mux"
	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/meta"
	"github.com/lf-edge/ekuiper/internal/pkg/model"
	"github.com/lf-edge/ekuiper/internal/pkg/store"
	"github.com/lf-edge/ekuiper/internal/processor"
	"github.com/lf-edge/ekuiper/internal/testx"
	"github.com/lf-edge/ekuiper/internal/topo/connection/factory"
	"github.com/lf-edge/ekuiper/internal/topo/rule"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/errorx"
)

func init() {
	testx.InitEnv("server")
	streamProcessor = processor.NewStreamProcessor()
	ruleProcessor = processor.NewRuleProcessor()
	rulesetProcessor = processor.NewRulesetProcessor(ruleProcessor, streamProcessor)
	registry = &RuleRegistry{internal: make(map[string]*rule.RuleState)}
	uploadsDb, _ = store.GetKV("uploads")
	uploadsStatusDb, _ = store.GetKV("uploadsStatusDb")
	sysMetrics = NewMetrics()
	factory.InitClientsFactory()
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
	r.HandleFunc("/rules/{name}/start", startRuleHandler).Methods(http.MethodPost)
	r.HandleFunc("/rules/{name}/stop", stopRuleHandler).Methods(http.MethodPost)
	r.HandleFunc("/rules/{name}/restart", restartRuleHandler).Methods(http.MethodPost)
	r.HandleFunc("/rules/{name}/topo", getTopoRuleHandler).Methods(http.MethodGet)
	r.HandleFunc("/rules/{name}/reset_state", ruleStateHandler).Methods(http.MethodPut)
	r.HandleFunc("/rules/{name}/explain", explainRuleHandler).Methods(http.MethodGet)
	r.HandleFunc("/rules/validate", validateRuleHandler).Methods(http.MethodPost)
	r.HandleFunc("/rules/status/all", getAllRuleStatusHandler).Methods(http.MethodGet)
	r.HandleFunc("/ruletest", testRuleHandler).Methods(http.MethodPost)
	r.HandleFunc("/ruletest/{name}/start", testRuleStartHandler).Methods(http.MethodPost)
	r.HandleFunc("/ruletest/{name}", testRuleStopHandler).Methods(http.MethodDelete)
	r.HandleFunc("/ruleset/export", exportHandler).Methods(http.MethodPost)
	r.HandleFunc("/ruleset/import", importHandler).Methods(http.MethodPost)
	r.HandleFunc("/configs", configurationUpdateHandler).Methods(http.MethodPatch)
	r.HandleFunc("/config/uploads", fileUploadHandler).Methods(http.MethodPost, http.MethodGet)
	r.HandleFunc("/config/uploads/{name}", fileDeleteHandler).Methods(http.MethodDelete)
	r.HandleFunc("/data/export", configurationExportHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/data/import", configurationImportHandler).Methods(http.MethodPost)
	r.HandleFunc("/data/import/status", configurationStatusHandler).Methods(http.MethodGet)
	r.HandleFunc("/connection/websocket", connectionHandler).Methods(http.MethodGet, http.MethodPost, http.MethodDelete)
	r.HandleFunc("/metadata/sinks/{name}/confKeys/{confKey}", sinkConfKeyHandler).Methods(http.MethodDelete, http.MethodPut)
	suite.r = r
}

func (suite *RestTestSuite) Test_Connection() {
	req, err := http.NewRequest(http.MethodPost, "/connection/websocket", bytes.NewBufferString(`{"endpoint":"/api/data"}`))
	require.NoError(suite.T(), err)
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
	req, err = http.NewRequest(http.MethodDelete, "/connection/websocket", bytes.NewBufferString(`{"endpoint":"/api/data"}`))
	require.NoError(suite.T(), err)
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
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
			reply = recoverRule(rule)
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

	req1, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/rules/rule321/explain", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body) //nolint
	returnStr := string(returnVal)
	expect = "{\"type\":\"ProjectPlan\",\"info\":\"Fields:[ * ]\",\"id\":0,\"children\":[1]}\n\n   {\"type\":\"DataSourcePlan\",\"info\":\"StreamName: alert wildcard:true\",\"id\":1,\"children\":null}\n\n"
	assert.Equal(suite.T(), expect, returnStr)

	req1, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/rules/rule32211/explain", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body) //nolint
	returnStr = string(returnVal)
	expect = "{\"error\":1002,\"message\":\"explain rules error: Rule rule32211 is not found.\"}\n"
	assert.Equal(suite.T(), expect, returnStr)

	// get rule topo
	req1, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/rules/rule321/topo", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body)

	expect = "{\"sources\":[\"source_alert\"],\"edges\":{\"op_2_decoder\":[\"op_3_project\"],\"op_3_project\":[\"sink_nop_0\"],\"source_alert\":[\"op_2_decoder\"]}}"
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
	expect = `{"triggered":true,"id":"rule321","sql":"select * from alert","actions":[{"nop":{}}],"options":{"debug":false,"logFilename":"","isEventTime":false,"lateTolerance":1000,"concurrency":1,"bufferLength":1024,"sendMetaToSink":false,"sendError":true,"qos":0,"checkpointInterval":300000,"restartStrategy":{"attempts":0,"delay":1000,"multiplier":2,"maxDelay":30000,"jitterFactor":0.1},"cron":"","duration":"","cronDatetimeRange":null}}`
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
	rule2 := &api.Rule{}
	require.NoError(suite.T(), json.Unmarshal(returnVal, rule2))
	require.Len(suite.T(), rule2.Actions, 1)
	mqttAction := rule2.Actions[0]["mqtt"]
	require.NotNil(suite.T(), mqttAction)
	mqttOption, ok := mqttAction.(map[string]interface{})
	require.True(suite.T(), ok)
	require.Equal(suite.T(), "******", mqttOption["password"])

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
	factory.InitClientsFactory()
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

func (suite *ServerTestSuite) TestCreateInValidSinkRule() {
	var sql, reply string
	var err error
	sql = `Create Stream test543 (a bigint) WITH (DATASOURCE="../internal/server/rpc_test_data/test.json", FORMAT="JSON", type="file");`
	err = suite.s.Stream(sql, &reply)
	require.NoError(suite.T(), err)
	reply = ""
	rule := `{"id":"rule","sql":"select * from test543","actions":[{"mqtt":{"server":"tcp://docker.for.mac.host.internal:1883","topic":"collect/labels","qos":100,"clientId":"center","sendSingle":true}}]}`
	ruleId := "rule"
	args := &model.RPCArgDesc{Name: ruleId, Json: rule}
	err = suite.s.CreateRule(args, &reply)
	require.Error(suite.T(), err)
}

func (suite *ServerTestSuite) TestStartRuleAfterSchemaChange() {
	reply := ""
	sql := `Create Stream test (a bigint) WITH (DATASOURCE="../internal/server/rpc_test_data/test.json", FORMAT="JSON", type="file");`
	err := suite.s.Stream(sql, &reply)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "Stream test is created.\n", reply)

	reply = ""
	rule := `{
			  "sql": "SELECT a from test;",
			  "actions": [{
				"file": {
				  "path": "../internal/server/rpc_test_data/data/result.txt",
				  "interval": 5000,
				  "fileType": "lines",
				  "format": "json"
				}
			  }]
	}`
	ruleId := "myRule"
	args := &model.RPCArgDesc{Name: ruleId, Json: rule}
	err = suite.s.CreateRule(args, &reply)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "Rule myRule was created successfully, please use 'bin/kuiper getstatus rule myRule' command to get rule status.", reply)

	reply = ""
	err = suite.s.GetStatusRule(ruleId, &reply)
	assert.Nil(suite.T(), err)

	reply = ""
	err = suite.s.StopRule(ruleId, &reply)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "Rule myRule was stopped.", reply)

	reply = ""
	sql = `drop stream test`
	err = suite.s.Stream(sql, &reply)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "Stream test is dropped.\n", reply)

	reply = ""
	sql = `Create Stream test (b bigint) WITH (DATASOURCE="../internal/server/rpc_test_data/test.json", FORMAT="JSON", type="file");`
	err = suite.s.Stream(sql, &reply)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "Stream test is created.\n", reply)

	reply = ""
	err = suite.s.StartRule(ruleId, &reply)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), err.Error(), "unknown field a")
}

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
	failpoint.Enable("github.com/lf-edge/ekuiper/internal/server/updateOffset", "return(1)")
	defer func() {
		failpoint.Disable("github.com/lf-edge/ekuiper/internal/server/updateOffset")
	}()

	failpoint.Enable("github.com/lf-edge/ekuiper/internal/server/updateOffset", "return(2)")
	req1, _ = http.NewRequest(http.MethodPut, "http://localhost:8080/rules/rule344421/reset_state", bytes.NewBufferString(`{"type":1,"params":{"streamName":"demo","input":{"a":1}}}`))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body) //nolint
	returnStr = string(returnVal)
	require.Equal(suite.T(), `{"error":1000,"message":"rule rule344421 should be running when modify state"}`+"\n", returnStr)

	failpoint.Enable("github.com/lf-edge/ekuiper/internal/server/updateOffset", "return(3)")
	req1, _ = http.NewRequest(http.MethodPut, "http://localhost:8080/rules/rule344421/reset_state", bytes.NewBufferString(`{"type":1,"params":{"streamName":"demo","input":{"a":1}}}`))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body) //nolint
	returnStr = string(returnVal)
	require.Equal(suite.T(), `success`, returnStr)
}

func (suite *RestTestSuite) TestCreateRuleReplacePasswd() {
	meta.InitYamlConfigManager()
	confKeyJson := `{"insecureSkipVerify":false,"protocolVersion":"3.1.1","qos":1,"server":"tcp://122.9.166.75:1883","token":"123","password":"4444"}`
	req, _ := http.NewRequest(http.MethodPut, "http://localhost:8080/metadata/sinks/mqtt/confKeys/broker", bytes.NewBufferString(confKeyJson))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	require.Equal(suite.T(), http.StatusOK, w.Code)

	buf1 := bytes.NewBuffer([]byte(`{"sql":"CREATE stream demodemo() WITH (DATASOURCE=\"0\", TYPE=\"mqtt\")"}`))
	req1, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/streams", buf1)
	w1 := httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)

	ruleJson2 := `{"id":"test1234","triggered":false,"sql":"select * from demodemo","actions":[{"mqtt":{"password":"******","topic":"123","server":"123","resourceId":"broker"}}]}`
	buf2 := bytes.NewBuffer([]byte(ruleJson2))
	req2, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/rules", buf2)
	w2 := httptest.NewRecorder()
	suite.r.ServeHTTP(w2, req2)
	require.Equal(suite.T(), http.StatusCreated, w2.Code)

	r, err := ruleProcessor.GetRuleById("test1234")
	require.NoError(suite.T(), err)
	mqttSinkConfig := r.Actions[0]["mqtt"]
	require.NotNil(suite.T(), mqttSinkConfig)
	c, ok := mqttSinkConfig.(map[string]interface{})
	require.True(suite.T(), ok)
	require.Equal(suite.T(), "4444", c["password"])
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

func (suite *RestTestSuite) TestSinkHiddenPassword() {
	buf1 := bytes.NewBuffer([]byte(`{"sql":"CREATE stream demo78() WITH (DATASOURCE=\"0\", TYPE=\"mqtt\")"}`))
	req1, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/streams", buf1)
	w1 := httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)

	ruleJson2 := `{"triggered":false,"id":"rule34","sql":"select * from demo78;","actions":[{"mqtt":{"server":"tcp://broker.emqx.io:1883","topic":"devices/demo_001/messages/events/","qos":0,"clientId":"demo_001","username":"xyz.azure-devices.net/demo_001/?api-version=2018-06-30","password":"12345"}}]}`
	buf2 := bytes.NewBuffer([]byte(ruleJson2))
	req2, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/rules", buf2)
	w2 := httptest.NewRecorder()
	suite.r.ServeHTTP(w2, req2)
	require.Equal(suite.T(), http.StatusCreated, w2.Code)

	ruleJson2 = `{"triggered":false,"id":"rule34","sql":"select * from demo78;","actions":[{"mqtt":{"server":"tcp://broker.emqx.io:1883","topic":"devices/demo_001/messages/events/","qos":0,"clientId":"demo_001","username":"xyz.azure-devices.net/demo_001/?api-version=2018-06-30","password":"******"}}]}`
	buf2 = bytes.NewBuffer([]byte(ruleJson2))
	req2, _ = http.NewRequest(http.MethodPut, "http://localhost:8080/rules/rule34", buf2)
	w2 = httptest.NewRecorder()
	suite.r.ServeHTTP(w2, req2)
	require.Equal(suite.T(), http.StatusOK, w2.Code)

	ruleJson, err := ruleProcessor.GetRuleJson("rule34")
	require.NoError(suite.T(), err)
	r := &api.Rule{}
	require.NoError(suite.T(), json.Unmarshal([]byte(ruleJson), r))
	m := r.Actions[0]["mqtt"].(map[string]interface{})
	require.Equal(suite.T(), "12345", m["password"])
}
