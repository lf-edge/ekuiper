// Copyright 2023 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/processor"
	"github.com/lf-edge/ekuiper/internal/testx"
	"github.com/lf-edge/ekuiper/internal/topo/rule"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func init() {
	testx.InitEnv()
	streamProcessor = processor.NewStreamProcessor()
	ruleProcessor = processor.NewRuleProcessor()
	rulesetProcessor = processor.NewRulesetProcessor(ruleProcessor, streamProcessor)
	registry = &RuleRegistry{internal: make(map[string]*rule.RuleState)}
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
	r.HandleFunc("/streams/{name}", streamHandler).Methods(http.MethodGet, http.MethodDelete, http.MethodPut)
	r.HandleFunc("/streams/{name}/schema", streamSchemaHandler).Methods(http.MethodGet)
	r.HandleFunc("/tables", tablesHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/tables/{name}", tableHandler).Methods(http.MethodGet, http.MethodDelete, http.MethodPut)
	r.HandleFunc("/tables/{name}/schema", tableSchemaHandler).Methods(http.MethodGet)
	r.HandleFunc("/rules", rulesHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/rules/{name}", ruleHandler).Methods(http.MethodDelete, http.MethodGet, http.MethodPut)
	r.HandleFunc("/rules/{name}/status", getStatusRuleHandler).Methods(http.MethodGet)
	r.HandleFunc("/rules/{name}/start", startRuleHandler).Methods(http.MethodPost)
	r.HandleFunc("/rules/{name}/stop", stopRuleHandler).Methods(http.MethodPost)
	r.HandleFunc("/rules/{name}/restart", restartRuleHandler).Methods(http.MethodPost)
	r.HandleFunc("/rules/{name}/topo", getTopoRuleHandler).Methods(http.MethodGet)
	r.HandleFunc("/ruleset/export", exportHandler).Methods(http.MethodPost)
	r.HandleFunc("/ruleset/import", importHandler).Methods(http.MethodPost)
	r.HandleFunc("/config/uploads", fileUploadHandler).Methods(http.MethodPost, http.MethodGet)
	r.HandleFunc("/config/uploads/{name}", fileDeleteHandler).Methods(http.MethodDelete)
	r.HandleFunc("/data/export", configurationExportHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/data/import", configurationImportHandler).Methods(http.MethodPost)
	r.HandleFunc("/data/import/status", configurationStatusHandler).Methods(http.MethodGet)
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

	buf1 := bytes.NewBuffer([]byte(`{"sql":"CREATE stream alert() WITH (DATASOURCE=\"0\", TYPE=\"mqtt\")"}`))
	req1, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/streams", buf1)
	w1 := httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)

	// create rule with trigger false
	ruleJson := `{"id": "rule1","triggered": false,"sql": "select * from alert","actions": [{"log": {}}]}`

	buf2 := bytes.NewBuffer([]byte(ruleJson))
	req2, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/rules", buf2)
	w2 := httptest.NewRecorder()
	suite.r.ServeHTTP(w2, req2)

	// get all rules
	req3, _ := http.NewRequest(http.MethodGet, "http://localhost:8080/rules", bytes.NewBufferString("any"))
	w3 := httptest.NewRecorder()
	suite.r.ServeHTTP(w3, req3)

	_, _ = io.ReadAll(w3.Result().Body)

	// update rule, will set rule to triggered
	ruleJson = `{"id": "rule1","triggered": false,"sql": "select * from alert","actions": [{"nop": {}}]}`

	buf2 = bytes.NewBuffer([]byte(ruleJson))
	req1, _ = http.NewRequest(http.MethodPut, "http://localhost:8080/rules/rule1", buf2)
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)

	assert.Equal(suite.T(), http.StatusOK, w1.Code)

	// update wron rule
	ruleJson = `{"id": "rule1","sql": "select * from alert1","actions": [{"nop": {}}]}`

	buf2 = bytes.NewBuffer([]byte(ruleJson))
	req1, _ = http.NewRequest(http.MethodPut, "http://localhost:8080/rules/rule1", buf2)
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)

	assert.Equal(suite.T(), http.StatusBadRequest, w1.Code)

	// get rule
	req1, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/rules/rule1", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)

	returnVal, _ := io.ReadAll(w1.Result().Body)
	expect := `{"id": "rule1","triggered": false,"sql": "select * from alert","actions": [{"nop": {}}]}`
	assert.Equal(suite.T(), expect, string(returnVal))

	// get rule status
	req1, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/rules/rule1/status", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body) //nolint

	// get rule topo
	req1, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/rules/rule1/topo", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body)

	expect = `{"sources":["source_alert"],"edges":{"op_2_project":["sink_nop_0"],"source_alert":["op_2_project"]}}`
	assert.Equal(suite.T(), expect, string(returnVal))

	// start rule
	req1, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/rules/rule1/start", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body)

	expect = `Rule rule1 was started`
	assert.Equal(suite.T(), expect, string(returnVal))

	// stop rule
	req1, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/rules/rule1/stop", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body)

	expect = `Rule rule1 was stopped.`
	assert.Equal(suite.T(), expect, string(returnVal))

	// restart rule
	req1, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/rules/rule1/restart", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body)

	expect = `Rule rule1 was restarted`
	assert.Equal(suite.T(), expect, string(returnVal))

	// delete rule
	req1, _ = http.NewRequest(http.MethodDelete, "http://localhost:8080/rules/rule1", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	suite.r.ServeHTTP(w1, req1)

	// drop stream
	req, _ := http.NewRequest(http.MethodDelete, "http://localhost:8080/streams/alert", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
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
	returnVal, _ := io.ReadAll(w1.Result().Body)
	assert.Equal(suite.T(), http.StatusOK, w1.Code)
	exp := "{\"streams\":{\"demo\":\"CREATE STREAM demo () WITH (DATASOURCE=\\\"users\\\", FORMAT=\\\"JSON\\\")\",\"plugin\":\"\\n              CREATE STREAM plugin\\n              ()\\n              WITH (FORMAT=\\\"json\\\", CONF_KEY=\\\"default\\\", TYPE=\\\"mqtt\\\", SHARED=\\\"false\\\", );\\n          \"},\"tables\":{},\"rules\":{\"myRule\":\"{\\\"id\\\": \\\"myRule\\\",\\\"sql\\\": \\\"SELECT * FROM demo\\\",\\\"actions\\\": [{\\\"log\\\": {}}]}\",\"rule1\":\"{\\\"id\\\":\\\"rule1\\\",\\\"name\\\":\\\"\\\",\\\"sql\\\":\\\"select name from plugin\\\",\\\"actions\\\":[{\\\"log\\\":{\\\"runAsync\\\":false,\\\"omitIfEmpty\\\":false,\\\"sendSingle\\\":true,\\\"bufferLength\\\":1024,\\\"enableCache\\\":false,\\\"format\\\":\\\"json\\\"}}],\\\"options\\\":{\\\"restartStrategy\\\":{}}}\"}}"
	assert.Equal(suite.T(), exp, string(returnVal))
}

func (suite *RestTestSuite) Test_dataImport() {
	file := "rpctest/data/import_configuration.json"
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
	returnVal, _ := io.ReadAll(w.Result().Body)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
	exp := "{\"streams\":{\"demo\":\"CREATE STREAM demo () WITH (DATASOURCE=\\\"users\\\", FORMAT=\\\"JSON\\\")\"},\"tables\":{},\"rules\":{\"myRule\":\"{\\\"id\\\": \\\"myRule\\\",\\\"sql\\\": \\\"SELECT * FROM demo\\\",\\\"actions\\\": [{\\\"log\\\": {}}]}\"},\"nativePlugins\":{},\"portablePlugins\":{},\"sourceConfig\":{},\"sinkConfig\":{},\"connectionConfig\":{},\"Service\":{},\"Schema\":{}}"
	assert.Equal(suite.T(), exp, string(returnVal))

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

	req, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/config/uploads", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)

	req, _ = http.NewRequest(http.MethodDelete, "http://localhost:8080/config/uploads/test.txt", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
	os.Remove(uploadDir)
}

func TestRestTestSuite(t *testing.T) {
	suite.Run(t, new(RestTestSuite))
}
