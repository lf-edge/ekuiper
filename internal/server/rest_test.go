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
	"github.com/gorilla/mux"
	"github.com/lf-edge/ekuiper/internal/processor"
	"github.com/lf-edge/ekuiper/internal/testx"
	"github.com/lf-edge/ekuiper/internal/topo/rule"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func init() {
	testx.InitEnv()
	streamProcessor = processor.NewStreamProcessor()
	ruleProcessor = processor.NewRuleProcessor()
	rulesetProcessor = processor.NewRulesetProcessor(ruleProcessor, streamProcessor)
	registry = &RuleRegistry{internal: make(map[string]*rule.RuleState)}

}

func Test_rootHandler(t *testing.T) {
	r := mux.NewRouter()
	r.HandleFunc("/", rootHandler).Methods(http.MethodGet, http.MethodPost)

	req, _ := http.NewRequest(http.MethodPost, "/", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	resp := w.Result()

	if !reflect.DeepEqual(resp.StatusCode, 200) {
		t.Errorf("Expect\t %v\nBut got\t%v", 200, resp.StatusCode)
	}
}

func Test_sourcesManageHandler(t *testing.T) {

	req, _ := http.NewRequest(http.MethodGet, "/", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()

	sourcesManageHandler(w, req, ast.TypeStream)

	if !reflect.DeepEqual(w.Result().StatusCode, 200) {
		t.Errorf("Expect\t %v\nBut got\t%v", 200, w.Result().StatusCode)
	}

	//get scan table
	req, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/streams?kind=scan", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()

	sourcesManageHandler(w, req, ast.TypeTable)

	if !reflect.DeepEqual(w.Result().StatusCode, 200) {
		t.Errorf("Expect\t %v\nBut got\t%v", 200, w.Result().StatusCode)
	}

	//get lookup table
	req, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/streams?kind=lookup", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()

	sourcesManageHandler(w, req, ast.TypeTable)

	if !reflect.DeepEqual(w.Result().StatusCode, 200) {
		t.Errorf("Expect\t %v\nBut got\t%v", 200, w.Result().StatusCode)
	}

	//create table
	buf := bytes.NewBuffer([]byte(` {"sql":"CREATE TABLE alertTable() WITH (DATASOURCE=\"0\", TYPE=\"redis\", KIND=\"lookup\")"}`))
	req, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/streams?kind=lookup", buf)
	w = httptest.NewRecorder()

	sourcesManageHandler(w, req, ast.TypeTable)

	var returnVal []byte
	returnVal, _ = io.ReadAll(w.Result().Body)
	fmt.Printf("returnVal %s\n", string(returnVal))

	//create stream
	buf = bytes.NewBuffer([]byte(`{"sql":"CREATE stream alert() WITH (DATASOURCE=\"0\", TYPE=\"mqtt\")"}`))
	req, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/streams", buf)
	w = httptest.NewRecorder()

	sourcesManageHandler(w, req, ast.TypeStream)

	returnVal, _ = io.ReadAll(w.Result().Body)

	fmt.Printf("returnVal %s\n", string(returnVal))

	//get stream
	r := mux.NewRouter()
	r.HandleFunc("/streams/{name}", streamHandler).Methods(http.MethodGet, http.MethodDelete, http.MethodPut)

	req, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/streams/alert", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)

	expect := []byte(`{"Name":"alert","Options":{"datasource":"0","type":"mqtt"},"Statement":null,"StreamFields":null,"StreamType":0}`)
	exp := map[string]interface{}{}
	_ = json.NewDecoder(bytes.NewBuffer(expect)).Decode(&exp)

	res := map[string]interface{}{}
	_ = json.NewDecoder(w.Result().Body).Decode(&res)
	if !reflect.DeepEqual(exp, res) {
		t.Errorf("Expect\t%v\nBut got\t%v", exp, res)
	}

	//get table
	r = mux.NewRouter()
	r.HandleFunc("/tables/{name}", tableHandler).Methods(http.MethodGet, http.MethodDelete, http.MethodPut)

	req, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/tables/alertTable", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)

	expect = []byte(`{"Name":"alertTable","Options":{"datasource":"0","type":"redis","kind":"lookup"},"Statement":null,"StreamFields":null,"StreamType":1}`)
	exp = map[string]interface{}{}
	_ = json.NewDecoder(bytes.NewBuffer(expect)).Decode(&exp)

	res = map[string]interface{}{}
	_ = json.NewDecoder(w.Result().Body).Decode(&res)

	if !reflect.DeepEqual(exp, res) {
		t.Errorf("Expect\t%v\nBut got\t%v", exp, res)
	}

	//put table
	buf = bytes.NewBuffer([]byte(` {"sql":"CREATE TABLE alertTable() WITH (DATASOURCE=\"0\", TYPE=\"memory\", KEY=\"id\", KIND=\"lookup\")"}`))
	r = mux.NewRouter()
	r.HandleFunc("/tables/{name}", tableHandler).Methods(http.MethodGet, http.MethodDelete, http.MethodPut)

	req, _ = http.NewRequest(http.MethodPut, "http://localhost:8080/tables/alertTable", buf)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if !reflect.DeepEqual(w.Result().StatusCode, 200) {
		t.Errorf("Expect\t%v\nBut got\t%v", 200, w.Result().StatusCode)
	}

	//put stream
	buf = bytes.NewBuffer([]byte(`{"sql":"CREATE stream alert() WITH (DATASOURCE=\"0\", TYPE=\"httppull\")"}`))
	r = mux.NewRouter()
	r.HandleFunc("/streams/{name}", streamHandler).Methods(http.MethodGet, http.MethodDelete, http.MethodPut)

	req, _ = http.NewRequest(http.MethodPut, "http://localhost:8080/streams/alert", buf)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if !reflect.DeepEqual(w.Result().StatusCode, 200) {
		t.Errorf("Expect\t%v\nBut got\t%v", 200, w.Result().StatusCode)
	}

	//drop table
	r = mux.NewRouter()
	r.HandleFunc("/tables/{name}", tableHandler).Methods(http.MethodGet, http.MethodDelete, http.MethodPut)

	req, _ = http.NewRequest(http.MethodDelete, "http://localhost:8080/tables/alertTable", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if !reflect.DeepEqual(w.Result().StatusCode, 200) {
		t.Errorf("Expect\t%v\nBut got\t%v", 200, w.Result().StatusCode)
	}

	//drop stream
	r = mux.NewRouter()
	r.HandleFunc("/streams/{name}", streamHandler).Methods(http.MethodGet, http.MethodDelete, http.MethodPut)

	req, _ = http.NewRequest(http.MethodDelete, "http://localhost:8080/streams/alert", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if !reflect.DeepEqual(w.Result().StatusCode, 200) {
		t.Errorf("Expect\t%v\nBut got\t%v", 200, w.Result().StatusCode)
	}
}

func Test_rulesManageHandler(t *testing.T) {
	//Start rules
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
			//err = server.StartRule(rule, &reply)
			reply = recoverRule(rule)
			if 0 != len(reply) {
				logger.Info(reply)
			}
		}
	}

	r := mux.NewRouter()
	r.HandleFunc("/rules", rulesHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/streams", streamsHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/rules/{name}", ruleHandler).Methods(http.MethodDelete, http.MethodGet, http.MethodPut)
	r.HandleFunc("/streams/{name}", streamHandler).Methods(http.MethodGet, http.MethodDelete, http.MethodPut)
	r.HandleFunc("/rules/{name}/status", getStatusRuleHandler).Methods(http.MethodGet)
	r.HandleFunc("/rules/{name}/topo", getTopoRuleHandler).Methods(http.MethodGet)
	r.HandleFunc("/rules/{name}/start", startRuleHandler).Methods(http.MethodPost)
	r.HandleFunc("/rules/{name}/stop", stopRuleHandler).Methods(http.MethodPost)
	r.HandleFunc("/rules/{name}/restart", restartRuleHandler).Methods(http.MethodPost)

	buf1 := bytes.NewBuffer([]byte(`{"sql":"CREATE stream alert() WITH (DATASOURCE=\"0\", TYPE=\"mqtt\")"}`))
	req1, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/streams", buf1)
	w1 := httptest.NewRecorder()
	r.ServeHTTP(w1, req1)

	//create rule with trigger false
	ruleJson := `{"id": "rule1","triggered": false,"sql": "select * from alert","actions": [{"log": {}}]}`

	buf2 := bytes.NewBuffer([]byte(ruleJson))
	req2, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/rules", buf2)
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, req2)

	// get all rules
	req3, _ := http.NewRequest(http.MethodGet, "http://localhost:8080/rules", bytes.NewBufferString("any"))
	w3 := httptest.NewRecorder()
	r.ServeHTTP(w3, req3)

	returnVal, _ := io.ReadAll(w3.Result().Body)

	expect := `[{"id":"rule1","name":"rule1","status":"Stopped: no context found."}]`

	if string(returnVal) != expect {
		t.Errorf("Expect\t%v\nBut got\t%v", expect, string(returnVal))
	}

	//update rule, will set rule to triggered
	ruleJson = `{"id": "rule1","triggered": false,"sql": "select * from alert","actions": [{"nop": {}}]}`

	buf2 = bytes.NewBuffer([]byte(ruleJson))
	req1, _ = http.NewRequest(http.MethodPut, "http://localhost:8080/rules/rule1", buf2)
	w1 = httptest.NewRecorder()
	r.ServeHTTP(w1, req1)

	if w1.Result().StatusCode != 200 {
		t.Errorf("Expect\t%v\nBut got\t%v", 200, w1.Result().StatusCode)
	}

	//get rule
	req1, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/rules/rule1", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	r.ServeHTTP(w1, req1)

	returnVal, _ = io.ReadAll(w1.Result().Body)
	expect = `{"id": "rule1","triggered": false,"sql": "select * from alert","actions": [{"nop": {}}]}`
	if string(returnVal) != expect {
		t.Errorf("Expect\t%v\nBut got\t%v", expect, string(returnVal))
	}

	//get rule status
	req1, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/rules/rule1/status", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body)

	expect = `"status": "running"`
	if !strings.Contains(string(returnVal), expect) {
		t.Errorf("Expect\t%v\nBut got\t%v", expect, string(returnVal))
	}

	//get rule topo
	req1, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/rules/rule1/topo", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body)

	expect = `{"sources":["source_alert"],"edges":{"op_2_project":["sink_nop_0"],"source_alert":["op_2_project"]}}`
	if string(returnVal) != expect {
		t.Errorf("Expect\t%v\nBut got\t%v", expect, string(returnVal))
	}

	//start rule
	req1, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/rules/rule1/start", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body)

	expect = `Rule rule1 was started`
	if string(returnVal) != expect {
		t.Errorf("Expect\t%v\nBut got\t%v", expect, string(returnVal))
	}

	//stop rule
	req1, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/rules/rule1/stop", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body)

	expect = `Rule rule1 was stopped.`
	if string(returnVal) != expect {
		t.Errorf("Expect\t%v\nBut got\t%v", expect, string(returnVal))
	}

	//restart rule
	req1, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/rules/rule1/restart", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	r.ServeHTTP(w1, req1)
	returnVal, _ = io.ReadAll(w1.Result().Body)

	expect = `Rule rule1 was restarted`
	if string(returnVal) != expect {
		t.Errorf("Expect\t%v\nBut got\t%v", expect, string(returnVal))
	}

	//delete rule
	req1, _ = http.NewRequest(http.MethodDelete, "http://localhost:8080/rules/rule1", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	r.ServeHTTP(w1, req1)

	//drop stream
	req, _ := http.NewRequest(http.MethodDelete, "http://localhost:8080/streams/alert", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
}

func Test_ruleSetImport(t *testing.T) {
	r := mux.NewRouter()
	r.HandleFunc("/ruleset/import", importHandler).Methods(http.MethodPost)
	r.HandleFunc("/ruleset/export", exportHandler).Methods(http.MethodPost)

	ruleJson := `{"streams":{"plugin":"\n              CREATE STREAM plugin\n              ()\n              WITH (FORMAT=\"json\", CONF_KEY=\"default\", TYPE=\"mqtt\", SHARED=\"false\", );\n          "},"tables":{},"rules":{"rule1":"{\"id\":\"rule1\",\"name\":\"\",\"sql\":\"select name from plugin\",\"actions\":[{\"log\":{\"runAsync\":false,\"omitIfEmpty\":false,\"sendSingle\":true,\"bufferLength\":1024,\"enableCache\":false,\"format\":\"json\"}}],\"options\":{\"restartStrategy\":{}}}"}}`

	ruleSetJson := map[string]string{
		"content": ruleJson,
	}
	buf, _ := json.Marshal(ruleSetJson)
	buf2 := bytes.NewBuffer(buf)
	req1, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/ruleset/import", buf2)
	w1 := httptest.NewRecorder()
	r.ServeHTTP(w1, req1)

	req1, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/ruleset/export", bytes.NewBufferString("any"))
	w1 = httptest.NewRecorder()
	r.ServeHTTP(w1, req1)
	returnVal, _ := io.ReadAll(w1.Result().Body)
	fmt.Printf("########## %s\n", string(returnVal))
}
