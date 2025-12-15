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

//go:build !core

package server

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/lf-edge/ekuiper/v2/internal/plugin/js"
	"github.com/lf-edge/ekuiper/v2/internal/plugin/native"
	"github.com/lf-edge/ekuiper/v2/internal/service"
    "github.com/gorilla/mux"
)

type RestValidationTestSuite struct {
	suite.Suite
	r *mux.Router
}

func (suite *RestValidationTestSuite) TestStreamValidation() {
	// streams/{name}
	req, _ := http.NewRequest(http.MethodGet, "http://localhost:8080/streams/invalid.name", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "invalid characters")

	req, _ = http.NewRequest(http.MethodDelete, "http://localhost:8080/streams/invalid.name", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "invalid characters")

	req, _ = http.NewRequest(http.MethodPut, "http://localhost:8080/streams/invalid.name", bytes.NewBufferString(`{"sql":"CREATE stream alert() WITH (DATASOURCE=\"0\", TYPE=\"mqtt\")"}`))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "invalid characters")
}

func (suite *RestValidationTestSuite) TestTableValidation() {
	// tables/{name}
	req, _ := http.NewRequest(http.MethodGet, "http://localhost:8080/tables/invalid.name", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "invalid characters")

	req, _ = http.NewRequest(http.MethodDelete, "http://localhost:8080/tables/invalid.name", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "invalid characters")

	req, _ = http.NewRequest(http.MethodPut, "http://localhost:8080/tables/invalid.name", bytes.NewBufferString(`{"sql":"CREATE TABLE alertTable() WITH (DATASOURCE=\"0\", TYPE=\"memory\", KEY=\"id\", KIND=\"lookup\")"}`))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "invalid characters")
}

func (suite *RestValidationTestSuite) TestRuleValidation() {
	// rules/{name}
	req, _ := http.NewRequest(http.MethodGet, "http://localhost:8080/rules/invalid.name", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "invalid characters")

	req, _ = http.NewRequest(http.MethodDelete, "http://localhost:8080/rules/invalid.name", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "invalid characters")

	req, _ = http.NewRequest(http.MethodPut, "http://localhost:8080/rules/invalid.name", bytes.NewBufferString("{}"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "invalid characters")

	// rules/{name}/status
	req, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/rules/invalid.name/status", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "invalid characters")

	// rules/{name}/start
	req, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/rules/invalid.name/start", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "invalid characters")

	// rules/{name}/stop
	req, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/rules/invalid.name/stop", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "invalid characters")

	// rules/{name}/restart
	req, _ = http.NewRequest(http.MethodPost, "http://localhost:8080/rules/invalid.name/restart", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "invalid characters")

	// rules/{name}/topo
	req, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/rules/invalid.name/topo", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "invalid characters")
}

func (suite *RestValidationTestSuite) SetupTest() {
	suite.r = mux.NewRouter()
	var err error
	serviceManager, err = service.InitManager()
	if err != nil {
		panic(err)
	}
	nativeManager, err = native.InitManager()
	if err != nil {
		panic(err)
	}
	js.InitManager()

	// Register service handlers
	suite.r.HandleFunc("/services/{name}", serviceHandler).Methods(http.MethodDelete, http.MethodGet, http.MethodPut)

	// Register plugin handlers
	suite.r.HandleFunc("/plugins/sources/{name}", sourceHandler).Methods(http.MethodDelete, http.MethodGet, http.MethodPut)
	suite.r.HandleFunc("/plugins/sinks/{name}", sinkHandler).Methods(http.MethodDelete, http.MethodGet, http.MethodPut)
	suite.r.HandleFunc("/plugins/functions/{name}", functionHandler).Methods(http.MethodDelete, http.MethodGet, http.MethodPut)

	// Register script handlers
	suite.r.HandleFunc("/udf/javascript/{id}", jsfuncHandler).Methods(http.MethodGet, http.MethodDelete, http.MethodPut)
}

func (suite *RestValidationTestSuite) TestServiceValidation() {
	// services/{name}
	req, _ := http.NewRequest(http.MethodGet, "http://localhost:8080/services/invalid.name", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "invalid characters")

	req, _ = http.NewRequest(http.MethodDelete, "http://localhost:8080/services/invalid.name", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "invalid characters")

	req, _ = http.NewRequest(http.MethodPut, "http://localhost:8080/services/invalid.name", bytes.NewBufferString("{}"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "invalid characters")
}

func (suite *RestValidationTestSuite) TestPluginValidation() {
	// plugins/sources/{name}
	req, _ := http.NewRequest(http.MethodGet, "http://localhost:8080/plugins/sources/invalid.name", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "invalid characters")

	// plugins/sinks/{name}
	req, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/plugins/sinks/invalid.name", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "invalid characters")

	// plugins/functions/{name}
	req, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/plugins/functions/invalid.name", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "invalid characters")
}

func (suite *RestValidationTestSuite) TestScriptValidation() {
	// udf/javascript/{id}
	req, _ := http.NewRequest(http.MethodGet, "http://localhost:8080/udf/javascript/invalid.id", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "invalid characters")

	req, _ = http.NewRequest(http.MethodDelete, "http://localhost:8080/udf/javascript/invalid.id", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "invalid characters")

	req, _ = http.NewRequest(http.MethodPut, "http://localhost:8080/udf/javascript/invalid.id", bytes.NewBufferString("{}"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "invalid characters")
}

func TestRestValidationTestSuite(t *testing.T) {
	suite.Run(t, new(RestValidationTestSuite))
}
