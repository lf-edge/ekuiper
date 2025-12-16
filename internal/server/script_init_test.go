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

//go:build script || full

package server

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/suite"
)

type ScriptTestSuite struct {
	suite.Suite
	sc scriptComp
	r  *mux.Router
}

func (suite *ScriptTestSuite) SetupTest() {
	suite.sc = scriptComp{}
	suite.r = mux.NewRouter()
	suite.sc.register()
	suite.sc.rest(suite.r)
}

func (suite *ScriptTestSuite) TestAPI() {
	// create correct
	body := `{"id": "area", "description": "function to calculate area", "script": "function area(x, y) { return x * y; }", "is_agg": false}`
	req, _ := http.NewRequest(http.MethodPost, "/udf/javascript", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	suite.Equal(http.StatusCreated, w.Code)

	// duplicate error
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	suite.Equal(http.StatusBadRequest, w.Code)

	// invalid json
	invalid := `{"id": "area, "description": "function to calculate area", "script": "function area(x, y) { return x * y; }", "is_agg": false}`
	req, _ = http.NewRequest(http.MethodPost, "/udf/javascript", bytes.NewBufferString(invalid))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	suite.Equal(http.StatusBadRequest, w.Code)

	// invalid id
	invalidId := `{"id": "inv/alid", "description": "function to calculate area", "script": "function area(x, y) { return x * y; }", "is_agg": false}`
	req, _ = http.NewRequest(http.MethodPost, "/udf/javascript", bytes.NewBufferString(invalidId))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	suite.Equal(http.StatusBadRequest, w.Code)

	// get correct
	req, _ = http.NewRequest(http.MethodGet, "/udf/javascript/area", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	suite.Equal(http.StatusOK, w.Code)
	exp := "{\"id\":\"area\",\"description\":\"function to calculate area\",\"script\":\"function area(x, y) { return x * y; }\",\"isAgg\":false}"
	suite.Equal(exp, w.Body.String())

	// get inexist
	req, _ = http.NewRequest(http.MethodGet, "/udf/javascript/area1", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	suite.Equal(http.StatusBadRequest, w.Code)

	// list
	req, _ = http.NewRequest(http.MethodGet, "/udf/javascript", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	suite.Equal(http.StatusOK, w.Code)
	suite.Equal(`["area"]`, w.Body.String())

	// update
	body2 := `{"id": "area", "description": "function to calculate area", "script": "function area(x, y) { return x + y; }", "is_agg": false}`
	req, _ = http.NewRequest(http.MethodPut, "/udf/javascript/area", bytes.NewBufferString(body2))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	suite.Equal(http.StatusOK, w.Code)

	// update invalid json
	req, _ = http.NewRequest(http.MethodPut, "/udf/javascript/area", bytes.NewBufferString(invalid))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	suite.Equal(http.StatusBadRequest, w.Code)

	// delete
	req, _ = http.NewRequest(http.MethodDelete, "/udf/javascript/area", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	suite.Equal(http.StatusOK, w.Code)

	// update in-exist, support upsert
	req, _ = http.NewRequest(http.MethodPut, "/udf/javascript/area", bytes.NewBufferString(body))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	suite.Equal(http.StatusOK, w.Code)

	// delete
	req, _ = http.NewRequest(http.MethodDelete, "/udf/javascript/area", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	suite.Equal(http.StatusOK, w.Code)
}

func TestScriptTestSuite(t *testing.T) {
	suite.Run(t, new(ScriptTestSuite))
}
