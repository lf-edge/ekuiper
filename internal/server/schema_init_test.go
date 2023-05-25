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
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/suite"
	"net/http"
	"net/http/httptest"
	"testing"
)

type SchemaTestSuite struct {
	suite.Suite
	sc schemaComp
	r  *mux.Router
}

func (suite *SchemaTestSuite) SetupTest() {
	suite.sc = schemaComp{}
	suite.r = mux.NewRouter()
	suite.sc.register()
	suite.sc.rest(suite.r)
}

func (suite *SchemaTestSuite) TestSchema() {
	proto := `{"name": "test", "Content": "message ListOfDoubles {repeated double doubles=1;}"}`
	req, _ := http.NewRequest(http.MethodPost, "/schemas/protobuf", bytes.NewBufferString(proto))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	suite.Equal(http.StatusCreated, w.Code)

	req, _ = http.NewRequest(http.MethodGet, "/schemas/protobuf", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	suite.Equal(http.StatusOK, w.Code)

	req, _ = http.NewRequest(http.MethodGet, "/schemas/protobuf/test", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	suite.Equal(http.StatusOK, w.Code)

	req, _ = http.NewRequest(http.MethodDelete, "/schemas/protobuf/test", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	suite.Equal(http.StatusOK, w.Code)

}

func TestSchemaTestSuite(t *testing.T) {
	suite.Run(t, new(SchemaTestSuite))
}
