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
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/schema"
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

	// test invalid id
	protoInvalid := `{"name": "test.invalid", "Content": "message ListOfDoubles {repeated double doubles=1;}"}`
	req, _ = http.NewRequest(http.MethodPost, "/schemas/protobuf", bytes.NewBufferString(protoInvalid))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	suite.Equal(http.StatusBadRequest, w.Code)

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

func (suite *SchemaTestSuite) TestSchemaUploadUpsert() {
	const name = "upload_upsert_test"
	defer func() {
		_ = schema.DeleteSchema("protobuf", name)
	}()

	request := newSchemaUploadRequest(suite.T(), "/schemas/protobuf/"+name+"/upload", "first.proto", []byte("message First { required string name = 1; }"), "1")
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, request)
	suite.Equal(http.StatusCreated, w.Code)
	suite.Equal("application/json", w.Header().Get("Content-Type"))
	suite.Equal("/schemas/protobuf/"+name, w.Header().Get("Location"))
	suite.JSONEq(`{"type":"protobuf","name":"`+name+`"}`, w.Body.String())

	request = newSchemaUploadRequest(suite.T(), "/schemas/protobuf/"+name+"/upload", "second.proto", []byte("message Second { required int32 id = 1; }"), "1")
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, request)
	suite.Equal(http.StatusOK, w.Code)

	gotten, err := schema.GetSchema("protobuf", name)
	require.NoError(suite.T(), err)
	suite.Equal("message Second { required int32 id = 1; }", gotten.Content)

	key, script := schema.GetSchemaInstallScript("protobuf_" + name)
	suite.Equal("protobuf_"+name, key)
	suite.NotContains(script, ".upload-")
	stored := &schema.Info{}
	require.NoError(suite.T(), json.Unmarshal([]byte(script), stored))
	suite.Equal(gotten.FilePath, strings.TrimPrefix(stored.FilePath, "file://"))

	dataDir, err := conf.GetDataLoc()
	require.NoError(suite.T(), err)
	tempFiles, err := filepath.Glob(filepath.Join(dataDir, "uploads", "schemas", ".upload-*"))
	require.NoError(suite.T(), err)
	suite.Empty(tempFiles)
}

func (suite *SchemaTestSuite) TestSchemaUploadZip() {
	const name = "test1"
	_ = schema.DeleteSchema("protobuf", name)
	defer func() {
		_ = schema.DeleteSchema("protobuf", name)
	}()
	content, err := os.ReadFile("../schema/test/test1.zip")
	require.NoError(suite.T(), err)
	request := newSchemaUploadRequest(suite.T(), "/schemas/protobuf/"+name+"/upload", "schema.zip", content, "")
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, request)
	suite.Equal(http.StatusCreated, w.Code)
	gotten, err := schema.GetSchema("protobuf", name)
	require.NoError(suite.T(), err)
	suite.Contains(gotten.Content, "message Person")
}

func (suite *SchemaTestSuite) TestSchemaUploadErrors() {
	request, err := http.NewRequest(http.MethodPut, "/schemas/protobuf/bad.name/upload", strings.NewReader("not multipart"))
	require.NoError(suite.T(), err)
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, request)
	suite.Equal(http.StatusBadRequest, w.Code)

	request, err = http.NewRequest(http.MethodPut, "/schemas/protobuf/valid_name/upload", strings.NewReader("not multipart"))
	require.NoError(suite.T(), err)
	request.Header.Set("Content-Type", "application/octet-stream")
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, request)
	suite.Equal(http.StatusUnsupportedMediaType, w.Code)
}

func newSchemaUploadRequest(t *testing.T, target, filename string, content []byte, version string) *http.Request {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", filename)
	require.NoError(t, err)
	_, err = part.Write(content)
	require.NoError(t, err)
	if version != "" {
		require.NoError(t, writer.WriteField("version", version))
	}
	require.NoError(t, writer.Close())
	request, err := http.NewRequest(http.MethodPut, target, &body)
	require.NoError(t, err)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	return request
}

func TestSchemaTestSuite(t *testing.T) {
	suite.Run(t, new(SchemaTestSuite))
}
