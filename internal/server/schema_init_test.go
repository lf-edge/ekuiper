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
	"errors"
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

	request, err = http.NewRequest(http.MethodPut, "/schemas/protobuf/valid_name/upload", strings.NewReader("not multipart"))
	require.NoError(suite.T(), err)
	request.Header.Set("Content-Type", "multipart/form-data")
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, request)
	suite.Equal(http.StatusBadRequest, w.Code)

	request, err = http.NewRequest(http.MethodPut, "/schemas/protobuf/valid_name/upload", strings.NewReader("not multipart"))
	require.NoError(suite.T(), err)
	request.Header.Set("Content-Type", "%%%")
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, request)
	suite.Equal(http.StatusUnsupportedMediaType, w.Code)

	request = newSchemaMultipartRequest(suite.T(), "/schemas/protobuf/valid_name/upload", func(writer *multipart.Writer) {
		require.NoError(suite.T(), writer.WriteField("version", "1"))
	})
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, request)
	suite.Equal(http.StatusBadRequest, w.Code)

	request = newSchemaUploadRequest(suite.T(), "/schemas/unsupported/valid_name/upload", "schema.data", []byte("schema"), "")
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, request)
	suite.Equal(http.StatusBadRequest, w.Code)

	const name = "upload_older_version_test"
	defer func() { _ = schema.DeleteSchema("protobuf", name) }()
	request = newSchemaUploadRequest(suite.T(), "/schemas/protobuf/"+name+"/upload", "schema.proto", []byte("message Newer {}"), "2")
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, request)
	suite.Equal(http.StatusCreated, w.Code)
	request = newSchemaUploadRequest(suite.T(), "/schemas/protobuf/"+name+"/upload", "schema.proto", []byte("message Older {}"), "1")
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, request)
	suite.Equal(http.StatusBadRequest, w.Code)
}

func newSchemaUploadRequest(t *testing.T, target, filename string, content []byte, version string) *http.Request {
	t.Helper()
	return newSchemaMultipartRequest(t, target, func(writer *multipart.Writer) {
		part, err := writer.CreateFormFile("file", filename)
		require.NoError(t, err)
		_, err = part.Write(content)
		require.NoError(t, err)
		if version != "" {
			require.NoError(t, writer.WriteField("version", version))
		}
	})
}

func newSchemaMultipartRequest(t *testing.T, target string, writeParts func(*multipart.Writer)) *http.Request {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	writeParts(writer)
	require.NoError(t, writer.Close())
	request, err := http.NewRequest(http.MethodPut, target, &body)
	require.NoError(t, err)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	return request
}

func TestReceiveSchemaUpload(t *testing.T) {
	t.Run("accepts version before file and ignores unknown fields", func(t *testing.T) {
		request := newSchemaMultipartRequest(t, "/", func(writer *multipart.Writer) {
			require.NoError(t, writer.WriteField("version", "v1"))
			require.NoError(t, writer.WriteField("description", "ignored"))
			part, err := writer.CreateFormFile("file", "schema.proto")
			require.NoError(t, err)
			_, err = part.Write([]byte("message Test {}"))
			require.NoError(t, err)
		})
		reader, err := request.MultipartReader()
		require.NoError(t, err)
		upload, err := receiveSchemaUpload(reader)
		require.NoError(t, err)
		defer upload.cleanup()
		require.Equal(t, "v1", upload.version)
		content, err := os.ReadFile(upload.path)
		require.NoError(t, err)
		require.Equal(t, "message Test {}", string(content))
	})

	tests := []struct {
		name      string
		expected  string
		writeBody func(*testing.T, *multipart.Writer)
	}{
		{
			name:     "missing file",
			expected: "file field is required",
			writeBody: func(t *testing.T, writer *multipart.Writer) {
				require.NoError(t, writer.WriteField("version", "1"))
			},
		},
		{
			name:     "duplicate file",
			expected: "file field must appear exactly once",
			writeBody: func(t *testing.T, writer *multipart.Writer) {
				_, err := writer.CreateFormFile("file", "first.proto")
				require.NoError(t, err)
				_, err = writer.CreateFormFile("file", "second.proto")
				require.NoError(t, err)
			},
		},
		{
			name:     "duplicate version",
			expected: "version field must not be repeated",
			writeBody: func(t *testing.T, writer *multipart.Writer) {
				require.NoError(t, writer.WriteField("version", "1"))
				require.NoError(t, writer.WriteField("version", "2"))
			},
		},
		{
			name:     "version too large",
			expected: "version field is too large",
			writeBody: func(t *testing.T, writer *multipart.Writer) {
				require.NoError(t, writer.WriteField("version", strings.Repeat("v", maxSchemaUploadFieldSize+1)))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := newSchemaMultipartRequest(t, "/", func(writer *multipart.Writer) {
				tt.writeBody(t, writer)
			})
			reader, err := request.MultipartReader()
			require.NoError(t, err)
			upload, err := receiveSchemaUpload(reader)
			require.Nil(t, upload)
			require.EqualError(t, err, tt.expected)
		})
	}

	t.Run("malformed multipart", func(t *testing.T) {
		reader := multipart.NewReader(strings.NewReader("--boundary\r\nmalformed"), "boundary")
		upload, err := receiveSchemaUpload(reader)
		require.Nil(t, upload)
		require.Error(t, err)
	})
}

type failingResponseWriter struct {
	header http.Header
}

func (w *failingResponseWriter) Header() http.Header {
	return w.header
}

func (*failingResponseWriter) Write([]byte) (int, error) {
	return 0, errors.New("injected response write failure")
}

func (*failingResponseWriter) WriteHeader(int) {}

func TestHandleErrorWithStatusWriteFailure(t *testing.T) {
	w := &failingResponseWriter{header: make(http.Header)}
	handleErrorWithStatus(w, errors.New("request failed"), "schema upload error", http.StatusBadRequest, logger)
	require.Equal(t, ContentTypeJSON, w.Header().Get(ContentType))
}

func TestSchemaTestSuite(t *testing.T) {
	suite.Run(t, new(SchemaTestSuite))
}
