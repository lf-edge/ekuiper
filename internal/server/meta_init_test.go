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
	"path"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/lf-edge/ekuiper/v2/extensions/impl/sql"
	"github.com/lf-edge/ekuiper/v2/extensions/impl/sql/client"
	"github.com/lf-edge/ekuiper/v2/extensions/impl/sql/testx"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/meta"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

type MetaTestSuite struct {
	suite.Suite
	m metaComp
	r *mux.Router
}

func (suite *MetaTestSuite) SetupTest() {
	suite.m = metaComp{}
	suite.r = mux.NewRouter()
	suite.m.rest(suite.r)
	meta.InitYamlConfigManager()
	confDir, err := conf.GetConfLoc()
	if err != nil {
		fmt.Println(err)
	}
	if err := meta.ReadSinkMetaFile(path.Join(confDir, "sinks", "mqtt.json"), true); nil != err {
		fmt.Println(err)
	}
	if err := meta.ReadSourceMetaFile(path.Join(confDir, "mqtt_source.json"), true, false); nil != err {
		fmt.Println(err)
	}
}

var (
	address = "localhost"
	port    = 33061
)

func init() {
	modules.RegisterConnection("sql", client.CreateConnection)
	modules.RegisterLookupSource("sql", sql.GetLookupSource)
	modules.RegisterSink("sql", sql.GetSink)
}

func (suite *MetaTestSuite) TestLookupPing() {
	connection.InitConnectionManager4Test()
	s, err := testx.SetupEmbeddedMysqlServer(address, port)
	require.NoError(suite.T(), err)
	defer func() {
		s.Close()
	}()
	props := map[string]interface{}{
		"dburl":      fmt.Sprintf("mysql://root:@%v:%v/test", address, port),
		"datasource": "t",
		"table":      "t",
	}
	b, _ := json.Marshal(props)
	req, _ := http.NewRequest(http.MethodPost, "/metadata/lookups/connection/sql", bytes.NewBuffer(b))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	require.Equal(suite.T(), http.StatusOK, w.Code)
	req, _ = http.NewRequest(http.MethodPost, "/metadata/sinks/connection/sql", bytes.NewBuffer(b))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	require.Equal(suite.T(), http.StatusOK, w.Code)
}

func (suite *MetaTestSuite) TestSinksMetaHandler() {
	req, _ := http.NewRequest(http.MethodGet, "/metadata/sinks", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
}

func (suite *MetaTestSuite) TestNewSinkMetaHandler() {
	req, _ := http.NewRequest(http.MethodGet, "/metadata/sinks/mqtt", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
}

func (suite *MetaTestSuite) TestFunctionsMetaHandler() {
	req, _ := http.NewRequest(http.MethodGet, "/metadata/functions", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
}

func (suite *MetaTestSuite) TestOperatorsMetaHandler() {
	req, _ := http.NewRequest(http.MethodGet, "/metadata/operators", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
}

func (suite *MetaTestSuite) TestSourcesMetaHandler() {
	req, _ := http.NewRequest(http.MethodGet, "/metadata/sources", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
}

func (suite *MetaTestSuite) TestConnectionsMetaHandler() {
	req, _ := http.NewRequest(http.MethodGet, "/metadata/connections", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
}

func (suite *MetaTestSuite) TestSourceMetaHandler() {
	req, _ := http.NewRequest(http.MethodGet, "/metadata/sources/mqtt", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
}

func (suite *MetaTestSuite) TestConnectionMetaHandler() {
	req, _ := http.NewRequest(http.MethodGet, "/metadata/connections/mqtt", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
}

func (suite *MetaTestSuite) TestSourceConfHandler() {
	req, _ := http.NewRequest(http.MethodGet, "/metadata/sources/yaml/mqtt", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
}

func (suite *MetaTestSuite) TestConnectionConfHandler() {
	req, _ := http.NewRequest(http.MethodGet, "/metadata/connections/yaml/mqtt", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
}

func (suite *MetaTestSuite) TestSinkConfHandler() {
	req, _ := http.NewRequest(http.MethodGet, "/metadata/sinks/yaml/mqtt", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
}

func (suite *MetaTestSuite) TestSourceConfKeyHandler() {
	req, _ := http.NewRequest(http.MethodPut, "/metadata/sources/mqtt/confKeys/test", bytes.NewBufferString(`{"qos": 0, "server": "tcp://10.211.55.6:1883"}`))
	w := httptest.NewRecorder()
	DataDir, _ := conf.GetDataLoc()
	os.MkdirAll(path.Join(DataDir, "sources"), 0o755)
	if _, err := os.Create(path.Join(DataDir, "sources", "mqtt.yaml")); err != nil {
		fmt.Println(err)
	}
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
	os.Remove(path.Join(DataDir, "sources", "mqtt.yaml"))
	os.Remove(path.Join(DataDir, "sources"))

	// Test invalid confKey
	req, _ = http.NewRequest(http.MethodPut, "/metadata/sources/mqtt/confKeys/invalid.test", bytes.NewBufferString(`{"qos": 0, "server": "tcp://10.211.55.6:1883"}`))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)
}

func (suite *MetaTestSuite) TestConnectionConfKeyHandler() {
	req, _ := http.NewRequest(http.MethodPut, "/metadata/connections/mqtt/confKeys/test", bytes.NewBufferString(`{"qos": 0, "server": "tcp://10.211.55.6:1883"}`))
	w := httptest.NewRecorder()
	DataDir, _ := conf.GetDataLoc()
	os.MkdirAll(path.Join(DataDir, "connections"), 0o755)
	if _, err := os.Create(path.Join(DataDir, "connections", "connection.yaml")); err != nil {
		fmt.Println(err)
	}
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
	os.Remove(path.Join(DataDir, "connections", "connection.yaml"))
	os.Remove(path.Join(DataDir, "connections"))

	// Test invalid confKey
	req, _ = http.NewRequest(http.MethodPut, "/metadata/connections/mqtt/confKeys/invalid.test", bytes.NewBufferString(`{"qos": 0, "server": "tcp://10.211.55.6:1883"}`))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)
}

func (suite *MetaTestSuite) TestSinkConfKeyHandler() {
	req, _ := http.NewRequest(http.MethodPut, "/metadata/sinks/mqtt/confKeys/test", bytes.NewBufferString(`{"qos": 0, "server": "tcp://10.211.55.6:1883", "password":"123456"}`))
	DataDir, _ := conf.GetDataLoc()
	os.MkdirAll(path.Join(DataDir, "sinks"), 0o755)
	if _, err := os.Create(path.Join(DataDir, "sinks", "mqtt.yaml")); err != nil {
		fmt.Println(err)
	}
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
	os.Remove(path.Join(DataDir, "sinks", "mqtt.yaml"))
	os.Remove(path.Join(DataDir, "sinks"))

	// Test invalid confKey
	req, _ = http.NewRequest(http.MethodPut, "/metadata/sinks/mqtt/confKeys/invalid.test", bytes.NewBufferString(`{"qos": 0, "server": "tcp://10.211.55.6:1883", "password":"123456"}`))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)
}

func (suite *MetaTestSuite) TestResourcesHandler() {
	req, _ := http.NewRequest(http.MethodGet, "/metadata/resources", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
}

func (suite *MetaTestSuite) TestNotHiddenPassword() {
	req, _ := http.NewRequest(http.MethodPut, "/metadata/connections/test/confKeys/test", bytes.NewBufferString(`{"password": "123456","token":"123456","url": "sqlserver://username:password123456@140.210.204.147/testdb"}`))
	w := httptest.NewRecorder()
	DataDir, _ := conf.GetDataLoc()
	os.MkdirAll(path.Join(DataDir, "connections"), 0o755)
	if _, err := os.Create(path.Join(DataDir, "connections", "connection.yaml")); err != nil {
		fmt.Println(err)
	}
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
	req, _ = http.NewRequest(http.MethodGet, "/metadata/connections/yaml/test", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
	assert.Equal(suite.T(), bytes.NewBufferString(`{"test":{"password":"123456","token":"123456","url":"sqlserver://username:password123456@140.210.204.147/testdb"}}`), w.Body)

	os.Remove(path.Join(DataDir, "connections", "connection.yaml"))
	os.Remove(path.Join(DataDir, "connections"))
}

func TestMetaTestSuite(t *testing.T) {
	suite.Run(t, new(MetaTestSuite))
}

func (suite *MetaTestSuite) TestResourceSourceType() {
	req, _ := http.NewRequest(http.MethodPut, "/metadata/sources/mqtt/confKeys/demo", bytes.NewBufferString(`{"qos": 0, "server": "tcp://10.211.55.6:1883"}`))
	w := httptest.NewRecorder()
	DataDir, _ := conf.GetDataLoc()
	os.MkdirAll(path.Join(DataDir, "sources"), 0o755)
	_, err := os.Create(path.Join(DataDir, "sources", "mqtt.yaml"))
	require.NoError(suite.T(), err)
	suite.r.ServeHTTP(w, req)
	require.Equal(suite.T(), http.StatusOK, w.Code)

	req, _ = http.NewRequest(http.MethodGet, "/metadata/resource?sourceType=stream", bytes.NewBufferString("any"))
	suite.r.ServeHTTP(w, req)
	require.Equal(suite.T(), http.StatusOK, w.Code)
	returnval, _ := io.ReadAll(w.Body)
	require.Equal(suite.T(), `{}`, string(returnval))

	req, _ = http.NewRequest(http.MethodGet, "/metadata/resource", bytes.NewBufferString("any"))
	suite.r.ServeHTTP(w, req)
	require.Equal(suite.T(), http.StatusOK, w.Code)
	returnval, _ = io.ReadAll(w.Body)
	require.Equal(suite.T(), `{"mqtt":{"default":{"insecureSkipVerify":false,"protocolVersion":"3.1.1","qos":1,"server":"tcp://127.0.0.1:1883"},"demo":{"qos":0,"server":"tcp://10.211.55.6:1883"}}}`, string(returnval))
}
