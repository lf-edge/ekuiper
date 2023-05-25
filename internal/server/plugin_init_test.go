// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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

//go:build plugin || !core
// +build plugin !core

package server

import (
	"bytes"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lf-edge/ekuiper/internal/plugin"
)

type PluginTestSuite struct {
	suite.Suite
	m pluginComp
	r *mux.Router
}

func (suite *PluginTestSuite) SetupTest() {
	suite.m = pluginComp{}
	suite.r = mux.NewRouter()
	suite.m.rest(suite.r)
	suite.m.register()
}
func (suite *PluginTestSuite) Test_fetchPluginList() {
	version = "1.4.0"
	type args struct {
		t     plugin.PluginType
		hosts string
		os    string
		arch  string
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			"source",
			args{
				t:     plugin.SOURCE,
				hosts: "http://127.0.0.1:8080",
				os:    "debian",
				arch:  "amd64",
			},
			nil,
		},
		{
			"sink",
			args{
				t:     plugin.SINK,
				hosts: "http://127.0.0.1:8080",
				os:    "debian",
				arch:  "amd64",
			},
			nil,
		},
		{
			"function",
			args{
				t:     plugin.FUNCTION,
				hosts: "http://127.0.0.1:8080",
				os:    "debian",
				arch:  "amd64",
			},
			nil,
		},
	}
	for _, tt := range tests {
		_, gotErr := fetchPluginList(tt.args.t, tt.args.hosts, tt.args.os, tt.args.arch)
		assert.Equal(suite.T(), tt.wantErr, gotErr)
	}
}

func (suite *PluginTestSuite) TestSourcesHandler() {
	req, _ := http.NewRequest(http.MethodGet, "/plugins/sources", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
}

func (suite *PluginTestSuite) TestSinksHandler() {
	req, _ := http.NewRequest(http.MethodGet, "/plugins/sinks", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
}

func (suite *PluginTestSuite) TestFunctionsHandler() {
	req, _ := http.NewRequest(http.MethodGet, "/plugins/functions", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
}

func (suite *PluginTestSuite) TestUdfsHandler() {
	req, _ := http.NewRequest(http.MethodGet, "/plugins/udfs", bytes.NewBufferString("any"))
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	assert.Equal(suite.T(), http.StatusOK, w.Code)
}

func TestPluginTestSuite(t *testing.T) {
	suite.Run(t, new(PluginTestSuite))
}
