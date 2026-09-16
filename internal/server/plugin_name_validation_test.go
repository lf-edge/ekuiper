// Copyright 2026 EMQ Technologies Co., Ltd.
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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/meta"
)

// The plugin-name validation in confKey handlers and the portable plugin
// handler must reject traversal at the edge, before any manager is touched.
func TestConfKeyHandlerPluginNameValidation(t *testing.T) {
	conf.InitConf()
	conf.IsTesting = true
	meta.InitYamlConfigManager()

	handlers := map[string]func(http.ResponseWriter, *http.Request){
		"source":     sourceConfKeyHandler,
		"sink":       sinkConfKeyHandler,
		"connection": connectionConfKeyHandler,
	}
	for name, h := range handlers {
		// Traversal and separator names are rejected.
		for _, bad := range []string{"../../etc", "a/b", ".."} {
			req, _ := http.NewRequest(http.MethodPut,
				"/metadata/x/"+bad+"/confKeys/test",
				bytes.NewBufferString(`{"qos": 0}`))
			req = mux.SetURLVars(req, map[string]string{"name": bad, "confKey": "test"})
			rr := httptest.NewRecorder()
			h(rr, req)
			assert.NotEqual(t, http.StatusOK, rr.Code, "%s handler accepted %q", name, bad)
		}
		// A clean name passes validation (handler proceeds to the manager).
		req, _ := http.NewRequest(http.MethodPut,
			"/metadata/sources/mqtt/confKeys/plugintest",
			bytes.NewBufferString(`{"qos": 0}`))
		req = mux.SetURLVars(req, map[string]string{"name": "mqtt", "confKey": "plugintest"})
		rr := httptest.NewRecorder()
		h(rr, req)
		assert.Equal(t, http.StatusOK, rr.Code, "%s handler rejected clean name: %s", name, rr.Body.String())
	}
}

func TestPortableHandlerNameValidation(t *testing.T) {
	for _, bad := range []string{"../../etc", "a/b", "..", ""} {
		req, _ := http.NewRequest(http.MethodDelete, "/plugins/portables/"+bad, bytes.NewReader(nil))
		req = mux.SetURLVars(req, map[string]string{"name": bad})
		rr := httptest.NewRecorder()
		portableHandler(rr, req)
		assert.NotEqual(t, http.StatusOK, rr.Code, "portable handler accepted %q", bad)
	}
}
