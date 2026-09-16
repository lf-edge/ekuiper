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
)

// The plugin-name validation in confKey handlers must reject traversal at
// the edge, before any manager is touched, so this test needs no config
// or manager setup. Normal handler behavior is covered by functional tests.
func TestConfKeyHandlerPluginNameValidation(t *testing.T) {
	handlers := map[string]func(http.ResponseWriter, *http.Request){
		"source":     sourceConfKeyHandler,
		"sink":       sinkConfKeyHandler,
		"connection": connectionConfKeyHandler,
	}
	for name, h := range handlers {
		req, _ := http.NewRequest(http.MethodPut,
			"/metadata/x/../../etc/confKeys/test",
			bytes.NewBufferString(`{"qos": 0}`))
		req = mux.SetURLVars(req, map[string]string{"name": "../../etc", "confKey": "test"})
		rr := httptest.NewRecorder()
		h(rr, req)
		assert.Equal(t, http.StatusBadRequest, rr.Code, "%s handler accepted traversal", name)
		assert.Contains(t, rr.Body.String(), "Invalid plugin name", "%s handler wrong error", name)
	}
}

func TestPortableHandlerNameValidation(t *testing.T) {
	req, _ := http.NewRequest(http.MethodDelete, "/plugins/portables/../../etc", bytes.NewReader(nil))
	req = mux.SetURLVars(req, map[string]string{"name": "../../etc"})
	rr := httptest.NewRecorder()
	portableHandler(rr, req)
	assert.Equal(t, http.StatusBadRequest, rr.Code, "portable handler accepted traversal")
	assert.Contains(t, rr.Body.String(), "invalid characters")
}
