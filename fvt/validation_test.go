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

package fvt

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidation(t *testing.T) {
	defer func() {
		// Cleanup if needed
	}()

	tests := []struct {
		name   string
		path   string
		method string
		body   string
	}{
		// Streams
		{"Get Stream Invalid", "streams/invalid%20name", http.MethodGet, ""},
		{"Delete Stream Invalid", "streams/invalid%20name", http.MethodDelete, ""},
		{"Put Stream Invalid", "streams/invalid%20name", http.MethodPut, `{"sql":"CREATE stream alert() WITH (DATASOURCE=\"0\", TYPE=\"mqtt\")"}`},

		// Tables
		{"Get Table Invalid", "tables/invalid%20name", http.MethodGet, ""},
		{"Delete Table Invalid", "tables/invalid%20name", http.MethodDelete, ""},
		{"Put Table Invalid", "tables/invalid%20name", http.MethodPut, `{"sql":"CREATE TABLE alertTable() WITH (DATASOURCE=\"0\", TYPE=\"memory\", KEY=\"id\", KIND=\"lookup\")"}`},

		// Rules
		{"Get Rule Invalid", "rules/invalid%20name", http.MethodGet, ""},
		{"Delete Rule Invalid", "rules/invalid%20name", http.MethodDelete, ""},
		{"Put Rule Invalid", "rules/invalid%20name", http.MethodPut, "{}"},

		// Services
		{"Get Service Invalid", "services/invalid%20name", http.MethodGet, ""},
		{"Delete Service Invalid", "services/invalid%20name", http.MethodDelete, ""},
		{"Put Service Invalid", "services/invalid%20name", http.MethodPut, "{}"},

		// Plugins
		{"Get Source Plugin Invalid", "plugins/sources/invalid%20name", http.MethodGet, ""},
		{"Delete Source Plugin Invalid", "plugins/sources/invalid%20name", http.MethodDelete, ""},
		{"Get Sink Plugin Invalid", "plugins/sinks/invalid%20name", http.MethodGet, ""},
		{"Delete Sink Plugin Invalid", "plugins/sinks/invalid%20name", http.MethodDelete, ""},
		{"Get Function Plugin Invalid", "plugins/functions/invalid%20name", http.MethodGet, ""},
		{"Delete Function Plugin Invalid", "plugins/functions/invalid%20name", http.MethodDelete, ""},

		// Scripts
		{"Get Script Invalid", "udf/javascript/invalid.id", http.MethodGet, ""},
		{"Delete Script Invalid", "udf/javascript/invalid.id", http.MethodDelete, ""},
		{"Put Script Invalid", "udf/javascript/invalid.id", http.MethodPut, "{}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := client.Req(tt.path, tt.method, tt.body)
			assert.NoError(t, err)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			txt, _ := GetResponseText(resp)
			assert.Contains(t, txt, "invalid characters")
		})
	}
}
