// Copyright 2025 EMQ Technologies Co., Ltd.
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
	"net/http"
	"net/http/httptest"
)

func batchRequestHandler(w http.ResponseWriter, r *http.Request) {
	batchRequest := make([]*EachRequest, 0)
	if err := json.NewDecoder(r.Body).Decode(&batchRequest); err != nil {
		handleError(w, err, "", logger)
		return
	}
	allResponse := make([]*EachResponse, 0)
	for _, batchReq := range batchRequest {
		resp := &EachResponse{}
		rr := httptest.NewRecorder()
		req, err := http.NewRequest(batchReq.Method, batchReq.Path, bytes.NewBuffer([]byte(batchReq.Body)))
		if err != nil {
			resp.Error = err.Error()
			allResponse = append(allResponse, resp)
			continue
		}
		router.ServeHTTP(rr, req)
		resp.Code = rr.Code
		resp.Response = rr.Body.String()
		allResponse = append(allResponse, resp)
	}
	jsonResponse(allResponse, w, logger)
}

type EachRequest struct {
	Method string `json:"method"`
	Path   string `json:"path"`
	Body   string `json:"body"`
}

type EachResponse struct {
	Code     int    `json:"code"`
	Response string `json:"response"`
	Error    string `json:"error"`
}
