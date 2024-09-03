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

package server

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/lf-edge/ekuiper/v2/pkg/tracer"
)

func getTraceByID(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	root := tracer.GetSpanByTraceID(id)
	if root == nil {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	jsonResponse(root, w, logger)
}

func tracerHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		handleError(w, err, "Invalid body", logger)
		return
	}
	req := &SetTracerRequest{}
	if err := json.Unmarshal(body, req); err != nil {
		handleError(w, err, "Invalid body", logger)
		return
	}
	if err := tracer.SetTracer(req.Action, req.ServiceName, req.CollectorUrl); err != nil {
		handleError(w, err, "", logger)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("success"))
}

type SetTracerRequest struct {
	ServiceName  string `json:"serviceName"`
	Action       bool   `json:"action"`
	CollectorUrl string `json:"collector_url"`
}
