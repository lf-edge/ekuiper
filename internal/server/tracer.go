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
	"net/http"
	"strconv"

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

func getTraceIDByRuleID(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["ruleID"]
	l := r.URL.Query().Get("limit")
	limit, err := strconv.ParseInt(l, 10, 64)
	if err != nil {
		limit = 0
	}
	root := tracer.GetTraceIDListByRuleID(id, int(limit))
	if root == nil {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	jsonResponse(root, w, logger)
}
