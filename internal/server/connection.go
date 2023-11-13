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
	"net/http"

	"github.com/gorilla/mux"

	"github.com/lf-edge/ekuiper/internal/io/http/httpserver"
	"github.com/lf-edge/ekuiper/internal/topo/context"
)

func connectionHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	endpoint := vars["endpoint"]

	switch r.Method {
	case http.MethodPost:
		_, _, _, err := httpserver.RegisterWebSocketEndpoint(context.Background(), endpoint)
		if err != nil {
			handleError(w, err, "", logger)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		err := httpserver.UnRegisterWebSocketEndpoint(endpoint)
		if err != nil {
			handleError(w, err, "", logger)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}
