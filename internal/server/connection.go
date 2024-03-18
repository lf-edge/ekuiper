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
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/lf-edge/ekuiper/internal/io/http/httpserver"
	"github.com/lf-edge/ekuiper/internal/topo/context"
)

type ConnectionBody struct {
	Endpoint string `json:"endpoint"`
}

func connectionHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		handleError(w, err, "Invalid body", logger)
		return
	}
	cb := &ConnectionBody{}
	if err = json.Unmarshal(body, cb); err != nil {
		handleError(w, err, "Invalid body", logger)
		return
	}
	if len(cb.Endpoint) < 1 {
		handleError(w, errors.New("endpoint should be defined"), "Invalid body", logger)
		return
	}
	switch r.Method {
	case http.MethodGet:
		exists := httpserver.CheckWebsocketEndpoint(cb.Endpoint)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(strconv.FormatBool(exists)))
	case http.MethodPost:
		_, _, _, err := httpserver.RegisterWebSocketEndpoint(context.Background(), cb.Endpoint)
		if err != nil {
			handleError(w, err, "", logger)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		err := httpserver.UnRegisterWebSocketEndpoint(cb.Endpoint)
		if err != nil {
			handleError(w, err, "", logger)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}
