// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
)

type ConnectionRequest struct {
	ID    string                 `json:"id"`
	Typ   string                 `json:"typ"`
	Props map[string]interface{} `json:"props"`
}

func connectionsStatusHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	switch r.Method {
	case http.MethodGet:
		allStatus := connection.GetAllConnectionStatus(context.Background())
		w.WriteHeader(http.StatusOK)
		jsonResponse(allStatus, w, logger)
	}
}

func connectionsHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	switch r.Method {
	case http.MethodPost:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			handleError(w, err, "Invalid body", logger)
			return
		}
		req := &ConnectionRequest{}
		if err := json.Unmarshal(body, req); err != nil {
			handleError(w, err, "Invalid body", logger)
			return
		}
		_, err = connection.CreateNamedConnection(context.Background(), req.ID, req.Typ, req.Props)
		if err != nil {
			handleError(w, err, "create connection failed", logger)
			return
		}
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("success"))
	case http.MethodGet:
		metaList := connection.GetAllConnectionsMeta()
		resp := make([]*ConnectionResponse, 0)
		for _, meta := range metaList {
			resp = append(resp, getConnectionRespByMeta(meta))
		}
		w.WriteHeader(http.StatusOK)
		jsonResponse(resp, w, logger)
	}
}

type ConnectionResponse struct {
	ID       string         `json:"id"`
	Typ      string         `json:"typ"`
	Props    map[string]any `json:"props"`
	Status   string         `json:"status,omitempty"`
	Err      string         `json:"err,omitempty"`
	RefCount int            `json:"refCount,omitempty"`
}

func connectionHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	id := mux.Vars(r)["id"]
	switch r.Method {
	case http.MethodGet:
		meta, err := connection.GetConnectionDetail(context.Background(), id)
		if err != nil {
			handleError(w, err, "", logger)
			return
		}
		res := getConnectionRespByMeta(meta)
		jsonResponse(res, w, logger)
	case http.MethodDelete:
		if err := connection.DropNameConnection(context.Background(), id); err != nil {
			handleError(w, err, "drop connection failed", logger)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("success"))
	}
}

func getConnectionRespByMeta(meta *connection.Meta) *ConnectionResponse {
	err := connection.PingConnection(context.Background(), meta.ID)
	r := &ConnectionResponse{
		Typ:      meta.Typ,
		ID:       meta.ID,
		Props:    meta.Props,
		RefCount: meta.GetRefCount(),
	}
	if err == nil {
		r.Status = "running"
	} else {
		r.Err = err.Error()
	}
	return r
}
