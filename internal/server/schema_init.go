// Copyright 2022 EMQ Technologies Co., Ltd.
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

//go:build schema || !core
// +build schema !core

package server

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/lf-edge/ekuiper/internal/pkg/def"
	"github.com/lf-edge/ekuiper/internal/schema"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	"net/http"
)

func init() {
	components["schema"] = schemaComp{}
}

type schemaComp struct{}

func (sc schemaComp) register() {
	err := schema.InitRegistry()
	if err != nil {
		panic(err)
	}
}

func (sc schemaComp) rest(r *mux.Router) {
	r.HandleFunc("/schemas/{type}", schemasHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/schemas/{type}/{name}", schemaHandler).Methods(http.MethodPut, http.MethodDelete, http.MethodGet)
}

func schemasHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	st := vars["type"]
	defer r.Body.Close()
	switch r.Method {
	case http.MethodGet:
		l, err := schema.GetAllForType(def.SchemaType(st))
		if err != nil {
			handleError(w, err, "", logger)
			return
		}
		jsonResponse(l, w, logger)
	case http.MethodPost:
		sch := &schema.Info{Type: def.SchemaType(st)}
		err := json.NewDecoder(r.Body).Decode(sch)
		if err != nil {
			handleError(w, err, "Invalid body: Error decoding schema json", logger)
			return
		}
		if sch.Content != "" && sch.FilePath != "" {
			handleError(w, nil, "Invalid body: Cannot specify both content and file", logger)
			return
		}
		err = schema.Register(sch)
		if err != nil {
			handleError(w, err, "schema create command error", logger)
			return
		}
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(fmt.Sprintf("%s schema %s is created", sch.Type, sch.Name)))
	}
}

func schemaHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	st := vars["type"]
	name := vars["name"]
	switch r.Method {
	case http.MethodGet:
		j, err := schema.GetSchema(def.SchemaType(st), name)
		if err != nil {
			handleError(w, err, "", logger)
			return
		} else if j == nil {
			handleError(w, errorx.NewWithCode(errorx.NOT_FOUND, "not found"), "", logger)
			return
		}
		jsonResponse(j, w, logger)
	case http.MethodDelete:
		err := schema.DeleteSchema(def.SchemaType(st), name)
		if err != nil {
			handleError(w, err, fmt.Sprintf("delete %s schema %s error", st, name), logger)
			return
		}
		w.WriteHeader(http.StatusOK)
		result := fmt.Sprintf("%s schema %s is deleted", st, name)
		w.Write([]byte(result))
	case http.MethodPut:
		sch := &schema.Info{Type: def.SchemaType(st), Name: name}
		err := json.NewDecoder(r.Body).Decode(sch)
		if err != nil {
			handleError(w, err, "Invalid body: Error decoding schema json", logger)
			return
		}
		if sch.Type != def.SchemaType(st) || sch.Name != name {
			handleError(w, nil, "Invalid body: Type or name does not match", logger)
			return
		}
		if sch.Content != "" && sch.FilePath != "" {
			handleError(w, nil, "Invalid body: Cannot specify both content and file", logger)
			return
		}
		err = schema.CreateOrUpdateSchema(sch)
		if err != nil {
			handleError(w, err, "schema update command error", logger)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(fmt.Sprintf("%s schema %s is updated", sch.Type, sch.Name)))
	}
}
