// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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

//go:build script || full

package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/lf-edge/ekuiper/v2/internal/binder"
	"github.com/lf-edge/ekuiper/v2/internal/plugin/js"
	"github.com/lf-edge/ekuiper/v2/pkg/validate"
)

func init() {
	components["script"] = scriptComp{}
}

type scriptComp struct{}

func (p scriptComp) register() {
	err := js.InitManager()
	if err != nil {
		panic(err)
	}
	entries = append(entries, binder.FactoryEntry{Name: "javascript function", Factory: js.GetManager(), Weight: 7})
}

func (p scriptComp) rest(r *mux.Router) {
	r.HandleFunc("/udf/javascript", jsfuncsHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/udf/javascript/{id}", jsfuncHandler).Methods(http.MethodGet, http.MethodDelete, http.MethodPut)
}

func (p scriptComp) exporter() ConfManager {
	return js.GetManager()
}

func jsfuncsHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	switch r.Method {
	case http.MethodGet:
		content, err := js.GetManager().List()
		if err != nil {
			handleError(w, err, "jsfuncs list command error", logger)
			return
		}
		jsonResponse(content, w, logger)
	case http.MethodPost:
		sd := &js.Script{}
		err := json.NewDecoder(r.Body).Decode(sd)
		// Problems decoding
		if err != nil {
			handleError(w, err, "Invalid body: Error decoding the new javascript function json", logger)
			return
		}
		if err := validate.ValidateID(sd.Id); err != nil {
			handleError(w, err, "", logger)
			return
		}
		err = js.GetManager().Create(sd)
		if err != nil {
			handleError(w, err, "javascript function create command error", logger)
			return
		}
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, "javascript function %s is created", sd.Id)
	}
}

func jsfuncHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	name := vars["id"]
	if err := validate.ValidateID(name); err != nil {
		handleError(w, err, "", logger)
		return
	}
	switch r.Method {
	case http.MethodDelete:
		err := js.GetManager().Delete(name)
		if err != nil {
			handleError(w, err, fmt.Sprintf("delete javascript function %s error", name), logger)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "javascript function %s is deleted", name)
	case http.MethodGet:
		j, err := js.GetManager().GetScript(name)
		if err != nil {
			handleError(w, err, fmt.Sprintf("describe javascript function %s error", name), logger)
			return
		}
		jsonResponse(j, w, logger)
	case http.MethodPut:
		sd := &js.Script{}
		err := json.NewDecoder(r.Body).Decode(sd)
		// Problems decoding
		if err != nil {
			handleError(w, err, "Invalid body: Error decoding the javascript function json", logger)
			return
		}
		if err := validate.ValidateID(sd.Id); err != nil {
			handleError(w, err, "", logger)
			return
		}
		err = js.GetManager().Update(sd)
		if err != nil {
			handleError(w, err, "javascript function update command error", logger)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "javascript function %s is updated", sd.Id)
	}
}
