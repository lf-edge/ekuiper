// Copyright erfenjiao, 630166475@qq.com.
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

//go:build wasm
// +build wasm

package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/lf-edge/ekuiper/internal/binder"
	"github.com/lf-edge/ekuiper/internal/plugin"
	"github.com/lf-edge/ekuiper/internal/plugin/wasm"
	"github.com/lf-edge/ekuiper/pkg/errorx"
)

var wasmManager *wasm.Manager

func init() {
	components["wasm"] = wasmComp{}
}

type wasmComp struct{}

func (p wasmComp) register() {
	var err error
	wasmManager, err = wasm.InitManager()
	if err != nil {
		panic(err)
	}
	entries = append(entries, binder.FactoryEntry{Name: "wasm plugin", Factory: wasmManager, Weight: 8})
}

func (p wasmComp) rest(r *mux.Router) {
	r.HandleFunc("/plugins/wasm", wasmsHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/plugins/wasm/{name}", wasmHandler).Methods(http.MethodGet, http.MethodDelete)
}

func wasmsHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	switch r.Method {
	case http.MethodGet:
		content := wasmManager.List()
		jsonResponse(content, w, logger)
	case http.MethodPost:
		sd := plugin.NewPluginByType(plugin.WASM)
		err := json.NewDecoder(r.Body).Decode(sd)
		// Problems decoding
		if err != nil {
			handleError(w, err, "Invalid body: Error decoding the wasm plugin json", logger)
			return
		}
		err = wasmManager.Register(sd)
		if err != nil {
			handleError(w, err, "wasm plugin create command error", logger)
			return
		}
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(fmt.Sprintf("wasm plugin %s is created", sd.GetName())))
	}
}

func wasmHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	name := vars["name"]
	switch r.Method {
	case http.MethodDelete:
		err := wasmManager.Delete(name)
		if err != nil {
			handleError(w, err, fmt.Sprintf("delete wasm plugin %s error", name), logger)
			return
		}
		w.WriteHeader(http.StatusOK)
		result := fmt.Sprintf("wasm plugin %s is deleted", name)
		w.Write([]byte(result))
	case http.MethodGet:
		j, ok := wasmManager.GetPluginInfo(name)
		if !ok {
			handleError(w, errorx.NewWithCode(errorx.NOT_FOUND, "not found"), fmt.Sprintf("describe wasm plugin %s error", name), logger)
			return
		}
		jsonResponse(j, w, logger)
	}
}
