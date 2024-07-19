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

//go:build portable || !core

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/lf-edge/ekuiper/v2/internal/binder"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/plugin"
	"github.com/lf-edge/ekuiper/v2/internal/plugin/portable"
	"github.com/lf-edge/ekuiper/v2/internal/plugin/portable/runtime"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
)

var portableManager *portable.Manager

func init() {
	components["portable"] = portableComp{}
}

type portableComp struct{}

func (p portableComp) register() {
	var err error
	portableManager, err = portable.InitManager()
	if err != nil {
		panic(err)
	}
	entries = append(entries, binder.FactoryEntry{Name: "portable plugin", Factory: portableManager, Weight: 8})
}

func (p portableComp) rest(r *mux.Router) {
	r.HandleFunc("/plugins/portables", portablesHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/plugins/portables/{name}", portableHandler).Methods(http.MethodGet, http.MethodDelete, http.MethodPut)
	r.HandleFunc("/plugins/portables/{name}/status", portableStatusHandler).Methods(http.MethodGet)
}

func (p portableComp) exporter() ConfManager {
	return portableExporter{}
}

func portablesHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	switch r.Method {
	case http.MethodGet:
		content := portableManager.List()
		jsonResponse(content, w, logger)
	case http.MethodPost:
		sd := plugin.NewPluginByType(plugin.PORTABLE)
		err := json.NewDecoder(r.Body).Decode(sd)
		// Problems decoding
		if err != nil {
			handleError(w, err, "Invalid body: Error decoding the portable plugin json", logger)
			return
		}
		err = portableManager.Register(sd)
		if err != nil {
			handleError(w, err, "portable plugin create command error", logger)
			return
		}
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, "portable plugin %s is created", sd.GetName())
	}
}

func portableStatusHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	name := vars["name"]
	status, ok := runtime.GetPluginInsManager().GetPluginInsStatus(name)
	if !ok {
		handleError(w, errorx.NewWithCode(errorx.NOT_FOUND, "not found"), fmt.Sprintf("portable plugin %s not found", name), logger)
		return
	}
	w.WriteHeader(http.StatusOK)
	jsonResponse(status, w, logger)
}

func portableHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	name := vars["name"]
	switch r.Method {
	case http.MethodDelete:
		referenced := checkBeforeDrop(name)
		if referenced {
			handleError(w, fmt.Errorf("plugin %s referenced by other rules", name), "", logger)
		}
		err := portableManager.Delete(name)
		if err != nil {
			handleError(w, err, fmt.Sprintf("delete portable plugin %s error", name), logger)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "portable plugin %s is deleted", name)
	case http.MethodGet:
		j, ok := portableManager.GetPluginInfo(name)
		if !ok {
			handleError(w, errorx.NewWithCode(errorx.NOT_FOUND, "not found"), fmt.Sprintf("describe portable plugin %s error", name), logger)
			return
		}
		jsonResponse(j, w, logger)
	case http.MethodPut:
		sd := plugin.NewPluginByType(plugin.PORTABLE)
		err := json.NewDecoder(r.Body).Decode(sd)
		// Problems decoding
		if err != nil {
			handleError(w, err, "Invalid body: Error decoding the portable plugin json", logger)
			return
		}
		err = portableManager.Delete(name)
		if err != nil {
			conf.Log.Errorf("delete portable plugin %s error: %v", name, err)
		}
		err = portableManager.Register(sd)
		if err != nil {
			handleError(w, err, "portable plugin update command error", logger)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "portable plugin %s is updated", sd.GetName())
	}
}

type portableExporter struct{}

func (e portableExporter) Import(ctx context.Context, plugins map[string]string) map[string]string {
	return portableManager.PluginImport(ctx, plugins)
}

func (e portableExporter) PartialImport(ctx context.Context, plugins map[string]string) map[string]string {
	return portableManager.PluginPartialImport(ctx, plugins)
}

func (e portableExporter) Export() map[string]string {
	return portableManager.GetAllPlugins()
}

func (e portableExporter) Status() map[string]string {
	return portableManager.GetAllPluginsStatus()
}

func (e portableExporter) Reset() {
	portableManager.UninstallAllPlugins()
}

func checkBeforeDrop(name string) bool {
	status, ok := runtime.GetPluginInsManager().GetPluginInsStatus(name)
	if !ok {
		return false
	}
	if len(status.RefCount) > 0 {
		return true
	}
	return false
}
