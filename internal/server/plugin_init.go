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

//go:build plugin || !core
// +build plugin !core

package server

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/lf-edge/ekuiper/internal/binder"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/plugin"
	"github.com/lf-edge/ekuiper/internal/plugin/native"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	"net/http"
	"runtime"
	"strings"
)

var nativeManager *native.Manager

func init() {
	components["plugin"] = pluginComp{}
}

type pluginComp struct{}

func (p pluginComp) register() {
	var err error
	nativeManager, err = native.InitManager()
	if err != nil {
		panic(err)
	}
	entries = append(entries, binder.FactoryEntry{Name: "native plugin", Factory: nativeManager, Weight: 9})
}

func (p pluginComp) rest(r *mux.Router) {
	r.HandleFunc("/plugins/sources/prebuild", prebuildSourcePlugins).Methods(http.MethodGet)
	r.HandleFunc("/plugins/sinks/prebuild", prebuildSinkPlugins).Methods(http.MethodGet)
	r.HandleFunc("/plugins/functions/prebuild", prebuildFuncsPlugins).Methods(http.MethodGet)
	r.HandleFunc("/plugins/sources", sourcesHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/plugins/sources/{name}", sourceHandler).Methods(http.MethodDelete, http.MethodGet)
	r.HandleFunc("/plugins/sinks", sinksHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/plugins/sinks/{name}", sinkHandler).Methods(http.MethodDelete, http.MethodGet)
	r.HandleFunc("/plugins/functions", functionsHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/plugins/functions/{name}", functionHandler).Methods(http.MethodDelete, http.MethodGet)
	r.HandleFunc("/plugins/functions/{name}/register", functionRegisterHandler).Methods(http.MethodPost)
	r.HandleFunc("/plugins/udfs", functionsListHandler).Methods(http.MethodGet)
	r.HandleFunc("/plugins/udfs/{name}", functionsGetHandler).Methods(http.MethodGet)
}

func pluginsHandler(w http.ResponseWriter, r *http.Request, t plugin.PluginType) {
	defer r.Body.Close()
	switch r.Method {
	case http.MethodGet:
		content := nativeManager.List(t)
		jsonResponse(content, w, logger)
	case http.MethodPost:
		sd := plugin.NewPluginByType(t)
		err := json.NewDecoder(r.Body).Decode(sd)
		// Problems decoding
		if err != nil {
			handleError(w, err, fmt.Sprintf("Invalid body: Error decoding the %s plugin json", plugin.PluginTypes[t]), logger)
			return
		}
		err = nativeManager.Register(t, sd)
		if err != nil {
			handleError(w, err, fmt.Sprintf("%s plugins create command error", plugin.PluginTypes[t]), logger)
			return
		}
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(fmt.Sprintf("%s plugin %s is created", plugin.PluginTypes[t], sd.GetName())))
	}
}

func pluginHandler(w http.ResponseWriter, r *http.Request, t plugin.PluginType) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	name := vars["name"]
	cb := r.URL.Query().Get("stop")
	switch r.Method {
	case http.MethodDelete:
		r := cb == "1"
		err := nativeManager.Delete(t, name, r)
		if err != nil {
			handleError(w, err, fmt.Sprintf("delete %s plugin %s error", plugin.PluginTypes[t], name), logger)
			return
		}
		w.WriteHeader(http.StatusOK)
		result := fmt.Sprintf("%s plugin %s is deleted", plugin.PluginTypes[t], name)
		if r {
			result = fmt.Sprintf("%s and Kuiper will be stopped", result)
		} else {
			result = fmt.Sprintf("%s and Kuiper must restart for the change to take effect.", result)
		}
		w.Write([]byte(result))
	case http.MethodGet:
		j, ok := nativeManager.GetPluginInfo(t, name)
		if !ok {
			handleError(w, errorx.NewWithCode(errorx.NOT_FOUND, "not found"), fmt.Sprintf("describe %s plugin %s error", plugin.PluginTypes[t], name), logger)
			return
		}
		jsonResponse(j, w, logger)
	}
}

//list or create source plugin
func sourcesHandler(w http.ResponseWriter, r *http.Request) {
	pluginsHandler(w, r, plugin.SOURCE)
}

//delete a source plugin
func sourceHandler(w http.ResponseWriter, r *http.Request) {
	pluginHandler(w, r, plugin.SOURCE)
}

//list or create sink plugin
func sinksHandler(w http.ResponseWriter, r *http.Request) {
	pluginsHandler(w, r, plugin.SINK)
}

//delete a sink plugin
func sinkHandler(w http.ResponseWriter, r *http.Request) {
	pluginHandler(w, r, plugin.SINK)
}

//list or create function plugin
func functionsHandler(w http.ResponseWriter, r *http.Request) {
	pluginsHandler(w, r, plugin.FUNCTION)
}

//list all user defined functions in all function plugins
func functionsListHandler(w http.ResponseWriter, _ *http.Request) {
	content := nativeManager.ListSymbols()
	jsonResponse(content, w, logger)
}

func functionsGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	j, ok := nativeManager.GetPluginBySymbol(plugin.FUNCTION, name)
	if !ok {
		handleError(w, errorx.NewWithCode(errorx.NOT_FOUND, "not found"), fmt.Sprintf("describe function %s error", name), logger)
		return
	}
	jsonResponse(map[string]string{"name": name, "plugin": j}, w, logger)
}

//delete a function plugin
func functionHandler(w http.ResponseWriter, r *http.Request) {
	pluginHandler(w, r, plugin.FUNCTION)
}

type functionList struct {
	Functions []string `json:"functions,omitempty"`
}

// register function list for function plugin. If a plugin exports multiple functions, the function list must be registered
// either by create or register. If the function plugin has been loaded because of auto load through so file, the function
// list MUST be registered by this API or only the function with the same name as the plugin can be used.
func functionRegisterHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	name := vars["name"]
	_, ok := nativeManager.GetPluginInfo(plugin.FUNCTION, name)
	if !ok {
		handleError(w, errorx.NewWithCode(errorx.NOT_FOUND, "not found"), fmt.Sprintf("register %s plugin %s error", plugin.PluginTypes[plugin.FUNCTION], name), logger)
		return
	}
	sd := functionList{}
	err := json.NewDecoder(r.Body).Decode(&sd)
	// Problems decoding
	if err != nil {
		handleError(w, err, fmt.Sprintf("Invalid body: Error decoding the function list json %s", r.Body), logger)
		return
	}
	err = nativeManager.RegisterFuncs(name, sd.Functions)
	if err != nil {
		handleError(w, err, fmt.Sprintf("function plugins %s regiser functions error", name), logger)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("function plugin %s function list is registered", name)))
}

func prebuildSourcePlugins(w http.ResponseWriter, r *http.Request) {
	prebuildPluginsHandler(w, r, plugin.SOURCE)
}

func prebuildSinkPlugins(w http.ResponseWriter, r *http.Request) {
	prebuildPluginsHandler(w, r, plugin.SINK)
}

func prebuildFuncsPlugins(w http.ResponseWriter, r *http.Request) {
	prebuildPluginsHandler(w, r, plugin.FUNCTION)
}

func prebuildPluginsHandler(w http.ResponseWriter, _ *http.Request, t plugin.PluginType) {
	emsg := "It's strongly recommended to install plugins at linux. If you choose to proceed to install plugin, please make sure the plugin is already validated in your own build."
	if runtime.GOOS == "linux" {
		osrelease, err := Read()
		if err != nil {
			handleError(w, err, "", logger)
			return
		}
		prettyName := strings.ToUpper(osrelease["PRETTY_NAME"])
		var os = "debian"
		if strings.Contains(prettyName, "ALPINE") {
			os = "alpine"
		}

		hosts := conf.Config.Basic.PluginHosts
		if err, plugins := fetchPluginList(t, hosts, os, runtime.GOARCH); err != nil {
			handleError(w, err, "", logger)
		} else {
			jsonResponse(plugins, w, logger)
		}
	} else {
		handleError(w, fmt.Errorf(emsg), "", logger)
	}
}

var NativeSourcePlugin = []string{"random", "zmq", "sql", "video"}
var NativeSinkPlugin = []string{"image", "influx", "influx2", "tdengine", "zmq", "sql"}
var NativeFunctionPlugin = []string{"accumulateWordCount", "countPlusOne", "echo", "geohash", "image", "labelImage", "tfLite"}

func fetchPluginList(t plugin.PluginType, hosts, os, arch string) (err error, result map[string]string) {
	ptype := "sources"
	plugins := NativeSourcePlugin
	if t == plugin.SINK {
		ptype = "sinks"
		plugins = NativeSinkPlugin
	} else if t == plugin.FUNCTION {
		ptype = "functions"
		plugins = NativeFunctionPlugin
	}

	if hosts == "" || ptype == "" || os == "" {
		logger.Errorf("Invalid parameter value: hosts %s, ptype %s or os: %s should not be empty.", hosts, ptype, os)
		return fmt.Errorf("invalid configruation for plugin host in kuiper.yaml"), nil
	}
	result = make(map[string]string)
	hostsArr := strings.Split(hosts, ",")
	for _, host := range hostsArr {
		host := strings.Trim(host, " ")
		tmp := []string{host, "kuiper-plugins", version, os, ptype}
		//The url is similar to http://host:port/kuiper-plugins/0.9.1/debian/sinks/
		url := strings.Join(tmp, "/")

		for _, p := range plugins {
			result[p] = url + "/" + p + "_" + arch + ".zip"
		}
	}
	return
}

func pluginReset() {
	nativeManager.UninstallAllPlugins()
}

func pluginExport() map[string]string {
	return nativeManager.GetAllPlugins()
}

func pluginStatusExport() map[string]string {
	return nativeManager.GetAllPluginsStatus()
}

func pluginImport(plugins map[string]string) error {
	return nativeManager.PluginImport(plugins)
}

func pluginPartialImport(plugins map[string]string) map[string]string {
	return nativeManager.PluginPartialImport(plugins)
}
