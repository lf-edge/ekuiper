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

//go:build ui || !core
// +build ui !core

package server

import (
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/topo/node"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"io"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/lf-edge/ekuiper/internal/meta"
)

func init() {
	components["meta"] = metaComp{}
}

var metaEndpoints []restEndpoint

type metaComp struct {
}

func (m metaComp) register() {
	// do nothing
}

func (m metaComp) rest(r *mux.Router) {
	r.HandleFunc("/metadata/functions", functionsMetaHandler).Methods(http.MethodGet)
	r.HandleFunc("/metadata/operators", operatorsMetaHandler).Methods(http.MethodGet)
	r.HandleFunc("/metadata/sinks", sinksMetaHandler).Methods(http.MethodGet)
	r.HandleFunc("/metadata/sinks/{name}", newSinkMetaHandler).Methods(http.MethodGet)
	r.HandleFunc("/metadata/sources", sourcesMetaHandler).Methods(http.MethodGet)
	r.HandleFunc("/metadata/sources/{name}", sourceMetaHandler).Methods(http.MethodGet)
	r.HandleFunc("/metadata/sources/yaml/{name}", sourceConfHandler).Methods(http.MethodGet)
	r.HandleFunc("/metadata/sources/{name}/confKeys/{confKey}", sourceConfKeyHandler).Methods(http.MethodDelete, http.MethodPut)
	r.HandleFunc("/metadata/sinks/yaml/{name}", sinkConfHandler).Methods(http.MethodGet)
	r.HandleFunc("/metadata/sinks/{name}/confKeys/{confKey}", sinkConfKeyHandler).Methods(http.MethodDelete, http.MethodPut)

	r.HandleFunc("/metadata/connections", connectionsMetaHandler).Methods(http.MethodGet)
	r.HandleFunc("/metadata/connections/{name}", connectionMetaHandler).Methods(http.MethodGet)
	r.HandleFunc("/metadata/connections/yaml/{name}", connectionConfHandler).Methods(http.MethodGet)
	r.HandleFunc("/metadata/connections/{name}/confKeys/{confKey}", connectionConfKeyHandler).Methods(http.MethodDelete, http.MethodPut)

	r.HandleFunc("/metadata/resources", resourcesHandler).Methods(http.MethodGet)
	r.HandleFunc("/metadata/sources/connection/{name}", sourceConnectionHandler).Methods(http.MethodPost)
	r.HandleFunc("/metadata/sinks/connection/{name}", sinkConnectionHandler).Methods(http.MethodPost)
	r.HandleFunc("/metadata/reset/yaml", metaResetHandler).Methods(http.MethodGet)
	for _, endpoint := range metaEndpoints {
		endpoint(r)
	}
}

// list sink plugin
func sinksMetaHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	sinks := meta.GetSinks()
	jsonResponse(sinks, w, logger)
	return
}

// Get sink metadata when creating rules
func newSinkMetaHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	pluginName := vars["name"]

	language := getLanguage(r)
	ptrMetadata, err := meta.GetSinkMeta(pluginName, language)
	if err != nil {
		handleError(w, err, "", logger)
		return
	}
	jsonResponse(ptrMetadata, w, logger)
}

// list functions
func functionsMetaHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	funcs := meta.GetFunctions()
	jsonByteResponse(funcs, w, logger)
	return
}

// list operators
func operatorsMetaHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	ops := meta.GetOperators()
	jsonByteResponse(ops, w, logger)
	return
}

// list source plugin
func sourcesMetaHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	kind := r.URL.Query().Get("kind")
	switch strings.ToLower(kind) {
	case "lookup":
		kind = ast.StreamKindLookup
	default:
		kind = ast.StreamKindScan
	}
	ret := meta.GetSourcesPlugins(kind)
	if nil != ret {
		jsonResponse(ret, w, logger)
		return
	}
}

// list shareMeta
func connectionsMetaHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	ret := meta.GetConnectionPlugins()
	if nil != ret {
		jsonResponse(ret, w, logger)
		return
	}
}

// Get source metadata when creating stream
func sourceMetaHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	pluginName := vars["name"]
	language := getLanguage(r)
	ret, err := meta.GetSourceMeta(pluginName, language)
	if err != nil {
		handleError(w, err, "", logger)
		return
	}
	if nil != ret {
		jsonResponse(ret, w, logger)
		return
	}
}

// Get source metadata when creating stream
func connectionMetaHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	pluginName := vars["name"]
	language := getLanguage(r)
	ret, err := meta.GetConnectionMeta(pluginName, language)
	if err != nil {
		handleError(w, err, "", logger)
		return
	}
	if nil != ret {
		jsonResponse(ret, w, logger)
		return
	}
}

// Get source yaml
func sourceConfHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	pluginName := vars["name"]
	language := getLanguage(r)
	configOperatorKey := fmt.Sprintf(meta.SourceCfgOperatorKeyTemplate, pluginName)
	ret, err := meta.GetYamlConf(configOperatorKey, language)
	if err != nil {
		handleError(w, err, "", logger)
		return
	} else {
		w.Write(ret)
	}
}

// Get share yaml
func connectionConfHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	pluginName := vars["name"]
	language := getLanguage(r)
	configOperatorKey := fmt.Sprintf(meta.ConnectionCfgOperatorKeyTemplate, pluginName)
	ret, err := meta.GetYamlConf(configOperatorKey, language)
	if err != nil {
		handleError(w, err, "", logger)
		return
	} else {
		_, _ = w.Write(ret)
	}
}

// Get sink yaml
func sinkConfHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	pluginName := vars["name"]
	language := getLanguage(r)
	configOperatorKey := fmt.Sprintf(meta.SinkCfgOperatorKeyTemplate, pluginName)
	ret, err := meta.GetYamlConf(configOperatorKey, language)
	if err != nil {
		handleError(w, err, "", logger)
		return
	} else {
		_, _ = w.Write(ret)
	}
}

// Add  del confkey
func sourceConfKeyHandler(w http.ResponseWriter, r *http.Request) {

	defer r.Body.Close()
	var err error
	vars := mux.Vars(r)
	pluginName := vars["name"]
	confKey := vars["confKey"]
	language := getLanguage(r)
	switch r.Method {
	case http.MethodDelete:
		err = meta.DelSourceConfKey(pluginName, confKey, language)
	case http.MethodPut:
		v, err1 := io.ReadAll(r.Body)
		if err1 != nil {
			handleError(w, err, "Invalid body", logger)
			return
		}
		err = meta.AddSourceConfKey(pluginName, confKey, language, v)
	}
	if err != nil {
		handleError(w, err, "", logger)
		return
	}
}

// Add  del confkey
func sinkConfKeyHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var err error
	vars := mux.Vars(r)
	pluginName := vars["name"]
	confKey := vars["confKey"]
	language := getLanguage(r)
	switch r.Method {
	case http.MethodDelete:
		err = meta.DelSinkConfKey(pluginName, confKey, language)
	case http.MethodPut:
		v, err1 := io.ReadAll(r.Body)
		if err1 != nil {
			handleError(w, err, "Invalid body", logger)
			return
		}
		err = meta.AddSinkConfKey(pluginName, confKey, language, v)
	}
	if err != nil {
		handleError(w, err, "", logger)
		return
	}
}

// Add  del confkey
func connectionConfKeyHandler(w http.ResponseWriter, r *http.Request) {

	defer r.Body.Close()
	var err error
	vars := mux.Vars(r)
	pluginName := vars["name"]
	confKey := vars["confKey"]
	language := getLanguage(r)
	switch r.Method {
	case http.MethodDelete:
		err = meta.DelConnectionConfKey(pluginName, confKey, language)
	case http.MethodPut:
		v, err1 := io.ReadAll(r.Body)
		if err1 != nil {
			handleError(w, err1, "Invalid body", logger)
			return
		}
		err = meta.AddConnectionConfKey(pluginName, confKey, language, v)
	}
	if err != nil {
		handleError(w, err, "", logger)
		return
	}
}

// get updatable resources
func resourcesHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	language := getLanguage(r)
	ret, err := meta.GetResources(language)
	if err != nil {
		handleError(w, err, "", logger)
		return
	} else {
		_, _ = w.Write(ret)
	}
}

func getLanguage(r *http.Request) string {
	language := r.Header.Get("Content-Language")
	if 0 == len(language) {
		language = "en_US"
	} else {
		strings.ReplaceAll(language, "-", "_")
	}
	return language
}

func sinkConnectionHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)

	sinkNm := vars["name"]
	config := map[string]interface{}{}
	v, _ := io.ReadAll(r.Body)
	err := json.Unmarshal(v, &config)
	if err != nil {
		handleError(w, err, "", logger)
		return
	}

	err = node.SinkOpen(sinkNm, config)
	if err != nil {
		handleError(w, err, "", logger)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func sourceConnectionHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)

	sourceNm := vars["name"]
	config := map[string]interface{}{}
	v, _ := io.ReadAll(r.Body)
	err := json.Unmarshal(v, &config)
	if err != nil {
		handleError(w, err, "", logger)
		return
	}

	err = node.SourceOpen(sourceNm, config)
	if err != nil {
		handleError(w, err, "", logger)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func metaResetHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	meta.ResetConfigs()
	w.WriteHeader(http.StatusOK)
}
