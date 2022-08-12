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

//go:build (plugin || !core) && (ui || !core)
// +build plugin !core
// +build ui !core

package server

import (
	"fmt"
	"github.com/gorilla/mux"
	"github.com/lf-edge/ekuiper/internal"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/plugin"
	"net/http"
	"os"
	"runtime"
	"strings"
)

// This must be and will be run after meta_init.go init()
func init() {
	metaEndpoints = append(metaEndpoints, func(r *mux.Router) {
		r.HandleFunc("/plugins/sources/prebuild", prebuildSourcePlugins).Methods(http.MethodGet)
		r.HandleFunc("/plugins/sinks/prebuild", prebuildSinkPlugins).Methods(http.MethodGet)
		r.HandleFunc("/plugins/functions/prebuild", prebuildFuncsPlugins).Methods(http.MethodGet)
	})
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

func isOffcialDockerImage() bool {
	if !strings.EqualFold(os.Getenv("MAINTAINER"), "emqx.io") {
		return false
	}
	return true
}

func prebuildPluginsHandler(w http.ResponseWriter, r *http.Request, t plugin.PluginType) {
	emsg := "It's strongly recommended to install plugins at official released Debian Docker images. If you choose to proceed to install plugin, please make sure the plugin is already validated in your own build."
	if !isOffcialDockerImage() {
		handleError(w, fmt.Errorf(emsg), "", logger)
		return
	} else if runtime.GOOS == "linux" {
		osrelease, err := Read()
		if err != nil {
			handleError(w, err, "", logger)
			return
		}
		prettyName := strings.ToUpper(osrelease["PRETTY_NAME"])
		os := "debian"
		if strings.Contains(prettyName, "DEBIAN") {
			hosts := conf.Config.Basic.PluginHosts

			if err, plugins := fetchPluginList(t, hosts, os, runtime.GOARCH); err != nil {
				handleError(w, err, "", logger)
			} else {
				jsonResponse(plugins, w, logger)
			}
		} else {
			handleError(w, fmt.Errorf(emsg), "", logger)
			return
		}
	} else {
		handleError(w, fmt.Errorf(emsg), "", logger)
	}
}

var NativeSourcePlugin = []string{"random", "zmq", "sql"}
var NativeSinkPlugin = []string{"file", "image", "influx", "redis", "tdengine", "zmq", "sql"}
var NativeFunctionPlugin = []string{"accumulateWordCount", "countPlusOne", "echo", "geohash", "image", "labelImage"}

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
			result[p] = url + "/" + p + "_" + arch + internal.ZipFileSuffix
		}
	}
	return
}
