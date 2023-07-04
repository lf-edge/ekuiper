// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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

package main

import (
	"flag"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/server"
)

var Version = "unknown"

var (
	loadFileType string
	etcPath      string
	dataPath     string
	logPath      string
	pluginsPath  string
)

func init() {
	flag.StringVar(&loadFileType, "loadFileTye", "", "loadFileType indicates the how to load path")
	flag.StringVar(&etcPath, "etc", "", "etc indicates the path of etc dir")
	flag.StringVar(&dataPath, "data", "", "data indicates the path of data dir")
	flag.StringVar(&logPath, "log", "", "log indicates the path of log dir")
	flag.StringVar(&pluginsPath, "plugins", "", "plugins indicates the path of plugins dir")
	if len(loadFileType) > 0 {
		conf.PathConfig.LoadFileType = loadFileType
	}
	if len(etcPath) > 0 {
		conf.PathConfig.EtcDir = etcPath
	}
	if len(dataPath) > 0 {
		conf.PathConfig.DataDir = dataPath
	}
	if len(logPath) > 0 {
		conf.PathConfig.LogDir = logPath
	}
	if len(pluginsPath) > 0 {
		conf.PathConfig.PluginsDir = pluginsPath
	}
}

func main() {
	server.StartUp(Version)
}
