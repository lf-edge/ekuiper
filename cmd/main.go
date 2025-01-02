// Copyright 2024 EMQ Technologies Co., Ltd.
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

package cmd

import (
	"flag"
	"os"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/server"
)

// The compile time variable
var (
	Version      = "unknown"
	LoadFileType = "relative"
)

var (
	loadFileType string
	etcPath      string
	dataPath     string
	logPath      string
	pluginsPath  string
)

func init() {
	fs := flag.NewFlagSet("ek", flag.ContinueOnError)
	fs.StringVar(&loadFileType, "loadFileType", "", "loadFileType indicates the how to load path")
	fs.StringVar(&etcPath, "etc", "", "etc indicates the path of etc dir")
	fs.StringVar(&dataPath, "data", "", "data indicates the path of data dir")
	fs.StringVar(&logPath, "log", "", "log indicates the path of log dir")
	fs.StringVar(&pluginsPath, "plugins", "", "plugins indicates the path of plugins dir")
	_ = fs.Parse(os.Args[1:])

	if len(loadFileType) > 0 {
		conf.PathConfig.LoadFileType = loadFileType
	} else {
		conf.PathConfig.LoadFileType = LoadFileType
	}
	if len(etcPath) > 0 {
		conf.PathConfig.Dirs["etc"] = etcPath
	}
	if len(dataPath) > 0 {
		conf.PathConfig.Dirs["data"] = dataPath
	}
	if len(logPath) > 0 {
		conf.PathConfig.Dirs["log"] = logPath
	}
	if len(pluginsPath) > 0 {
		conf.PathConfig.Dirs["plugins"] = pluginsPath
	}
}

func Main() {
	server.StartUp(Version)
}
