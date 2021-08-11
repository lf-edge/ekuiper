// Copyright 2021 EMQ Technologies Co., Ltd.
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

package conf

import (
	"fmt"
	"github.com/lestrrat-go/file-rotatelogs"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
	"io"
	"io/ioutil"
	"os"
	"path"
	"time"
)

const StreamConf = "kuiper.yaml"

var (
	Config    *KuiperConf
	IsTesting bool
)

func LoadConf(confName string) ([]byte, error) {
	confDir, err := GetConfLoc()
	if err != nil {
		return nil, err
	}

	file := path.Join(confDir, confName)
	b, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	return b, nil
}

type tlsConf struct {
	Certfile string `yaml:"certfile"`
	Keyfile  string `yaml:"keyfile"`
}

type KuiperConf struct {
	Basic struct {
		Debug          bool     `yaml:"debug"`
		ConsoleLog     bool     `yaml:"consoleLog"`
		FileLog        bool     `yaml:"fileLog"`
		RotateTime     int      `yaml:"rotateTime"`
		MaxAge         int      `yaml:"maxAge"`
		Ip             string   `yaml:"ip"`
		Port           int      `yaml:"port"`
		RestIp         string   `yaml:"restIp"`
		RestPort       int      `yaml:"restPort"`
		RestTls        *tlsConf `yaml:"restTls"`
		Prometheus     bool     `yaml:"prometheus"`
		PrometheusPort int      `yaml:"prometheusPort"`
		PluginHosts    string   `yaml:"pluginHosts"`
	}
	Rule api.RuleOption
	Sink struct {
		CacheThreshold    int  `yaml:"cacheThreshold"`
		CacheTriggerCount int  `yaml:"cacheTriggerCount"`
		DisableCache      bool `yaml:"disableCache"`
	}
	Store struct {
		Type  string `yaml:"type"`
		Redis struct {
			Host     string `yaml:"host"`
			Port     int    `yaml:"port"`
			Password string `yaml:"password"`
			Timeout  int    `yaml:"timeout"`
		}
		Sqlite struct {
			Path string `yaml:"path"`
			Name string `yaml:"name"`
		}
	}
}

func InitConf() {
	b, err := LoadConf(StreamConf)
	if err != nil {
		Log.Fatal(err)
	}

	kc := KuiperConf{
		Rule: api.RuleOption{
			LateTol:            1000,
			Concurrency:        1,
			BufferLength:       1024,
			CheckpointInterval: 300000, //5 minutes
			SendError:          true,
		},
	}
	if err := yaml.Unmarshal(b, &kc); err != nil {
		Log.Fatal(err)
	} else {
		Config = &kc
	}
	if 0 == len(Config.Basic.Ip) {
		Config.Basic.Ip = "0.0.0.0"
	}
	if 0 == len(Config.Basic.RestIp) {
		Config.Basic.RestIp = "0.0.0.0"
	}

	if Config.Basic.Debug {
		Log.SetLevel(logrus.DebugLevel)
	}

	if Config.Basic.FileLog {
		logDir, err := GetLoc(logDir)
		if err != nil {
			Log.Fatal(err)
		}

		file := path.Join(logDir, logFileName)
		logWriter, err := rotatelogs.New(
			file+".%Y-%m-%d_%H-%M-%S",
			rotatelogs.WithLinkName(file),
			rotatelogs.WithRotationTime(time.Hour*time.Duration(Config.Basic.RotateTime)),
			rotatelogs.WithMaxAge(time.Hour*time.Duration(Config.Basic.MaxAge)),
		)

		if err != nil {
			fmt.Println("Failed to init log file settings..." + err.Error())
			Log.Infof("Failed to log to file, using default stderr.")
		} else if Config.Basic.ConsoleLog {
			mw := io.MultiWriter(os.Stdout, logWriter)
			Log.SetOutput(mw)
		} else if !Config.Basic.ConsoleLog {
			Log.SetOutput(logWriter)
		}
	} else if Config.Basic.ConsoleLog {
		Log.SetOutput(os.Stdout)
	}
}

func init() {
	InitLogger()
	InitClock()
}
