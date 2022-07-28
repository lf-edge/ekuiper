// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/pkg/errorx"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"path"
	"time"
)

const ConfFileName = "kuiper.yaml"

var (
	Config    *KuiperConf
	IsTesting bool
)

type tlsConf struct {
	Certfile string `yaml:"certfile"`
	Keyfile  string `yaml:"keyfile"`
}

type SinkConf struct {
	MemoryCacheThreshold int  `json:"memoryCacheThreshold" yaml:"memoryCacheThreshold"`
	MaxDiskCache         int  `json:"maxDiskCache" yaml:"maxDiskCache"`
	BufferPageSize       int  `json:"bufferPageSize" yaml:"bufferPageSize"`
	EnableCache          bool `json:"enableCache" yaml:"enableCache"`
	ResendInterval       int  `json:"resendInterval" yaml:"resendInterval"`
	CleanCacheAtStop     bool `json:"cleanCacheAtStop" yaml:"cleanCacheAtStop"`
}

// Validate the configuration and reset to the default value for invalid values.
func (sc SinkConf) Validate() error {
	e := make(errorx.MultiError)
	if sc.MemoryCacheThreshold < 0 {
		sc.MemoryCacheThreshold = 1024
		Log.Warnf("memoryCacheThreshold is less than 0, set to 1024")
		e["memoryCacheThreshold"] = fmt.Errorf("memoryCacheThreshold must be positive")
	}
	if sc.MaxDiskCache < 0 {
		sc.MaxDiskCache = 1024000
		Log.Warnf("maxDiskCache is less than 0, set to 1024000")
		e["maxDiskCache"] = fmt.Errorf("maxDiskCache must be positive")
	}
	if sc.BufferPageSize <= 0 {
		sc.BufferPageSize = 256
		Log.Warnf("bufferPageSize is less than or equal to 0, set to 256")
		e["bufferPageSize"] = fmt.Errorf("bufferPageSize must be positive")
	}
	if sc.ResendInterval < 0 {
		sc.ResendInterval = 0
		Log.Warnf("resendInterval is less than 0, set to 0")
		e["resendInterval"] = fmt.Errorf("resendInterval must be positive")
	}
	if sc.BufferPageSize > sc.MemoryCacheThreshold {
		sc.MemoryCacheThreshold = sc.BufferPageSize
		Log.Warnf("memoryCacheThreshold is less than bufferPageSize, set to %d", sc.BufferPageSize)
		e["memoryCacheThresholdTooSmall"] = fmt.Errorf("memoryCacheThreshold must be greater than or equal to bufferPageSize")
	}
	if sc.MemoryCacheThreshold%sc.BufferPageSize != 0 {
		sc.MemoryCacheThreshold = sc.BufferPageSize * (sc.MemoryCacheThreshold/sc.BufferPageSize + 1)
		Log.Warnf("memoryCacheThreshold is not a multiple of bufferPageSize, set to %d", sc.MemoryCacheThreshold)
		e["memoryCacheThresholdNotMultiple"] = fmt.Errorf("memoryCacheThreshold must be a multiple of bufferPageSize")
	}
	if sc.BufferPageSize > sc.MaxDiskCache {
		sc.MaxDiskCache = sc.BufferPageSize
		Log.Warnf("maxDiskCache is less than bufferPageSize, set to %d", sc.BufferPageSize)
		e["maxDiskCacheTooSmall"] = fmt.Errorf("maxDiskCache must be greater than bufferPageSize")
	}
	if sc.MaxDiskCache%sc.BufferPageSize != 0 {
		sc.MaxDiskCache = sc.BufferPageSize * (sc.MaxDiskCache/sc.BufferPageSize + 1)
		Log.Warnf("maxDiskCache is not a multiple of bufferPageSize, set to %d", sc.MaxDiskCache)
		e["maxDiskCacheNotMultiple"] = fmt.Errorf("maxDiskCache must be a multiple of bufferPageSize")
	}
	return e.GetError()
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
		Authentication bool     `yaml:"authentication"`
		IgnoreCase     bool     `yaml:"ignoreCase"`
	}
	Rule  api.RuleOption
	Sink  *SinkConf
	Store struct {
		Type  string `yaml:"type"`
		Redis struct {
			Host               string `yaml:"host"`
			Port               int    `yaml:"port"`
			Password           string `yaml:"password"`
			Timeout            int    `yaml:"timeout"`
			ConnectionSelector string `yaml:"connectionSelector"`
		}
		Sqlite struct {
			Name string `yaml:"name"`
		}
	}
	Portable struct {
		PythonBin string `yaml:"pythonBin"`
	}
}

func InitConf() {
	cpath, err := GetConfLoc()
	if err != nil {
		panic(err)
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

	err = LoadConfigFromPath(path.Join(cpath, ConfFileName), &kc)
	if err != nil {
		Log.Fatal(err)
		panic(err)
	}
	Config = &kc
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

	if Config.Store.Type == "redis" && Config.Store.Redis.ConnectionSelector != "" {
		if err := RedisStorageConSelectorApply(Config.Store.Redis.ConnectionSelector, Config); err != nil {
			Log.Fatal(err)
		}
	}

	if Config.Portable.PythonBin == "" {
		Config.Portable.PythonBin = "python"
	}
	_ = Config.Sink.Validate()
}

func init() {
	InitLogger()
	InitClock()
}
