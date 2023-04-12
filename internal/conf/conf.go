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
func (sc *SinkConf) Validate() error {
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

type SourceConf struct {
	HttpServerIp   string   `json:"httpServerIp" yaml:"httpServerIp"`
	HttpServerPort int      `json:"httpServerPort" yaml:"httpServerPort"`
	HttpServerTls  *tlsConf `json:"httpServerTls" yaml:"httpServerTls"`
}

func (sc *SourceConf) Validate() error {
	e := make(errorx.MultiError)
	if sc.HttpServerIp == "" {
		sc.HttpServerIp = "0.0.0.0"
	}
	if sc.HttpServerPort <= 0 || sc.HttpServerPort > 65535 {
		Log.Warnf("invalid source.httpServerPort configuration %d, set to 10081", sc.HttpServerPort)
		e["invalidHttpServerPort"] = fmt.Errorf("httpServerPort must between 0 and 65535")
		sc.HttpServerPort = 10081
	}
	return e
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
	Rule   api.RuleOption
	Sink   *SinkConf
	Source *SourceConf
	Store  struct {
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
		PythonBin   string `yaml:"pythonBin"`
		InitTimeout int    `yaml:"initTimeout"`
	}
	State struct {
		Backend string `yaml:"backend"`
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
			Restart: &api.RestartStrategy{
				Attempts:     0,
				Delay:        1000,
				Multiplier:   2,
				MaxDelay:     30000,
				JitterFactor: 0.1,
			},
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
	if Config.Portable.InitTimeout <= 0 {
		Config.Portable.InitTimeout = 5000
	}
	if Config.Source == nil {
		Config.Source = &SourceConf{}
	}
	_ = Config.Source.Validate()
	if Config.Sink == nil {
		Config.Sink = &SinkConf{}
	}
	_ = Config.Sink.Validate()

	_ = ValidateRuleOption(&Config.Rule)
}

func ValidateRuleOption(option *api.RuleOption) error {
	e := make(errorx.MultiError)
	if option.CheckpointInterval < 0 {
		option.CheckpointInterval = 0
		Log.Warnf("checkpointInterval is negative, set to 0")
		e["invalidCheckpointInterval"] = fmt.Errorf("checkpointInterval must be greater than 0")
	}
	if option.Concurrency < 0 {
		option.Concurrency = 1
		Log.Warnf("concurrency is negative, set to 1")
		e["invalidConcurrency"] = fmt.Errorf("concurrency must be greater than 0")
	}
	if option.BufferLength < 0 {
		option.BufferLength = 1024
		Log.Warnf("bufferLength is negative, set to 1024")
		e["invalidBufferLength"] = fmt.Errorf("bufferLength must be greater than 0")
	}
	if option.LateTol < 0 {
		option.LateTol = 1000
		Log.Warnf("lateTol is negative, set to 1000")
		e["invalidLateTol"] = fmt.Errorf("lateTol must be greater than 0")
	}
	if option.Restart != nil {
		if option.Restart.Multiplier <= 0 {
			option.Restart.Multiplier = 2
			Log.Warnf("restart multiplier is negative, set to 2")
			e["invalidRestartMultiplier"] = fmt.Errorf("restart multiplier must be greater than 0")
		}
		if option.Restart.Attempts < 0 {
			option.Restart.Attempts = 0
			Log.Warnf("restart attempts is negative, set to 0")
			e["invalidRestartAttempts"] = fmt.Errorf("restart attempts must be greater than 0")
		}
		if option.Restart.Delay <= 0 {
			option.Restart.Delay = 1000
			Log.Warnf("restart delay is negative, set to 1000")
			e["invalidRestartDelay"] = fmt.Errorf("restart delay must be greater than 0")
		}
		if option.Restart.MaxDelay <= 0 {
			option.Restart.MaxDelay = option.Restart.Delay
			Log.Warnf("restart maxDelay is negative, set to %d", option.Restart.Delay)
			e["invalidRestartMaxDelay"] = fmt.Errorf("restart maxDelay must be greater than 0")
		}
		if option.Restart.JitterFactor <= 0 || option.Restart.JitterFactor >= 1 {
			option.Restart.JitterFactor = 0.1
			Log.Warnf("restart jitterFactor must between 0 and 1, set to 0.1")
			e["invalidRestartJitterFactor"] = fmt.Errorf("restart jitterFactor must between [0, 1)")
		}
	}
	return e.GetError()
}

func init() {
	InitLogger()
	InitClock()
}
