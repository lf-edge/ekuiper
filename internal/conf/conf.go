// Copyright 2023 EMQ Technologies Co., Ltd.
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
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yisaer/file-rotatelogs"

	"github.com/lf-edge/ekuiper/internal/conf/logger"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/schedule"
)

const (
	ConfFileName  = "kuiper.yaml"
	DebugLogLevel = "debug"
	InfoLogLevel  = "info"
	WarnLogLevel  = "warn"
	ErrorLogLevel = "error"
	FatalLogLevel = "fatal"
	PanicLogLevel = "panic"
)

var (
	Config    *KuiperConf
	IsTesting bool
	TestId    string
)

type tlsConf struct {
	Certfile string `yaml:"certfile"`
	Keyfile  string `yaml:"keyfile"`
}

type SinkConf struct {
	MemoryCacheThreshold int    `json:"memoryCacheThreshold" yaml:"memoryCacheThreshold"`
	MaxDiskCache         int    `json:"maxDiskCache" yaml:"maxDiskCache"`
	BufferPageSize       int    `json:"bufferPageSize" yaml:"bufferPageSize"`
	EnableCache          bool   `json:"enableCache" yaml:"enableCache"`
	ResendInterval       int    `json:"resendInterval" yaml:"resendInterval"`
	CleanCacheAtStop     bool   `json:"cleanCacheAtStop" yaml:"cleanCacheAtStop"`
	ResendAlterQueue     bool   `json:"resendAlterQueue" yaml:"resendAlterQueue"`
	ResendPriority       int    `json:"resendPriority" yaml:"resendPriority"`
	ResendIndicatorField string `json:"resendIndicatorField" yaml:"resendIndicatorField"`
}

// Validate the configuration and reset to the default value for invalid values.
func (sc *SinkConf) Validate() error {
	var errs error
	if sc.MemoryCacheThreshold < 0 {
		sc.MemoryCacheThreshold = 1024
		Log.Warnf("memoryCacheThreshold is less than 0, set to 1024")
		errs = errors.Join(errs, errors.New("memoryCacheThreshold:memoryCacheThreshold must be positive"))
	}
	if sc.MaxDiskCache < 0 {
		sc.MaxDiskCache = 1024000
		Log.Warnf("maxDiskCache is less than 0, set to 1024000")
		errs = errors.Join(errs, errors.New("maxDiskCache:maxDiskCache must be positive"))
	}
	if sc.BufferPageSize <= 0 {
		sc.BufferPageSize = 256
		Log.Warnf("bufferPageSize is less than or equal to 0, set to 256")
		errs = errors.Join(errs, errors.New("bufferPageSize:bufferPageSize must be positive"))
	}
	if sc.ResendInterval < 0 {
		sc.ResendInterval = 0
		Log.Warnf("resendInterval is less than 0, set to 0")
		errs = errors.Join(errs, errors.New("resendInterval:resendInterval must be positive"))
	}
	if sc.BufferPageSize > sc.MemoryCacheThreshold {
		sc.MemoryCacheThreshold = sc.BufferPageSize
		Log.Warnf("memoryCacheThreshold is less than bufferPageSize, set to %d", sc.BufferPageSize)
		errs = errors.Join(errs, errors.New("memoryCacheThresholdTooSmall:memoryCacheThreshold must be greater than or equal to bufferPageSize"))
	}
	if sc.MemoryCacheThreshold%sc.BufferPageSize != 0 {
		sc.MemoryCacheThreshold = sc.BufferPageSize * (sc.MemoryCacheThreshold/sc.BufferPageSize + 1)
		Log.Warnf("memoryCacheThreshold is not a multiple of bufferPageSize, set to %d", sc.MemoryCacheThreshold)
		errs = errors.Join(errs, errors.New("memoryCacheThresholdNotMultiple:memoryCacheThreshold must be a multiple of bufferPageSize"))
	}
	if sc.BufferPageSize > sc.MaxDiskCache {
		sc.MaxDiskCache = sc.BufferPageSize
		Log.Warnf("maxDiskCache is less than bufferPageSize, set to %d", sc.BufferPageSize)
		errs = errors.Join(errs, errors.New("maxDiskCacheTooSmall:maxDiskCache must be greater than bufferPageSize"))
	}
	if sc.MaxDiskCache%sc.BufferPageSize != 0 {
		sc.MaxDiskCache = sc.BufferPageSize * (sc.MaxDiskCache/sc.BufferPageSize + 1)
		Log.Warnf("maxDiskCache is not a multiple of bufferPageSize, set to %d", sc.MaxDiskCache)
		errs = errors.Join(errs, errors.New("maxDiskCacheNotMultiple:maxDiskCache must be a multiple of bufferPageSize"))
	}
	if sc.ResendPriority < -1 || sc.ResendPriority > 1 {
		sc.ResendPriority = 0
		Log.Warnf("resendPriority is not in [-1, 1], set to 0")
		errs = errors.Join(errs, errors.New("resendPriority:resendPriority must be -1, 0 or 1"))
	}
	return errs
}

type SourceConf struct {
	HttpServerIp   string   `json:"httpServerIp" yaml:"httpServerIp"`
	HttpServerPort int      `json:"httpServerPort" yaml:"httpServerPort"`
	HttpServerTls  *tlsConf `json:"httpServerTls" yaml:"httpServerTls"`
}

func (sc *SourceConf) Validate() error {
	var errs error
	if sc.HttpServerIp == "" {
		sc.HttpServerIp = "0.0.0.0"
	}
	if sc.HttpServerPort <= 0 || sc.HttpServerPort > 65535 {
		Log.Warnf("invalid source.httpServerPort configuration %d, set to 10081", sc.HttpServerPort)
		errs = errors.Join(errs, errors.New("invalidHttpServerPort:httpServerPort must between 0 and 65535"))
		sc.HttpServerPort = 10081
	}
	return errs
}

type SQLConf struct {
	MaxConnections int `yaml:"maxConnections"`
}

type syslogConf struct {
	Enable  bool   `yaml:"enable"`
	Network string `yaml:"network"`
	Address string `yaml:"address"`
	Tag     string `yaml:"tag"`
	Level   string `yaml:"level"`
}

func (s *syslogConf) Validate() error {
	if s.Network == "" {
		s.Network = "udp"
	}
	if s.Level == "" {
		s.Level = "info"
	}
	switch s.Level {
	case "debug", "info", "warn", "error":
		// valid, do nothing
	default:
		return fmt.Errorf("invalid syslog level: %s", s.Level)
	}
	return nil
}

type KuiperConf struct {
	Basic struct {
		LogLevel            string      `yaml:"logLevel"`
		Debug               bool        `yaml:"debug"`
		ConsoleLog          bool        `yaml:"consoleLog"`
		FileLog             bool        `yaml:"fileLog"`
		LogDisableTimestamp bool        `yaml:"logDisableTimestamp"`
		Syslog              *syslogConf `yaml:"syslog"`
		RotateTime          int         `yaml:"rotateTime"`
		MaxAge              int         `yaml:"maxAge"`
		RotateSize          int64       `yaml:"rotateSize"`
		RotateCount         int         `yaml:"rotateCount"`
		TimeZone            string      `yaml:"timezone"`
		Ip                  string      `yaml:"ip"`
		Port                int         `yaml:"port"`
		RestIp              string      `yaml:"restIp"`
		RestPort            int         `yaml:"restPort"`
		RestTls             *tlsConf    `yaml:"restTls"`
		Prometheus          bool        `yaml:"prometheus"`
		PrometheusPort      int         `yaml:"prometheusPort"`
		PluginHosts         string      `yaml:"pluginHosts"`
		Authentication      bool        `yaml:"authentication"`
		IgnoreCase          bool        `yaml:"ignoreCase"`
		SQLConf             *SQLConf    `yaml:"sql"`
		RulePatrolInterval  string      `yaml:"rulePatrolInterval"`
		CfgStorageType      string      `yaml:"cfgStorageType"`
		EnableOpenZiti      bool        `yaml:"enableOpenZiti"`
	}
	Rule   api.RuleOption
	Sink   *SinkConf
	Source *SourceConf
	Store  struct {
		Type         string `yaml:"type"`
		ExtStateType string `yaml:"extStateType"`
		Redis        struct {
			Host               string `yaml:"host"`
			Port               int    `yaml:"port"`
			Password           string `yaml:"password"`
			Timeout            int    `yaml:"timeout"`
			ConnectionSelector string `yaml:"connectionSelector"`
		}
		Sqlite struct {
			Name string `yaml:"name"`
		}
		Fdb struct {
			Path string `yaml:"path"`
		}
	}
	Portable struct {
		PythonBin   string `yaml:"pythonBin"`
		InitTimeout int    `yaml:"initTimeout"`
	}
}

func SetLogLevel(level string, debug bool) {
	if debug {
		Log.SetLevel(logrus.DebugLevel)
		return
	}
	switch level {
	case DebugLogLevel:
		Log.SetLevel(logrus.DebugLevel)
	case InfoLogLevel:
		Log.SetLevel(logrus.InfoLevel)
	case WarnLogLevel:
		Log.SetLevel(logrus.WarnLevel)
	case ErrorLogLevel:
		Log.SetLevel(logrus.ErrorLevel)
	case FatalLogLevel:
		Log.SetLevel(logrus.FatalLevel)
	case PanicLogLevel:
		Log.SetLevel(logrus.PanicLevel)
	}
}

func SetConsoleAndFileLog(consoleLog, fileLog bool) error {
	if !fileLog {
		if consoleLog {
			Log.SetOutput(os.Stdout)
		}
		return nil
	}

	logDir, err := GetLogLoc()
	if err != nil {
		return err
	}

	file := path.Join(logDir, logFileName)
	ro := []rotatelogs.Option{
		rotatelogs.WithRotationTime(time.Hour * time.Duration(Config.Basic.RotateTime)),
		rotatelogs.WithRotationSize(Config.Basic.RotateSize),
	}
	if Config.Basic.RotateCount > 0 {
		ro = append(ro, rotatelogs.WithRotationCount(uint(Config.Basic.RotateCount)))
	} else if Config.Basic.MaxAge > 0 {
		ro = append(ro, rotatelogs.WithMaxAge(time.Hour*time.Duration(Config.Basic.MaxAge)))
	}
	if !strings.EqualFold(runtime.GOOS, "windows") {
		ro = append(ro, rotatelogs.WithLinkName(file))
	}
	logWriter, err := rotatelogs.New(
		file[:len(file)-len(filepath.Ext(file))]+".%Y-%m-%dT%H-%M-%S"+filepath.Ext(file),
		ro...,
	)

	if err != nil {
		fmt.Printf("Failed to init log file settings: %v", err)
		Log.Infof("Failed to log to file, using default stderr.")
	} else if consoleLog {
		mw := io.MultiWriter(os.Stdout, logWriter)
		Log.SetOutput(mw)
	} else if !consoleLog {
		Log.SetOutput(logWriter)
	}
	if Config.Basic.RotateCount > 0 {
		// gc outdated log files by logrus itself
	} else if Config.Basic.MaxAge > 0 {
		gcOutdatedLog(logDir, time.Hour*time.Duration(Config.Basic.MaxAge))
	}
	return nil
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
			CheckpointInterval: 300000, // 5 minutes
			SendError:          true,
			RestartStrategy: &api.RestartStrategy{
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

	if len(Config.Basic.RulePatrolInterval) < 1 {
		Config.Basic.RulePatrolInterval = "10s"
	}

	if Config.Basic.LogLevel == "" {
		Config.Basic.LogLevel = InfoLogLevel
	}
	SetLogLevel(Config.Basic.LogLevel, Config.Basic.Debug)
	SetLogFormat(Config.Basic.LogDisableTimestamp)
	if err := SetConsoleAndFileLog(Config.Basic.ConsoleLog, Config.Basic.FileLog); err != nil {
		log.Fatal(err)
	}
	if os.Getenv(logger.KuiperSyslogKey) == "true" || Config.Basic.Syslog != nil {
		c := Config.Basic.Syslog
		if c == nil {
			c = &syslogConf{
				Enable: true,
			}
		}
		// Init when env is set OR enable is true
		if c.Enable {
			err := logger.InitSyslog(c.Network, c.Address, c.Level, c.Tag)
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	if Config.Basic.TimeZone != "" {
		if err := cast.SetTimeZone(Config.Basic.TimeZone); err != nil {
			Log.Fatal(err)
		}
	} else {
		if err := cast.SetTimeZone("Local"); err != nil {
			Log.Fatal(err)
		}
	}

	if Config.Store.Type == "redis" && Config.Store.Redis.ConnectionSelector != "" {
		if err := RedisStorageConSelectorApply(Config.Store.Redis.ConnectionSelector, Config); err != nil {
			Log.Fatal(err)
		}
	}
	if Config.Store.ExtStateType == "" {
		Config.Store.ExtStateType = "sqlite"
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
	if Config.Basic.CfgStorageType == "" {
		Config.Basic.CfgStorageType = "file"
	}

	_ = Config.Source.Validate()
	if Config.Sink == nil {
		Config.Sink = &SinkConf{}
	}
	_ = Config.Sink.Validate()

	if Config.Basic.Syslog != nil {
		_ = Config.Basic.Syslog.Validate()
	}

	_ = ValidateRuleOption(&Config.Rule)
}

func SetLogFormat(disableTimestamp bool) {
	Log.Formatter.(*logrus.TextFormatter).DisableTimestamp = disableTimestamp
}

func ValidateRuleOption(option *api.RuleOption) error {
	var errs error
	if option.CheckpointInterval < 0 {
		option.CheckpointInterval = 0
		Log.Warnf("checkpointInterval is negative, set to 0")
		errs = errors.Join(errs, errors.New("invalidCheckpointInterval:checkpointInterval must be greater than 0"))
	}
	if option.Concurrency < 0 {
		option.Concurrency = 1
		Log.Warnf("concurrency is negative, set to 1")
		errs = errors.Join(errs, errors.New("invalidConcurrency:concurrency must be greater than 0"))
	}
	if option.BufferLength < 0 {
		option.BufferLength = 1024
		Log.Warnf("bufferLength is negative, set to 1024")
		errs = errors.Join(errs, errors.New("invalidBufferLength:bufferLength must be greater than 0"))
	}
	if option.LateTol < 0 {
		option.LateTol = 1000
		Log.Warnf("lateTol is negative, set to 1000")
		errs = errors.Join(errs, errors.New("invalidLateTol:lateTol must be greater than 0"))
	}
	if option.RestartStrategy != nil {
		if option.RestartStrategy.Multiplier <= 0 {
			option.RestartStrategy.Multiplier = 2
			Log.Warnf("restart multiplier is negative, set to 2")
			errs = errors.Join(errs, errors.New("invalidRestartMultiplier:restart multiplier must be greater than 0"))
		}
		if option.RestartStrategy.Attempts < 0 {
			option.RestartStrategy.Attempts = 0
			Log.Warnf("restart attempts is negative, set to 0")
			errs = errors.Join(errs, errors.New("invalidRestartAttempts:restart attempts must be greater than 0"))
		}
		if option.RestartStrategy.Delay <= 0 {
			option.RestartStrategy.Delay = 1000
			Log.Warnf("restart delay is negative, set to 1000")
			errs = errors.Join(errs, errors.New("invalidRestartDelay:restart delay must be greater than 0"))
		}
		if option.RestartStrategy.MaxDelay <= 0 {
			option.RestartStrategy.MaxDelay = option.RestartStrategy.Delay
			Log.Warnf("restart maxDelay is negative, set to %d", option.RestartStrategy.Delay)
			errs = errors.Join(errs, errors.New("invalidRestartMaxDelay:restart maxDelay must be greater than 0"))
		}
		if option.RestartStrategy.JitterFactor <= 0 || option.RestartStrategy.JitterFactor >= 1 {
			option.RestartStrategy.JitterFactor = 0.1
			Log.Warnf("restart jitterFactor must between 0 and 1, set to 0.1")
			errs = errors.Join(errs, errors.New("invalidRestartJitterFactor:restart jitterFactor must between [0, 1)"))
		}
	}
	if err := schedule.ValidateRanges(option.CronDatetimeRange); err != nil {
		errs = errors.Join(errs, fmt.Errorf("validate cronDatetimeRange failed, err:%v", err))
	}
	return errs
}

func init() {
	logger.Log.Debugf("conf init")
	IsTesting = logger.IsTesting
	InitClock()
}

func gcOutdatedLog(filePath string, maxDuration time.Duration) {
	entries, err := os.ReadDir(filePath)
	if err != nil {
		Log.Errorf("gc outdated logs when started failed, err:%v", err)
	}
	now := time.Now()
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if isLogOutdated(entry.Name(), now, maxDuration) {
			err := os.Remove(path.Join(filePath, entry.Name()))
			if err != nil {
				Log.Errorf("remove outdated log %v failed, err:%v", entry.Name(), err)
			}
		}
	}
}

func isLogOutdated(name string, now time.Time, maxDuration time.Duration) bool {
	if name == logFileName {
		return false
	}
	layout := ".2006-01-02_15-04-05"
	logDateExt := path.Ext(name)
	if t, err := time.Parse(layout, logDateExt); err != nil {
		Log.Errorf("parse log %v datetime failed, err:%v", name, err)
		return false
	} else if int64(now.Sub(t))-int64(maxDuration) > 0 {
		return true
	}
	return false
}
