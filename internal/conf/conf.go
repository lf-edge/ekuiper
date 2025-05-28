// Copyright 2023-2025 EMQ Technologies Co., Ltd.
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
	"encoding/base64"
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

	"github.com/lf-edge/ekuiper/v2/internal/conf/logger"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/schedule"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
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
	Config    *model.KuiperConf
	IsTesting bool
	TestId    string
)

func InitConf() {
	cpath, err := GetConfLoc()
	if err != nil {
		panic(err)
	}
	kc := model.KuiperConf{
		Rule: def.RuleOption{
			LateTol:            cast.DurationConf(time.Second),
			Concurrency:        1,
			BufferLength:       1024,
			CheckpointInterval: cast.DurationConf(5 * time.Minute), // 5 minutes
			SendError:          false,
			RestartStrategy: &def.RestartStrategy{
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

	if time.Duration(Config.Basic.RulePatrolInterval) < time.Second {
		Log.Warnf("rule patrol interval %v is less than 1 second, set it to 10 seconds", Config.Basic.RulePatrolInterval)
		Config.Basic.RulePatrolInterval = cast.DurationConf(10 * time.Second)
	}

	if time.Duration(Config.Connection.BackoffMaxElapsedDuration) < 1 {
		Config.Connection.BackoffMaxElapsedDuration = cast.DurationConf(3 * time.Minute)
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
			c = &model.SyslogConf{
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

	if time.Duration(Config.Basic.GracefulShutdownTimeout) < 1 {
		Config.Basic.GracefulShutdownTimeout = cast.DurationConf(3 * time.Second)
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

	if Config.Basic.AesKey != "" {
		key, err := base64.StdEncoding.DecodeString(Config.Basic.AesKey)
		if err != nil {
			Log.Fatal(err)
		}
		Config.AesKey = key
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
	if Config.Portable.SendTimeout <= 0 {
		Config.Portable.SendTimeout = 5 * time.Second
	}
	if Config.Portable.RecvTimeout <= 0 {
		Config.Portable.RecvTimeout = 5 * time.Second
	}
	if Config.Source == nil {
		Config.Source = &model.SourceConf{}
	}

	if Config.Basic.MetricsDumpConfig.RetainedDuration < 1 {
		Config.Basic.MetricsDumpConfig.RetainedDuration = 6 * time.Hour
	}

	_ = Config.Source.Validate(Log)
	if Config.Sink == nil {
		Config.Sink = &model.SinkConf{}
	}
	_ = Config.Sink.Validate(Log)

	if Config.Basic.Syslog != nil {
		_ = Config.Basic.Syslog.Validate()
	}

	if Config.OpenTelemetry.RemoteEndpoint == "" {
		Config.OpenTelemetry.RemoteEndpoint = "localhost:4318"
	}

	if Config.OpenTelemetry.LocalTraceCapacity < 1 {
		Config.OpenTelemetry.LocalTraceCapacity = 2048
	}

	_ = ValidateRuleOption(&Config.Rule)
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
	} else {
		Log.SetOutput(logWriter)
	}
	if Config.Basic.RotateCount > 0 {
		// gc outdated log files by logrus itself
	} else if Config.Basic.MaxAge > 0 {
		gcOutdatedLog(logDir, time.Hour*time.Duration(Config.Basic.MaxAge))
	}
	return nil
}

func SetLogFormat(disableTimestamp bool) {
	Log.Formatter.(*logrus.TextFormatter).DisableTimestamp = disableTimestamp
}

func ValidateRuleOption(option *def.RuleOption) error {
	var errs error
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
		option.LateTol = cast.DurationConf(time.Second)
		Log.Warnf("lateTol is negative, set to 1 second")
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
