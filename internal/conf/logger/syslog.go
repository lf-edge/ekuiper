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

//go:build !windows

package logger

import (
	"log/syslog"

	logrus_syslog "github.com/sirupsen/logrus/hooks/syslog"
)

func InitSyslog(network, address, level, tag string) error {
	p := syslog.LOG_INFO
	switch level {
	case "debug":
		p = syslog.LOG_DEBUG
	case "info":
		p = syslog.LOG_INFO
	case "warn":
		p = syslog.LOG_WARNING
	case "error":
		p = syslog.LOG_ERR
	default:
		p = syslog.LOG_INFO
	}
	if hook, err := logrus_syslog.NewSyslogHook(network, address, p, tag); err != nil {
		Log.Error("Unable to connect to local syslog daemon")
		return err
	} else {
		Log.Infof("Setting up syslog network %s, address %s, level %s, tag %s", network, address, level, tag)
		Log.AddHook(hook)
	}
	return nil
}
