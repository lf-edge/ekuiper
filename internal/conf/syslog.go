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

//go:build !windows
// +build !windows

package conf

import (
	"log/syslog"
	"os"

	logrus_syslog "github.com/sirupsen/logrus/hooks/syslog"
)

func initSyslog() {
	if "true" == os.Getenv(KuiperSyslogKey) {
		if hook, err := logrus_syslog.NewSyslogHook("", "", syslog.LOG_INFO, ""); err != nil {
			Log.Error("Unable to connect to local syslog daemon")
		} else {
			Log.AddHook(hook)
		}
	}
}
