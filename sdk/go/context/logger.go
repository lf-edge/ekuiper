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

package context

import (
	filename "github.com/keepeye/logrus-filename"
	"github.com/sirupsen/logrus"

	"github.com/lf-edge/ekuiper/sdk/go/api"
)

var (
	Log *logrus.Logger
)

func init() {
	Log = logrus.New()
	filenameHook := filename.NewHook()
	filenameHook.Field = "file"
	Log.AddHook(filenameHook)
	Log.SetFormatter(&logrus.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		DisableColors:   true,
		FullTimestamp:   true,
	})
	//Log.Level = logrus.DebugLevel
	Log.WithField("type", "plugin")
}

func LogEntry(key string, value interface{}) api.Logger {
	return Log.WithField(key, value)
}
