// Copyright 2023-2024 EMQ Technologies Co., Ltd.
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

package logger

import (
	"io"
	"os"
	"strings"

	filename "github.com/keepeye/logrus-filename"
	"github.com/sirupsen/logrus"
)

var (
	Log       *logrus.Logger
	LogFile   *os.File
	IsTesting bool
)

const KuiperSyslogKey = "KuiperSyslogKey"

func init() {
	InitLogger()
}

func InitLogger() {
	if LogFile != nil {
		return
	}
	Log = logrus.New()
	Log.SetOutput(io.Discard)
	filenameHook := filename.NewHook()
	filenameHook.Field = "file"
	Log.AddHook(filenameHook)

	Log.SetFormatter(&logrus.TextFormatter{
		DisableColors: true,
		FullTimestamp: true,
	})
	Log.Debugf("init with args %s", os.Args)
	for _, arg := range os.Args {
		if strings.HasPrefix(arg, "-test.") {
			IsTesting = true
			break
		}
	}
}

func CloseLogger() {
	if LogFile != nil {
		LogFile.Close()
	}
}
