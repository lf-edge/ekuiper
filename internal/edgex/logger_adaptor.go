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

//go:build edgex

package edgex

import (
	"fmt"
	"io"

	"github.com/sirupsen/logrus"
)

const (
	OPENZITI_LOG_FORMAT         = "openziti: %s"
	OPENZITI_DEFAULT_LOG_FORMAT = "default openziti: %s"
)

func adaptLogging(log *logrus.Logger) {
	// with EdgeX enabled as of 2024, it includes OpenZiti support. OpenZiti uses the default
	// logrus logger. This quiets duplicative logging
	// Check if the hook is already added
	for _, hook := range log.Hooks[logrus.InfoLevel] {
		if _, ok := hook.(*LogrusAdaptor); ok {
			return
		}
	}
	hook := &LogrusAdaptor{
		lc: log,
	}
	logrus.StandardLogger().SetOutput(io.Discard)
	logrus.AddHook(hook)
}

type LogrusAdaptor struct {
	lc *logrus.Logger
}

func (f *LogrusAdaptor) Format(entry *logrus.Entry) ([]byte, error) {
	// Implement your custom formatting logic here
	return []byte(fmt.Sprintf("[%s] %s\n", entry.Level, entry.Message)), nil
}

func (f *LogrusAdaptor) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (f *LogrusAdaptor) Fire(e *logrus.Entry) error {
	switch e.Level {
	case logrus.DebugLevel:
		f.lc.Debugf(OPENZITI_LOG_FORMAT, e.Message)
	case logrus.InfoLevel:
		f.lc.Infof(OPENZITI_LOG_FORMAT, e.Message)
	case logrus.WarnLevel:
		f.lc.Warnf(OPENZITI_LOG_FORMAT, e.Message)
	case logrus.ErrorLevel:
		f.lc.Errorf(OPENZITI_LOG_FORMAT, e.Message)
	case logrus.FatalLevel:
		f.lc.Errorf(OPENZITI_LOG_FORMAT, e.Message)
	case logrus.PanicLevel:
		f.lc.Errorf(OPENZITI_LOG_FORMAT, e.Message)
	default:
		f.lc.Errorf(OPENZITI_DEFAULT_LOG_FORMAT, e.Message)
	}

	return nil
}
