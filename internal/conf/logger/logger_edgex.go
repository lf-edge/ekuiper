//go:build edgex

package logger

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
)

const OPENZITI_LOG_FORMAT = "openziti: %s"
const OPENZITI_DEFAULT_LOG_FORMAT = "default openziti: %s"

func adaptLogging(log *logrus.Logger) {
	// with EdgeX enabled as of 2024, it includes OpenZiti support. OpenZiti uses the default
	// logrus logger. This quiets duplicative logging
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
