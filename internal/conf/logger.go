package conf

import (
	filename "github.com/keepeye/logrus-filename"
	"github.com/sirupsen/logrus"
	"os"
	"strings"
)

const (
	logFileName = "stream.log"
)

var (
	Log     *logrus.Logger
	logFile *os.File
)

func InitLogger() {
	Log = logrus.New()
	initSyslog()
	filenameHook := filename.NewHook()
	filenameHook.Field = "file"
	Log.AddHook(filenameHook)

	Log.SetFormatter(&logrus.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		DisableColors:   true,
		FullTimestamp:   true,
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
	if logFile != nil {
		logFile.Close()
	}
}
