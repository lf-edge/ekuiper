package common

import (
	"bytes"
	"context"
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/go-yaml/yaml"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"path/filepath"
)

const (
	logFileName = "stream.log"
	LoggerKey = "logger"
	etc_dir = "/etc/"
	data_dir = "/data/"
	log_dir = "/log/"
)

var (
	Log *logrus.Logger
	Config *XStreamConf
	IsTesting bool
	logFile *os.File
	mockTicker *MockTicker
	mockTimer *MockTimer
	mockNow int64
)

type logRedirect struct {

}

func (l *logRedirect) Errorf(f string, v ...interface{}) {
	Log.Error(fmt.Sprintf(f, v...))
}

func (l *logRedirect) Infof(f string, v ...interface{}) {
	Log.Info(fmt.Sprintf(f, v...))
}

func (l *logRedirect) Warningf(f string, v ...interface{}) {
	Log.Warning(fmt.Sprintf(f, v...))
}

func (l *logRedirect) Debugf(f string, v ...interface{}) {
	Log.Debug(fmt.Sprintf(f, v...))
}

func GetLogger(ctx context.Context) *logrus.Entry {
	if ctx != nil{
		l, ok := ctx.Value(LoggerKey).(*logrus.Entry)
		if l != nil && ok {
			return l
		}
	}
	return Log.WithField("caller", "default")
}

func LoadConf(confName string) []byte {
	confDir, err := GetConfLoc()
	if err != nil {
		Log.Fatal(err)
	}

	file := confDir + confName
	b, err := ioutil.ReadFile(file)
	if err != nil {
		Log.Fatal(err)
	}
	return b
}

type XStreamConf struct {
	Debug bool `yaml:"debug"`
	Port int `yaml:"port"`
}

var StreamConf = "kuiper.yaml"

func init(){
	Log = logrus.New()
	Log.SetFormatter(&logrus.TextFormatter{
		DisableColors: true,
		FullTimestamp: true,
	})
	b := LoadConf(StreamConf)
	var cfg map[string]XStreamConf
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		Log.Fatal(err)
	}

	if c, ok := cfg["basic"]; !ok{
		Log.Fatal("no basic config in kuiper.yaml")
	}else{
		Config = &c
	}

	if !Config.Debug {
		logDir, err := GetLoc(log_dir)
		if err != nil {
			Log.Fatal(err)
		}
		file := logDir + logFileName
		logFile, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err == nil {
			Log.Out = logFile
		} else {
			Log.Infof("Failed to log to file, using default stderr")
		}
	}else{
		Log.SetLevel(logrus.DebugLevel)
	}
}

func DbOpen(dir string) (*badger.DB, error) {
	opts := badger.DefaultOptions(dir)
	opts.Logger = &logRedirect{}
	db, err := badger.Open(opts)
	return db, err
}

func DbClose(db *badger.DB) error {
	return db.Close()
}

func DbSet(db *badger.DB, key string, value string) error {

	err := db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(key))
		//key not found
		if err != nil {
			err = txn.Set([]byte(key), []byte(value))
		}else{
			err = fmt.Errorf("key %s already exist, delete it before creating a new one", key)
		}

		return err
	})
	return err
}

func DbGet(db *badger.DB, key string) (value string, err error) {
	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		err = item.Value(func(val []byte) error {
			value = string(val)
			return nil
		})
		return err
	})

	return
}

func DbDelete(db *badger.DB, key string) error {
	err := db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(key))
		//key not found
		if err != nil {
			return err
		}else{
			err = txn.Delete([]byte(key))
		}
		return err
	})
	return err
}

func DbKeys(db *badger.DB) (keys []string, err error) {
	err = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			keys = append(keys, string(k))
		}
		return nil
	})
	return
}

func PrintMap(m map[string]string, buff *bytes.Buffer) {

	for k, v := range m {
		buff.WriteString(fmt.Sprintf("%s: %s\n", k, v))
	}
}

func CloseLogger(){
	if logFile != nil {
		logFile.Close()
	}
}

func GetConfLoc()(string, error){
	return GetLoc(etc_dir)
}

func GetDataLoc() (string, error) {
	return GetLoc(data_dir)
}

func GetLoc(subdir string)(string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	confDir := dir + subdir
	if _, err := os.Stat(confDir); os.IsNotExist(err) {
		lastdir := dir
		for len(dir) > 0 {
			dir = filepath.Dir(dir)
			if lastdir == dir {
				break
			}
			confDir = dir + subdir
			if _, err := os.Stat(confDir); os.IsNotExist(err) {
				lastdir = dir
				continue
			} else {
				//Log.Printf("Trying to load file from %s", confDir)
				return confDir, nil
			}
		}
	} else {
		//Log.Printf("Trying to load file from %s", confDir)
		return confDir, nil
	}

	return "", fmt.Errorf("conf dir not found")
}

//Time related. For Mock
func GetTicker(duration int) Ticker {
	if IsTesting{
		if mockTicker == nil{
			mockTicker = NewMockTicker(duration)
		}else{
			mockTicker.SetDuration(duration)
		}
		return mockTicker
	}else{
		return NewDefaultTicker(duration)
	}
}

func GetTimer(duration int) Timer {
	if IsTesting{
		if mockTimer == nil{
			mockTimer = NewMockTimer(duration)
		}else{
			mockTimer.SetDuration(duration)
		}
		return mockTimer
	}else{
		return NewDefaultTimer(duration)
	}
}


/****** For Test Only ********/
func GetMockTicker() *MockTicker{
	return mockTicker
}

func ResetMockTicker(){
	if mockTicker != nil{
		mockTicker.lastTick = 0
	}
}

func GetMockTimer() *MockTimer{
	return mockTimer
}

func SetMockNow(now int64){
	mockNow = now
}

func GetMockNow() int64{
	return mockNow
}

/*********** Type Cast Utilities *****/
//TODO datetime type
func ToString(input interface{}) string{
	return fmt.Sprintf("%v", input)
}
func ToInt(input interface{}) (int, error){
	switch t := input.(type) {
	case float64:
		return int(t), nil
	case int64:
		return int(t), nil
	case int:
		return t, nil
	default:
		return 0, fmt.Errorf("unsupported type %T of %[1]v", input)
	}
}
