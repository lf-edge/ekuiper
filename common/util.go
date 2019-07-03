package common

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/sirupsen/logrus"
	"os"
	"time"
)

const LogLocation = "stream.log"

var (
	Log *logrus.Logger
	Env string

	logFile *os.File
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

func init(){
	flag.StringVar(&Env, "env", "dev", "set environment to prod or test")
	flag.Parse()
	Log = logrus.New()
	if Env == "prod"{
		logFile, err := os.OpenFile(LogLocation, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err == nil {
			Log.Out = logFile
		} else {
			Log.Infof("Failed to log to file, using default stderr")
		}
	}
}

func DbOpen(dir string) (*badger.DB, error) {
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
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
			err = errors.New(fmt.Sprintf("key %s already exist, delete it before creating a new one", key))
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

func GetConfLoc()(string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	confDir := dir + "/conf/"
	if _, err := os.Stat(confDir); os.IsNotExist(err) {
		return "", err
	}

	return confDir, nil
}


func GetDataLoc() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dataDir := dir + "/data/"

	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		if err := os.Mkdir(dataDir, os.ModePerm); err != nil {
			return "", fmt.Errorf("Find error %s when trying to locate xstream data folder.\n", err)
		}
	}

	return dataDir, nil
}

func TimeToUnixMilli(time time.Time) int64 {
	return time.UnixNano() / 1e6;
}