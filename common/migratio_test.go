package common

import (
	"github.com/patrickmn/go-cache"
	"os"
	"path"
	"path/filepath"
	"testing"
)

func TestDataMigration(t *testing.T) {
	kvs := make(map[string]string)
	kvs["mqtt"] = `{"sql":"create stream mqtt(age BIGINT) WITH (DATASOURCE = \"dev/+/msg\", FORMAT = \"json\");"}`
	kvs["log"] = `{"id":"log","sql":"SELECT  * FROM mqtt","actions":[{"log":{}}]}`

	dir, _ := filepath.Abs("testMigration")
	dirSqlite, _ := filepath.Split(dir)
	fpath := path.Join(dir, "stores.data")
	if f, _ := os.Stat(fpath); f != nil {
		os.Remove(fpath)
	}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, os.ModePerm)
	}
	defer os.RemoveAll(dir)
	defer os.Remove(path.Join(dirSqlite, "sqliteKV.db"))

	c := cache.New(cache.NoExpiration, 0)
	if err := gocacheOpen(c, fpath); nil != err {
		t.Error(err)
		return
	}

	for k, v := range kvs {
		if err := gocacheSet(c, fpath, k, v); nil != err {
			t.Error(err)
			return
		}
	}
	if err := gocacheClose(c, fpath); nil != err {
		t.Error(err)
	}

	if err := DataMigration(dir); nil != err {
		t.Error(err)
		return
	}

	store := GetSqliteKVStore(dir)
	if err := store.Open(); nil != err {
		t.Error(err)
		return
	}
	defer store.Close()
	for k, v := range kvs {
		var dbVal string
		if ok, _ := store.Get(k, &dbVal); !ok {
			t.Error("not found key ", k)
			return
		} else if v != dbVal {
			t.Error("gocache save:", v, "sqlite save:", dbVal)
			return
		}
	}

}
