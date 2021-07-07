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

package util

import (
	"github.com/lf-edge/ekuiper/pkg/kv"
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

	store := kv.GetDefaultKVStore(dir)
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
