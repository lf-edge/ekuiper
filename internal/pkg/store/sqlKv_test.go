// Copyright 2021 INTECH Process Automation Ltd.
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

package store

import (
	"github.com/lf-edge/ekuiper/internal/pkg/db/sql/sqlite"
	sb "github.com/lf-edge/ekuiper/internal/pkg/store/sql"
	"github.com/lf-edge/ekuiper/internal/pkg/store/test/common"
	st "github.com/lf-edge/ekuiper/pkg/kv/stores"
	"os"
	"path"
	"path/filepath"
	"testing"
)

const DbName = "sqliteKV.db"
const Table = "test"

func TestSqlKvSetnx(t *testing.T) {
	ks, db, abs := setupSqlKv()
	defer cleanSqlKv(db, abs)
	common.TestKvSetnx(ks, t)
}

func TestSqlKvSet(t *testing.T) {
	ks, db, abs := setupSqlKv()
	defer cleanSqlKv(db, abs)
	common.TestKvSet(ks, t)
}

func TestSqlKvGet(t *testing.T) {
	ks, db, abs := setupSqlKv()
	defer cleanSqlKv(db, abs)
	common.TestKvGet(ks, t)
}

func TestSqlKvKeys(t *testing.T) {
	ks, db, abs := setupSqlKv()
	defer cleanSqlKv(db, abs)

	length := 10
	common.TestKvKeys(length, ks, t)
}

func deleteIfExists(abs string) error {
	absPath := path.Join(abs, DbName)
	if f, _ := os.Stat(absPath); f != nil {
		return os.Remove(absPath)
	}
	return nil
}

func setupSqlKv() (st.KeyValue, *sqlite.Database, string) {
	absPath, err := filepath.Abs("test")
	if err != nil {
		panic(err)
	}
	err = deleteIfExists(absPath)
	if err != nil {
		panic(err)
	}
	config := sqlite.Config{
		Path: absPath,
		Name: DbName,
	}
	_, db := sqlite.NewSqliteDatabase(config)
	err = db.Connect()
	if err != nil {
		panic(err)
	}

	builder := sb.NewStoreBuilder(db)
	var store st.KeyValue
	err, store = builder.CreateStore(Table)
	if err != nil {
		panic(err)
	}
	return store, db, absPath
}

func cleanSqlKv(db *sqlite.Database, abs string) {
	if err := db.Disconnect(); err != nil {
		panic(err)
	}
	if err := deleteIfExists(abs); err != nil {
		panic(err)
	}
}
