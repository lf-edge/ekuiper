// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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

package sql

import (
	"github.com/lf-edge/ekuiper/internal/pkg/store/definition"
	"github.com/lf-edge/ekuiper/internal/pkg/store/sql/sqlite"
	"github.com/lf-edge/ekuiper/internal/pkg/store/test/common"
	"github.com/lf-edge/ekuiper/pkg/kv"
	"os"
	"path"
	"path/filepath"
	"testing"
)

const SDbName = "sqliteKV.db"
const STable = "test"

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

func TestSqlKvSetGet(t *testing.T) {
	ks, db, abs := setupSqlKv()
	defer cleanSqlKv(db, abs)
	common.TestKvSetGet(ks, t)
}

func TestSqlKvKeys(t *testing.T) {
	ks, db, abs := setupSqlKv()
	defer cleanSqlKv(db, abs)

	length := 10
	common.TestKvKeys(length, ks, t)
}

func TestSqlKvAll(t *testing.T) {
	ks, db, abs := setupSqlKv()
	defer cleanSqlKv(db, abs)

	length := 10
	common.TestKvAll(length, ks, t)
}

func TestSqlKvGetKeyedState(t *testing.T) {
	ks, db, abs := setupSqlKv()
	defer cleanSqlKv(db, abs)

	common.TestKvGetKeyedState(ks, t)
}

func deleteIfExists(abs string) error {
	absPath := path.Join(abs, SDbName)
	if f, _ := os.Stat(absPath); f != nil {
		return os.Remove(absPath)
	}
	return nil
}

func setupSqlKv() (kv.KeyValue, definition.Database, string) {
	absPath, err := filepath.Abs("test")
	if err != nil {
		panic(err)
	}
	err = deleteIfExists(absPath)
	if err != nil {
		panic(err)
	}
	config := definition.Config{
		Type:  "sqlite",
		Redis: definition.RedisConfig{},
		Sqlite: definition.SqliteConfig{
			Path: absPath,
			Name: SDbName,
		},
	}

	db, _ := sqlite.NewSqliteDatabase(config, "sqliteKV.db")
	err = db.Connect()
	if err != nil {
		panic(err)
	}

	builder := NewStoreBuilder(db.(Database))
	var store kv.KeyValue
	store, err = builder.CreateStore(STable)
	if err != nil {
		panic(err)
	}
	return store, db, absPath
}

func cleanSqlKv(db definition.Database, abs string) {
	if err := db.Disconnect(); err != nil {
		panic(err)
	}
	if err := deleteIfExists(abs); err != nil {
		panic(err)
	}
}
