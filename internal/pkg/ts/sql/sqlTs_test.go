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

package sql

import (
	"github.com/lf-edge/ekuiper/internal/pkg/db/sql/sqlite"
	"github.com/lf-edge/ekuiper/internal/pkg/ts/test/common"
	ts2 "github.com/lf-edge/ekuiper/pkg/kv"
	"os"
	"path"
	"path/filepath"
	"testing"
)

const (
	DbName = "sqliteTS.db"
	Table  = "test"
)

func TestSqlTsSet(t *testing.T) {
	ks, db, abs := setupSqlKv()
	defer cleanSqlKv(db, abs)

	common.TestTsSet(ks, t)
}

func TestSqlTsLast(t *testing.T) {
	ks, db, abs := setupSqlKv()
	defer cleanSqlKv(db, abs)

	common.TestTsLast(ks, t)
}

func TestSqlTsGet(t *testing.T) {
	ks, db, abs := setupSqlKv()
	defer cleanSqlKv(db, abs)

	common.TestTsGet(ks, t)
}

func TestSqlTsDelete(t *testing.T) {
	ks, db, abs := setupSqlKv()
	defer cleanSqlKv(db, abs)

	common.TestTsDelete(ks, t)
}

func TestSqlTsDeleteBefore(t *testing.T) {
	ks, db, abs := setupSqlKv()
	defer cleanSqlKv(db, abs)

	common.TestTsDeleteBefore(ks, t)
}

func deleteIfExists(abs string) error {
	absPath := path.Join(abs, DbName)
	if f, _ := os.Stat(absPath); f != nil {
		return os.Remove(absPath)
	}
	return nil
}

func setupSqlKv() (ts2.Tskv, *sqlite.Database, string) {
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

	builder := NewTsBuilder(db)
	if err != nil {
		panic(err)
	}
	var store ts2.Tskv
	err, store = builder.CreateTs(Table)
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
