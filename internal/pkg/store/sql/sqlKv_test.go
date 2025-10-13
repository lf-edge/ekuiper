// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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
	"bytes"
	"encoding/gob"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/store/definition"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store/sql/sqlite"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store/test/common"
	"github.com/lf-edge/ekuiper/v2/pkg/kv"
)

const (
	SDbName = "sqliteKV.db"
	STable  = "test"
)

func TestSqlGetSetDelete(t *testing.T) {
	ks, db, abs := setupSqlKv()
	defer cleanSqlKv(db, abs)
	require.NoError(t, ks.Set("pk1", "pv1"))
	var val string
	ok, err := ks.Get("pk1", &val)
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, "pv1", val)

	require.NoError(t, ks.Set("pk1", "pv2"))
	ok, err = ks.Get("pk1", &val)
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, "pv2", val)

	require.NoError(t, ks.Delete("pk1"))
	ok, err = ks.Get("pk1", &val)
	require.False(t, ok)
	require.NoError(t, err)

	require.NoError(t, ks.Set("pk2", "pv2"))
	ok, err = ks.Get("pk2", &val)
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, "pv2", val)

	require.NoError(t, ks.Delete("pk2"))
	ok, err = ks.Get("pk2", &val)
	require.False(t, ok)
	require.NoError(t, err)
}

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

func TestSqlGetByPrefix(t *testing.T) {
	ks, db, abs := setupSqlKv()
	defer cleanSqlKv(db, abs)
	require.NoError(t, ks.Set("prefix1", int64(1)))
	require.NoError(t, ks.Set("prefix2", int64(1)))
	m, err := ks.GetByPrefix("prefix")
	require.NoError(t, err)
	k1, ok := m["prefix1"]
	require.True(t, ok)
	dec := gob.NewDecoder(bytes.NewBuffer(k1))
	var v1 int64
	require.NoError(t, dec.Decode(&v1))
	require.Equal(t, int64(1), v1)
	k1, ok = m["prefix2"]
	require.True(t, ok)
	dec = gob.NewDecoder(bytes.NewBuffer(k1))
	require.NoError(t, dec.Decode(&v1))
	require.Equal(t, int64(1), v1)
}

func TestInvalidTableName(t *testing.T) {
	absPath, err := filepath.Abs("test")
	require.NoError(t, err)
	err = deleteIfExists(absPath)
	assert.NoError(t, err)
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
	require.NoError(t, err)
	builder := NewStoreBuilder(db.(Database))
	_, err = builder.CreateStore("1_abc")
	require.EqualError(t, err, "invalid table name: 1_abc")
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
