// Copyright 2025 EMQ Technologies Co., Ltd.
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

package pebble

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	pebbledb "github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/store/definition"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store/pebble/pebble_kv"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store/test/common"
	"github.com/lf-edge/ekuiper/v2/pkg/kv"
)

const (
	PebbleKvDBPath = "test_pebble"
	PebbleKvTable  = "test"
)

func TestPebbleGetSetDelete(t *testing.T) {
	ks, db, abs := setupPebbleKv()
	defer cleanPebbleKv(db, abs)

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

func TestPebbleKvSetnx(t *testing.T) {
	ks, db, abs := setupPebbleKv()
	defer cleanPebbleKv(db, abs)

	common.TestKvSetnx(ks, t)
}

func TestPebbleKvSet(t *testing.T) {
	ks, db, abs := setupPebbleKv()
	defer cleanPebbleKv(db, abs)

	common.TestKvSet(ks, t)
}

func TestPebbleKvGet(t *testing.T) {
	ks, db, abs := setupPebbleKv()
	defer cleanPebbleKv(db, abs)

	common.TestKvGet(ks, t)
}

func TestPebbleKvSetGet(t *testing.T) {
	ks, db, abs := setupPebbleKv()
	defer cleanPebbleKv(db, abs)

	common.TestKvSetGet(ks, t)
}

func TestPebbleKvKeys(t *testing.T) {
	ks, db, abs := setupPebbleKv()
	defer cleanPebbleKv(db, abs)

	common.TestKvKeys(10, ks, t)
}

func TestPebbleKvAll(t *testing.T) {
	ks, db, abs := setupPebbleKv()
	defer cleanPebbleKv(db, abs)

	common.TestKvAll(10, ks, t)
}

func TestPebbleKvGetKeyedState(t *testing.T) {
	ks, db, abs := setupPebbleKv()
	defer cleanPebbleKv(db, abs)

	common.TestKvGetKeyedState(ks, t)
}

func TestPebbleKv_SetEncodeError_And_SetnxEncodeError(t *testing.T) {
	ks, db, abs := setupPebbleKv()
	defer cleanPebbleKv(db, abs)

	require.Error(t, ks.Set("bad", make(chan int)))
	require.Error(t, ks.Setnx("badnx", make(chan int)))
}

func TestPebbleKv_DecodeError_Clean_Drop_DeleteNotFound(t *testing.T) {
	ks, db, abs := setupPebbleKv()
	defer cleanPebbleKv(db, abs)

	d := db.(KVDatabase)
	corruptKey := []byte(fmt.Sprintf("%s:%s", PebbleKvTable, "corrupt"))
	require.NoError(t, d.Apply(func(pdb *pebbledb.DB) error {
		return pdb.Set(corruptKey, []byte("not_gob"), nil)
	}))

	var v string
	found, err := ks.Get("corrupt", &v)
	require.False(t, found)
	require.Error(t, err)

	require.NoError(t, ks.Set("k1", "v1"))
	require.NoError(t, ks.Set("k2", "v2"))
	require.NoError(t, ks.Clean())
	keys, err := ks.Keys()
	require.NoError(t, err)
	require.Len(t, keys, 0)

	require.NoError(t, ks.Set("k3", "v3"))
	require.NoError(t, ks.Drop())
	keys, err = ks.Keys()
	require.NoError(t, err)
	require.Len(t, keys, 0)

	require.Error(t, ks.Delete("not_exists"))
}

func TestPebbleGetByPrefix(t *testing.T) {
	ks, db, abs := setupPebbleKv()
	defer cleanPebbleKv(db, abs)

	require.NoError(t, ks.Set("prefix1", int64(1)))
	require.NoError(t, ks.Set("prefix2", int64(1)))
	require.NoError(t, ks.Set("other", int64(999)))

	m, err := ks.GetByPrefix("prefix")
	require.NoError(t, err)

	k1, ok := m["prefix1"]
	require.True(t, ok)

	dec := gob.NewDecoder(bytes.NewBuffer(k1))
	var v1 int64
	require.NoError(t, dec.Decode(&v1))
	require.Equal(t, int64(1), v1)

	k2, ok := m["prefix2"]
	require.True(t, ok)

	dec = gob.NewDecoder(bytes.NewBuffer(k2))
	require.NoError(t, dec.Decode(&v1))
	require.Equal(t, int64(1), v1)

	_, ok = m["other"]
	require.False(t, ok)
}

func setupPebbleKv() (kv.KeyValue, definition.Database, string) {
	absPath, err := filepath.Abs("test_pebble")
	if err != nil {
		panic(err)
	}

	err = deletePebbleIfExists(absPath)
	if err != nil {
		panic(err)
	}

	config := definition.Config{
		Type: "pebble",
		Pebble: definition.PebbleConfig{
			Path: absPath,
			Name: PebbleKvDBPath,
		},
	}

	db, err := pebble.NewPebbleDatabase(config, PebbleKvDBPath)
	if err != nil {
		panic(err)
	}

	err = db.Connect()
	if err != nil {
		panic(err)
	}

	builder := NewStoreBuilder(db.(KVDatabase))
	store, err := builder.CreateStore(PebbleKvTable)
	if err != nil {
		panic(err)
	}

	return store, db, absPath
}

func deletePebbleIfExists(path string) error {
	return os.RemoveAll(path)
}

func cleanPebbleKv(db definition.Database, abs string) {
	_ = db.Disconnect()
	_ = deletePebbleIfExists(abs)
}
