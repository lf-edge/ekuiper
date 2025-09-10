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
	"os"
	"path/filepath"
	"testing"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/store/definition"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store/pebble/pebble_kv"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store/test/common"
	"github.com/lf-edge/ekuiper/v2/pkg/kv"
)

const (
	PebbleTsDBPath = "test_pebble_ts"
	PebbleTsTable  = "ts_test"
)

func TestPebbleTsSet(t *testing.T) {
	ks, db, abs := setupPebbleTs()
	defer cleanPebbleTs(db, abs)

	common.TestTsSet(ks, t)
}

func TestPebbleTsLast(t *testing.T) {
	ks, db, abs := setupPebbleTs()
	defer cleanPebbleTs(db, abs)

	common.TestTsLast(ks, t)
}

func TestPebbleTsGet(t *testing.T) {
	ks, db, abs := setupPebbleTs()
	defer cleanPebbleTs(db, abs)

	common.TestTsGet(ks, t)
}

func TestPebbleTsDelete(t *testing.T) {
	ks, db, abs := setupPebbleTs()
	defer cleanPebbleTs(db, abs)

	common.TestTsDelete(ks, t)
}

func TestPebbleTsDeleteBefore(t *testing.T) {
	ks, db, abs := setupPebbleTs()
	defer cleanPebbleTs(db, abs)

	common.TestTsDeleteBefore(ks, t)
}

func setupPebbleTs() (kv.Tskv, definition.Database, string) {
	absPath, err := filepath.Abs(PebbleTsDBPath)
	if err != nil {
		panic(err)
	}

	err = deleteTPebbleIfExists(absPath)
	if err != nil {
		panic(err)
	}

	config := definition.Config{
		Type: "pebble",
		Pebble: definition.PebbleConfig{
			Path: absPath,
			Name: PebbleTsDBPath,
		},
	}

	db, err := pebble.NewPebbleDatabase(config, PebbleTsTable)
	if err != nil {
		panic(err)
	}

	err = db.Connect()
	if err != nil {
		panic(err)
	}

	builder := NewTsBuilder(db.(KVDatabase))
	store, err := builder.CreateTs(PebbleTsTable)
	if err != nil {
		panic(err)
	}

	return store, db, absPath
}

func deleteTPebbleIfExists(path string) error {
	return os.RemoveAll(path)
}

func cleanPebbleTs(db definition.Database, abs string) {
	_ = db.Disconnect()
	_ = deleteTPebbleIfExists(abs)
}
