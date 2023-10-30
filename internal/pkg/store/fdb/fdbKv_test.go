// Copyright 2023 EMQ Technologies Co., Ltd.
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

//go:build fdb || full

package fdb

import (
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"

	"github.com/lf-edge/ekuiper/internal/pkg/store/test/common"
	"github.com/lf-edge/ekuiper/pkg/kv"
)

const (
	KVTable = "test"
	TimeOut = 30000
)

func TestFdbKvSetnx(t *testing.T) {
	ks, db, subspace := setupFdbKv()
	defer cleanFdbKv(db, subspace)
	common.TestKvSetnx(ks, t)
}

func TestFdbKvSet(t *testing.T) {
	ks, db, subspace := setupFdbKv()
	defer cleanFdbKv(db, subspace)
	common.TestKvSet(ks, t)
}

func TestFdbSetGet(t *testing.T) {
	ks, db, subspace := setupFdbKv()
	defer cleanFdbKv(db, subspace)
	common.TestKvSetGet(ks, t)
}

func TestFdbKvKeys(t *testing.T) {
	ks, db, subspace := setupFdbKv()
	defer cleanFdbKv(db, subspace)

	length := 10
	common.TestKvKeys(length, ks, t)
}

func TestFdbKvAll(t *testing.T) {
	ks, db, subspace := setupFdbKv()
	defer cleanFdbKv(db, subspace)

	length := 10
	common.TestKvAll(length, ks, t)
}

func TestFdbKvGetKeyedState(t *testing.T) {
	ks, db, subspace := setupFdbKv()
	defer cleanFdbKv(db, subspace)

	common.TestKvGetKeyedState(ks, t)
}

func cleanKV(client fdb.Database, subspace directory.DirectorySubspace) error {
	_, err := client.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.ClearRange(subspace)
		return
	})
	return err
}

func setupFdbKv() (kv.KeyValue, fdb.Database, directory.DirectorySubspace) {
	err := fdb.APIVersion(defaultAPIVersion)
	if err != nil {
		panic(err)
	}
	db, err := fdb.OpenDefault()
	if err != nil {
		panic(err)
	}
	err = db.Options().SetTransactionTimeout(TimeOut)
	if err != nil {
		panic(err)
	}
	builder := NewStoreBuilder(&db)
	var store kv.KeyValue
	store, err = builder.CreateStore(KVTable)
	if err != nil {
		panic(err)
	}
	dir, err := directory.CreateOrOpen(db, []string{KVNamespace, KVTable}, nil)
	if err != nil {
		panic(err)
	}
	err = cleanKV(db, dir)
	if err != nil {
		panic(err)
	}
	return store, db, dir
}

func cleanFdbKv(client fdb.Database, subspace directory.DirectorySubspace) {
	err := cleanKV(client, subspace)
	if err != nil {
		panic(err)
	}
	client.Close()
}
