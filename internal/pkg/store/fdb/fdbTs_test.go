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
	ts2 "github.com/lf-edge/ekuiper/pkg/kv"
)

const (
	TSTable = "test"
)

func TestFdbTsSet(t *testing.T) {
	ks, db, subspace := setupTFdbKv()
	defer cleanTFdbKv(db, subspace)

	common.TestTsSet(ks, t)
}

func TestFdbTsLast(t *testing.T) {
	ks, db, subspace := setupTFdbKv()
	defer cleanTFdbKv(db, subspace)

	common.TestTsLast(ks, t)
}

func TestFdbTsGet(t *testing.T) {
	ks, db, subspace := setupTFdbKv()
	defer cleanTFdbKv(db, subspace)

	common.TestTsGet(ks, t)
}

func TestFdbTsDelete(t *testing.T) {
	ks, db, subspace := setupTFdbKv()
	defer cleanTFdbKv(db, subspace)

	common.TestTsDelete(ks, t)
}

func TestFdbTsDeleteBefore(t *testing.T) {
	ks, db, subspace := setupTFdbKv()
	defer cleanTFdbKv(db, subspace)

	common.TestTsDeleteBefore(ks, t)
}

func cleanTS(client fdb.Database, subspace directory.DirectorySubspace) error {
	_, err := client.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.ClearRange(subspace)
		return
	})
	return err
}

func setupTFdbKv() (ts2.Tskv, fdb.Database, directory.DirectorySubspace) {
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
	builder := NewTsBuilder(&db)
	var store ts2.Tskv
	store, err = builder.CreateTs(TSTable)
	if err != nil {
		panic(err)
	}
	dir, err := directory.CreateOrOpen(db, []string{TSNamespace, TSTable}, nil)
	if err != nil {
		panic(err)
	}
	err = cleanTS(db, dir)
	if err != nil {
		panic(err)
	}
	return store, db, dir
}

func cleanTFdbKv(client fdb.Database, subspace directory.DirectorySubspace) {
	err := cleanTS(client, subspace)
	if err != nil {
		panic(err)
	}
	client.Close()
}
