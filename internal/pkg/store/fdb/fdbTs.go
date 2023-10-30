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
	"encoding/gob"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"

	kvEncoding "github.com/lf-edge/ekuiper/internal/pkg/store/encoding"
)

type ts struct {
	database *fdb.Database
	subspace directory.DirectorySubspace
	last     int64
}

func CreateFdbTs(fdb *fdb.Database, db string, table string) (*ts, error) {
	dir, err := directory.CreateOrOpen(fdb, []string{db, table}, nil)
	if err != nil {
		return nil, err
	}
	store := &ts{
		database: fdb,
		subspace: dir,
		last:     getLast(fdb, dir),
	}
	return store, nil
}

func (t *ts) Set(key int64, value interface{}) (bool, error) {
	if key <= t.last {
		return false, nil
	}
	b, err := kvEncoding.Encode(value)
	if nil != err {
		return false, err
	}
	_, err = t.database.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.Set(t.subspace.Pack(tuple.Tuple{key}), b)
		return
	})
	if err != nil {
		return false, err
	}
	t.last = key
	return true, nil
}

func (t *ts) Get(key int64, value interface{}) (bool, error) {
	val, err := t.database.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		ret, e = tr.Get(t.subspace.Pack(tuple.Tuple{key})).Get()
		return
	})
	if err != nil || string(val.([]byte)) == "" {
		return false, err
	}
	dec := gob.NewDecoder(strings.NewReader(string(val.([]byte))))
	if err := dec.Decode(value); err != nil {
		return false, err
	}
	return true, nil
}

func (t *ts) Last(value interface{}) (int64, error) {
	_, err := t.Get(t.last, value)
	if err != nil {
		return 0, err
	}
	return t.last, nil
}

func (t *ts) Delete(key int64) error {
	_, err := t.database.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.Clear(t.subspace.Pack(tuple.Tuple{key}))
		return
	})
	return err
}

func (t *ts) DeleteBefore(key int64) error {
	_, err := t.database.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.ClearRange(fdb.KeyRange{Begin: t.subspace, End: t.subspace.Pack(tuple.Tuple{key})})
		return
	})
	return err
}

func (t *ts) Close() error {
	return nil
}

func (t *ts) Drop() error {
	_, err := t.database.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.ClearRange(t.subspace)
		return
	})
	if err != nil {
		return err
	}
	return nil
}

func getLast(db *fdb.Database, subspace directory.DirectorySubspace) int64 {
	last, _ := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		results, err := tr.GetRange(subspace, fdb.RangeOptions{Limit: 1, Reverse: true}).GetSliceWithError()
		if err != nil || len(results) == 0 {
			return int64(0), err
		}
		key, err := subspace.Unpack(results[0].Key)
		if err != nil {
			return int64(0), err
		}
		return key[0].(int64), nil
	})
	return last.(int64)
}
