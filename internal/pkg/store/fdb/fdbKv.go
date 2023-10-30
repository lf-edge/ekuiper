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
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"

	kvEncoding "github.com/lf-edge/ekuiper/internal/pkg/store/encoding"
)

type fdbKvStore struct {
	database *fdb.Database
	subspace directory.DirectorySubspace
}

func createFdbKvStore(fdb *fdb.Database, db string, table string) (*fdbKvStore, error) {
	dir, err := directory.CreateOrOpen(fdb, []string{db, table}, nil)
	if err != nil {
		return nil, err
	}
	store := &fdbKvStore{
		database: fdb,
		subspace: dir,
	}
	return store, nil
}

func (kv fdbKvStore) Setnx(key string, value interface{}) error {
	b, err := kvEncoding.Encode(value)
	if nil != err {
		return err
	}
	_, err = kv.database.Transact(func(tr fdb.Transaction) (val interface{}, e error) {
		ret, err := tr.Get(kv.subspace.Pack(tuple.Tuple{key})).Get()
		if err != nil {
			return nil, err
		}
		if ret != nil {
			return nil, fmt.Errorf(`Item %s already exists`, key)
		}
		tr.Set(kv.subspace.Pack(tuple.Tuple{key}), b)
		return
	})

	return err
}

func (kv fdbKvStore) Set(key string, value interface{}) error {
	b, err := kvEncoding.Encode(value)
	if nil != err {
		return err
	}
	_, err = kv.database.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.Set(kv.subspace.Pack(tuple.Tuple{key}), b)
		return
	})

	return err
}

func (kv fdbKvStore) Get(key string, value interface{}) (bool, error) {
	val, err := kv.database.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		ret, e = tr.Get(kv.subspace.Pack(tuple.Tuple{key})).Get()
		return
	})
	if err != nil {
		return false, err
	}
	dec := gob.NewDecoder(strings.NewReader(string(val.([]byte))))
	if err := dec.Decode(value); err != nil {
		return false, err
	}
	return true, nil
}

func (kv fdbKvStore) GetKeyedState(key string) (interface{}, error) {
	val, err := kv.database.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		ret, e = tr.Get(kv.subspace.Pack(tuple.Tuple{key})).Get()
		return
	})
	if err != nil {
		return nil, err
	}
	var value interface{}
	if err := json.Unmarshal(val.([]byte), &value); err != nil {
		return nil, err
	}
	return value, nil
}

func (kv fdbKvStore) SetKeyedState(key string, value interface{}) error {
	b, err := json.Marshal(value)
	if nil != err {
		return err
	}
	_, err = kv.database.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.Set(kv.subspace.Pack(tuple.Tuple{key}), b)
		return
	})

	return err
}

func (kv fdbKvStore) Delete(key string) error {
	_, err := kv.database.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.Clear(kv.subspace.Pack(tuple.Tuple{key}))
		return
	})
	return err
}

func (kv fdbKvStore) Keys() ([]string, error) {
	keys, err := kv.database.Transact(func(tr fdb.Transaction) (interface{}, error) {
		var keys []string
		it := tr.GetRange(kv.subspace, fdb.RangeOptions{}).Iterator()
		for it.Advance() {
			keyVal, err := it.Get()
			if err != nil {
				return nil, err
			}
			ks, err := kv.subspace.Unpack(keyVal.Key)
			if err != nil {
				return nil, err
			}
			keys = append(keys, ks[0].(string))
		}
		return keys, nil
	})
	if err != nil {
		return nil, err
	}
	return keys.([]string), nil
}

func (kv fdbKvStore) All() (map[string]string, error) {
	alls, err := kv.database.Transact(func(tr fdb.Transaction) (interface{}, error) {
		alls := make(map[string]string)
		it := tr.GetRange(kv.subspace, fdb.RangeOptions{}).Iterator()
		for it.Advance() {
			keyVal, err := it.Get()
			if err != nil {
				return nil, err
			}
			ks, err := kv.subspace.Unpack(keyVal.Key)
			if err != nil {
				return nil, err
			}
			var value string
			dec := gob.NewDecoder(bytes.NewBuffer(keyVal.Value))
			if err := dec.Decode(&value); err != nil {
				return nil, err
			}
			alls[ks[0].(string)] = value
		}
		return alls, nil
	})
	if err != nil {
		return nil, err
	}
	return alls.(map[string]string), nil
}

func (kv fdbKvStore) Clean() error {
	_, err := kv.database.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.ClearRange(kv.subspace)
		return
	})
	if err != nil {
		return err
	}
	return nil
}

func (kv fdbKvStore) Drop() error {
	return kv.Clean()
}
