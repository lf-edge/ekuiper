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

package sqlkv

import (
	"github.com/lf-edge/ekuiper/pkg/kv"
	"sync"
)

type kvstores struct {
	stores map[string]kv.KeyValue
	mu     sync.Mutex
}

var stores = kvstores{
	stores: make(map[string]kv.KeyValue),
	mu:     sync.Mutex{},
}

var database Database

func Setup(dataDir string) error {
	err, db := newSqliteDatabase(dataDir)
	if err != nil {
		return err
	}
	err = db.Connect()
	if err != nil {
		return err
	}
	database = db
	return nil
}

func Close() {
	if database != nil {
		database.Disconnect()
		database = nil
	}
}

func (s *kvstores) get(table string) (kv.KeyValue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if store, contains := s.stores[table]; contains {
		return store, nil
	}
	err, store := CreateSqlKvStore(database, table)
	if err != nil {
		return nil, err
	}
	s.stores[table] = store
	return store, nil
}

func GetKVStore(table string) (kv.KeyValue, error) {
	return stores.get(table)
}
