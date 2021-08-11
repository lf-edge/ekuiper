// Copyright 2021 INTECH Process Automation Ltd.
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

package kv

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/pkg/db"
	"github.com/lf-edge/ekuiper/internal/pkg/store"
	"github.com/lf-edge/ekuiper/internal/pkg/ts"
	st "github.com/lf-edge/ekuiper/pkg/kv/stores"
	"sync"
)

type stores struct {
	kv        map[string]st.KeyValue
	ts        map[string]st.Tskv
	mu        sync.Mutex
	kvBuilder store.Builder
	tsBuilder ts.Builder
}

func newStores(db db.Database) (error, *stores) {
	var err error
	var kvBuilder store.Builder
	var tsBuilder ts.Builder
	err, kvBuilder = store.CreateStoreBuilder(db)
	if err != nil {
		return err, nil
	}
	err, tsBuilder = ts.CreateTsBuilder(db)
	if err != nil {
		return err, nil
	}
	return nil, &stores{
		kv:        make(map[string]st.KeyValue),
		ts:        make(map[string]st.Tskv),
		mu:        sync.Mutex{},
		kvBuilder: kvBuilder,
		tsBuilder: tsBuilder,
	}
}

func (s *stores) GetKV(table string) (error, st.KeyValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ks, contains := s.kv[table]; contains {
		return nil, ks
	}
	err, ks := s.kvBuilder.CreateStore(table)
	if err != nil {
		return err, nil
	}
	s.kv[table] = ks
	return nil, ks
}

func (s *stores) GetTS(table string) (error, st.Tskv) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if tts, contains := s.ts[table]; contains {
		return nil, tts
	}
	err, tts := s.tsBuilder.CreateTs(table)
	if err != nil {
		return err, nil
	}
	s.ts[table] = tts
	return nil, tts
}

var globalStores *stores = nil

func InitGlobalStores(db db.Database) error {
	var err error
	err, globalStores = newStores(db)
	return err
}

func GetKV(table string) (error, st.KeyValue) {
	if globalStores == nil {
		return fmt.Errorf("global stores are not initialized"), nil
	}
	return globalStores.GetKV(table)
}

func GetTS(table string) (error, st.Tskv) {
	if globalStores == nil {
		return fmt.Errorf("global stores are not initialized"), nil
	}
	return globalStores.GetTS(table)
}
