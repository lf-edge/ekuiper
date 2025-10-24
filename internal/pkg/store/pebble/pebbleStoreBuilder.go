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

import "github.com/lf-edge/ekuiper/v2/pkg/kv"

type StoreBuilder struct {
	database KVDatabase
}

func NewStoreBuilder(d KVDatabase) *StoreBuilder {
	return &StoreBuilder{
		database: d,
	}
}

func (b StoreBuilder) CreateStore(name string) (kv.KeyValue, error) {
	return createPebbleKvStore(b.database, name)
}
