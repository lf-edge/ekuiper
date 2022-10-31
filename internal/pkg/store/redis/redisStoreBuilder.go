// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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

//go:build redisdb || !core
// +build redisdb !core

package redis

import (
	"github.com/go-redis/redis/v7"
	"github.com/lf-edge/ekuiper/pkg/kv"
)

type StoreBuilder struct {
	database *redis.Client
}

func NewStoreBuilder(redis *redis.Client) StoreBuilder {
	return StoreBuilder{
		database: redis,
	}
}

func (b StoreBuilder) CreateStore(table string) (kv.KeyValue, error) {
	return createRedisKvStore(b.database, table)
}
