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

package redis

import (
	"github.com/alicebob/miniredis/v2"
	"github.com/lf-edge/ekuiper/internal/pkg/db/redis"
	"github.com/lf-edge/ekuiper/internal/pkg/ts/test/common"
	ts2 "github.com/lf-edge/ekuiper/pkg/kv"
	"strconv"
	"testing"
)

func TestRedisTsSet(t *testing.T) {
	ks, db, minRedis := setupRedisKv()
	defer cleanRedisKv(db, minRedis)

	common.TestTsSet(ks, t)
}

func TestRedisTsLast(t *testing.T) {
	ks, db, minRedis := setupRedisKv()
	defer cleanRedisKv(db, minRedis)

	common.TestTsLast(ks, t)
}

func TestRedisTsGet(t *testing.T) {
	ks, db, minRedis := setupRedisKv()
	defer cleanRedisKv(db, minRedis)

	common.TestTsGet(ks, t)
}

func TestRedisTsDelete(t *testing.T) {
	ks, db, minRedis := setupRedisKv()
	defer cleanRedisKv(db, minRedis)

	common.TestTsDelete(ks, t)
}

func TestRedisTsDeleteBefore(t *testing.T) {
	ks, db, minRedis := setupRedisKv()
	defer cleanRedisKv(db, minRedis)

	common.TestTsDeleteBefore(ks, t)
}

func setupRedisKv() (ts2.Tskv, *redis.Instance, *miniredis.Miniredis) {
	minRedis, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	redisDB := redis.NewRedis("localhost", stringToInt(minRedis.Port()))
	err = redisDB.Connect()
	if err != nil {
		panic(err)
	}

	builder := NewTsBuilder(redisDB)
	var ks ts2.Tskv
	err, ks = builder.CreateTs("test")
	if err != nil {
		panic(err)
	}
	return ks, &redisDB, minRedis
}

func cleanRedisKv(instance *redis.Instance, minRedis *miniredis.Miniredis) {
	instance.Disconnect()
	minRedis.Close()
}

func stringToInt(svalue string) int {
	ivalue, err := strconv.Atoi(svalue)
	if err != nil {
		panic(err)
	}
	return ivalue
}
