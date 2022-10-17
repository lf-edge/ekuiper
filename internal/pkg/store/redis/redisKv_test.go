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
	"github.com/alicebob/miniredis/v2"
	"github.com/lf-edge/ekuiper/internal/pkg/store/test/common"
	"github.com/lf-edge/ekuiper/pkg/kv"
	"strconv"
	"testing"
)

func TestRedisKvSetnx(t *testing.T) {
	ks, db, minRedis := setupRedisKv()
	defer cleanRedisKv(db, minRedis)
	common.TestKvSetnx(ks, t)
}

func TestRedisKvSet(t *testing.T) {
	ks, db, minRedis := setupRedisKv()
	defer cleanRedisKv(db, minRedis)
	common.TestKvSet(ks, t)
}

func TestRedisKvGet(t *testing.T) {
	ks, db, minRedis := setupRedisKv()
	defer cleanRedisKv(db, minRedis)
	common.TestKvGet(ks, t)
}

func TestRedisKvKeys(t *testing.T) {
	ks, db, minRedis := setupRedisKv()
	defer cleanRedisKv(db, minRedis)

	length := 10
	common.TestKvKeys(length, ks, t)
}

func TestRedisKvAll(t *testing.T) {
	ks, db, minRedis := setupRedisKv()
	defer cleanRedisKv(db, minRedis)

	length := 10
	common.TestKvAll(length, ks, t)
}

func setupRedisKv() (kv.KeyValue, *Instance, *miniredis.Miniredis) {
	minRedis, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	redisDB := NewRedis("localhost", stringToInt(minRedis.Port()))
	err = redisDB.Connect()
	if err != nil {
		panic(err)
	}
	builder := NewStoreBuilder(redisDB)
	var ks kv.KeyValue
	ks, err = builder.CreateStore("test")
	if err != nil {
		panic(err)
	}
	return ks, redisDB, minRedis
}

func cleanRedisKv(instance *Instance, minRedis *miniredis.Miniredis) {
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
