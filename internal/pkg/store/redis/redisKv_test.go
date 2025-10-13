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

package redis

import (
	"bytes"
	"encoding/gob"
	"strconv"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/store/test/common"
	"github.com/lf-edge/ekuiper/v2/pkg/kv"
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

func TestRedisKvSetGet(t *testing.T) {
	ks, db, minRedis := setupRedisKv()
	defer cleanRedisKv(db, minRedis)
	common.TestKvSetGet(ks, t)
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

func TestRedisKvGetKeyedState(t *testing.T) {
	ks, db, minRedis := setupRedisKv()
	defer cleanRedisKv(db, minRedis)

	common.TestKvGetKeyedState(ks, t)
}

func TestRedisGetByPrefix(t *testing.T) {
	ks, db, minRedis := setupRedisKv()
	defer cleanRedisKv(db, minRedis)
	require.NoError(t, ks.Set("prefix1", int64(1)))
	require.NoError(t, ks.Set("prefix2", int64(1)))
	m, err := ks.GetByPrefix("prefix")
	require.NoError(t, err)
	k1, ok := m["prefix1"]
	require.True(t, ok)
	dec := gob.NewDecoder(bytes.NewBuffer(k1))
	var v1 int64
	require.NoError(t, dec.Decode(&v1))
	require.Equal(t, int64(1), v1)
	k1, ok = m["prefix2"]
	require.True(t, ok)
	dec = gob.NewDecoder(bytes.NewBuffer(k1))
	require.NoError(t, dec.Decode(&v1))
	require.Equal(t, int64(1), v1)
}

func setupRedisKv() (kv.KeyValue, *redis.Client, *miniredis.Miniredis) {
	minRedis, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	redisDB := redis.NewClient(&redis.Options{
		Addr: minRedis.Addr(),
	})
	builder := NewStoreBuilder(redisDB)
	var ks kv.KeyValue
	ks, err = builder.CreateStore("test")
	if err != nil {
		panic(err)
	}
	return ks, redisDB, minRedis
}

func cleanRedisKv(instance *redis.Client, minRedis *miniredis.Miniredis) {
	instance.Close()
	minRedis.Close()
}

func stringToInt(svalue string) int {
	ivalue, err := strconv.Atoi(svalue)
	if err != nil {
		panic(err)
	}
	return ivalue
}
