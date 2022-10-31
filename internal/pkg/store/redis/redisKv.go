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
	"fmt"
	"github.com/go-redis/redis/v7"
	"github.com/lf-edge/ekuiper/internal/conf"
	kvEncoding "github.com/lf-edge/ekuiper/internal/pkg/store/encoding"
	"strings"
)

const KvPrefix = "KV:STORE"

type redisKvStore struct {
	database  *redis.Client
	table     string
	keyPrefix string
}

func createRedisKvStore(redis *redis.Client, table string) (*redisKvStore, error) {
	store := &redisKvStore{
		database:  redis,
		table:     table,
		keyPrefix: fmt.Sprintf("%s:%s", KvPrefix, table),
	}
	return store, nil
}

func (kv redisKvStore) Setnx(key string, value interface{}) error {
	err, b := kvEncoding.Encode(value)
	if nil != err {
		return err
	}
	done, err := kv.database.SetNX(kv.tableKey(key), b, 0).Result()
	if err != nil {
		return err
	}
	if !done {
		return fmt.Errorf("key %s already exists", key)
	}
	return nil
}

func (kv redisKvStore) Set(key string, value interface{}) error {
	err, b := kvEncoding.Encode(value)
	if nil != err {
		return err
	}
	return kv.database.SetNX(kv.tableKey(key), b, 0).Err()
}

func (kv redisKvStore) Get(key string, value interface{}) (bool, error) {
	val, err := kv.database.Get(kv.tableKey(key)).Result()
	if err != nil {
		return false, err
	}
	dec := gob.NewDecoder(bytes.NewBuffer([]byte(val)))
	if err := dec.Decode(value); err != nil {
		return false, err
	}
	return true, nil
}

func (kv redisKvStore) Delete(key string) error {
	return kv.database.Del(kv.tableKey(key)).Err()
}

func (kv redisKvStore) Keys() ([]string, error) {
	keys, err := kv.metaKeys()
	if err != nil {
		return nil, err
	}
	result := make([]string, 0)
	for _, k := range keys {
		result = append(result, kv.trimPrefix(k))
	}
	return result, nil
}

func (kv redisKvStore) All() (map[string]string, error) {
	keys, err := kv.metaKeys()
	if err != nil {
		return nil, err
	}
	var (
		value  string
		result = make(map[string]string)
	)
	for _, k := range keys {
		key := kv.trimPrefix(k)
		ok, err := kv.Get(key, &value)
		if err != nil {
			conf.Log.Errorf("get %s fail during get all in redi: %v", key, err)
		}
		if ok {
			result[key] = value
		}
	}
	return result, nil
}

func (kv redisKvStore) metaKeys() ([]string, error) {
	return kv.database.Keys(fmt.Sprintf("%s:*", kv.keyPrefix)).Result()
}

func (kv redisKvStore) Clean() error {
	keys, err := kv.metaKeys()
	if err != nil {
		return err
	}
	keysToRemove := make([]string, len(keys))
	for i, v := range keys {
		keysToRemove[i] = v
	}
	return kv.database.Del(keysToRemove...).Err()
}

func (kv redisKvStore) Drop() error {
	return kv.Clean()
}

func (kv redisKvStore) tableKey(key string) string {
	return fmt.Sprintf("%s:%s:%s", KvPrefix, kv.table, key)
}

func (kv redisKvStore) trimPrefix(fullKey string) string {
	prefixToTrim := fmt.Sprintf("%s:%s:", KvPrefix, kv.table)
	return strings.TrimPrefix(fullKey, prefixToTrim)
}
