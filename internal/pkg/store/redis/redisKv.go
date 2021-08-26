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
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/gomodule/redigo/redis"
	dbRedis "github.com/lf-edge/ekuiper/internal/pkg/db/redis"
	kvEncoding "github.com/lf-edge/ekuiper/internal/pkg/store/encoding"
	"strings"
)

const KvPrefix = "KV:STORE"

type redisKvStore struct {
	database  dbRedis.Instance
	table     string
	keyPrefix string
}

func CreateRedisKvStore(redis dbRedis.Instance, table string) (*redisKvStore, error) {
	store := &redisKvStore{
		database:  redis,
		table:     table,
		keyPrefix: fmt.Sprintf("%s:%s", KvPrefix, table),
	}
	return store, nil
}

func (kv redisKvStore) Setnx(key string, value interface{}) error {
	return kv.database.Apply(func(conn redis.Conn) error {
		err, b := kvEncoding.Encode(value)
		if nil != err {
			return err
		}
		tKey := kv.tableKey(key)
		reply, err := conn.Do("SETNX", tKey, b)
		if err != nil {
			return err
		}
		code, err := redis.Int(reply, err)
		if code == 0 {
			return fmt.Errorf("item %s already exists under %s key because of %s", key, tKey, err)
		}
		return nil
	})
}

func (kv redisKvStore) Set(key string, value interface{}) error {
	err, b := kvEncoding.Encode(value)
	if nil != err {
		return err
	}
	err = kv.database.Apply(func(conn redis.Conn) error {
		tKey := kv.tableKey(key)
		reply, err := conn.Do("SET", tKey, b)
		code, err := redis.String(reply, err)
		if err != nil {
			return err
		}
		if code != "OK" {
			return fmt.Errorf("item %s (under key %s) not set because of %s", key, tKey, err)
		}
		return nil
	})
	return err
}

func (kv redisKvStore) Get(key string, value interface{}) (bool, error) {
	result := false
	err := kv.database.Apply(func(conn redis.Conn) error {
		tKey := kv.tableKey(key)
		reply, err := conn.Do("GET", tKey)
		if err != nil {
			return err
		}
		buff, err := redis.Bytes(reply, err)
		if err != nil {
			result = false
			return nil
		}
		dec := gob.NewDecoder(bytes.NewBuffer(buff))
		if err := dec.Decode(value); err != nil {
			return err
		}
		result = true
		return nil
	})
	return result, err
}

func (kv redisKvStore) Delete(key string) error {
	return kv.database.Apply(func(conn redis.Conn) error {
		tKey := kv.tableKey(key)
		_, err := conn.Do("DEL", tKey)
		return err
	})
}

func (kv redisKvStore) Keys() ([]string, error) {
	keys := make([]string, 0)
	err := kv.database.Apply(func(conn redis.Conn) error {
		pattern := fmt.Sprintf("%s:*", kv.keyPrefix)
		reply, err := conn.Do("KEYS", pattern)
		keys, err = redis.Strings(reply, err)
		return err
	})
	result := make([]string, 0)
	for _, k := range keys {
		result = append(result, kv.trimPrefix(k))
	}
	return result, err
}

func (kv redisKvStore) Clean() error {
	keys, err := kv.Keys()
	if err != nil {
		return err
	}
	keysToRemove := make([]interface{}, len(keys))
	for i, v := range keysToRemove {
		keysToRemove[i] = v
	}
	err = kv.database.Apply(func(conn redis.Conn) error {
		_, err := conn.Do("DEL", keysToRemove...)
		return err
	})
	return err
}

func (kv redisKvStore) tableKey(key string) string {
	return fmt.Sprintf("%s:%s:%s", KvPrefix, kv.table, key)
}

func (kv redisKvStore) trimPrefix(fullKey string) string {
	prefixToTrim := fmt.Sprintf("%s:%s:", KvPrefix, kv.table)
	return strings.TrimPrefix(fullKey, prefixToTrim)
}
