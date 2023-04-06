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
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	kvEncoding "github.com/lf-edge/ekuiper/internal/pkg/store/encoding"
	"github.com/redis/go-redis/v9"
	"strconv"
)

const (
	TsPrefix             = "KV:TS"
	AddToSortedSet       = "ZADD"
	ReversedRangeByScore = "ZREVRANGEBYSCORE"
	RemoveRangeByScore   = "ZREMRANGEBYSCORE"
	Delete               = "DEL"
	ReversedRange        = "ZREVRANGE"
)

type ts struct {
	db    *redis.Client
	table string
	last  int64
	key   string
}

func init() {
	gob.Register(make(map[string]interface{}))
}

func createRedisTs(redis *redis.Client, table string) (*ts, error) {
	key := fmt.Sprintf("%s:%s", TsPrefix, table)
	lastTs, err := getLast(redis, key, nil)
	if err != nil {
		return nil, err
	}
	s := &ts{
		db:    redis,
		table: table,
		last:  lastTs,
		key:   key,
	}
	return s, nil
}

func (t *ts) Set(key int64, value interface{}) (bool, error) {
	if key <= t.last {
		return false, nil
	}
	b, err := kvEncoding.Encode(value)
	if err != nil {
		return false, err
	}
	length, err := t.db.ZAdd(context.Background(), t.key, redis.Z{Score: float64(key), Member: b}).Result()
	if err != nil {
		return false, err
	}
	if length == 0 {
		return false, fmt.Errorf("list at %s key should be non empty", t.key)
	}
	t.last = key
	return true, nil
}

func (t *ts) Get(key int64, value interface{}) (bool, error) {
	reply, err := t.db.ZRevRangeByScore(context.Background(), t.key, &redis.ZRangeBy{Min: strconv.FormatInt(key, 10), Max: strconv.FormatInt(key, 10)}).Result()
	if len(reply) == 0 {
		return false, fmt.Errorf("record under %s key and %d score not found", t.key, key)
	}
	dec := gob.NewDecoder(bytes.NewBuffer([]byte(reply[0])))
	err = dec.Decode(value)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (t *ts) Last(value interface{}) (int64, error) {
	return getLast(t.db, t.key, value)
}

func (t *ts) Delete(key int64) error {
	return t.db.ZRemRangeByScore(context.Background(), t.key, strconv.FormatInt(key, 10), strconv.FormatInt(key, 10)).Err()
}

func (t *ts) DeleteBefore(key int64) error {
	return t.db.ZRemRangeByScore(context.Background(), t.key, "-inf", strconv.FormatInt(key, 10)).Err()
}

func (t *ts) Close() error {
	return nil
}

func (t *ts) Drop() error {
	return t.db.Del(context.Background(), t.key).Err()
}

func getLast(db *redis.Client, key string, value interface{}) (int64, error) {
	var last int64 = 0
	reply, err := db.ZRevRangeWithScores(context.Background(), key, 0, 0).Result()
	if len(reply) > 0 {
		if value != nil {
			v := reply[0].Member.(string)
			dec := gob.NewDecoder(bytes.NewBuffer([]byte(v)))
			if err = dec.Decode(value); err != nil {
				return 0, err
			}
		}
		last = int64(reply[0].Score)
	}
	return last, nil
}
