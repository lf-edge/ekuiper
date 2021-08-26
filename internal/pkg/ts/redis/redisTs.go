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
	redis dbRedis.Instance
	table string
	last  int64
	key   string
}

func init() {
	gob.Register(make(map[string]interface{}))
}

func createRedisTs(redis dbRedis.Instance, table string) (error, *ts) {
	key := fmt.Sprintf("%s:%s", TsPrefix, table)
	err, lastTs := getLast(redis, key)
	if err != nil {
		return err, nil
	}
	s := &ts{
		redis: redis,
		table: table,
		last:  lastTs,
		key:   key,
	}
	return nil, s
}

func (t *ts) Set(key int64, value interface{}) (bool, error) {
	if key <= t.last {
		return false, nil
	}
	err, b := kvEncoding.Encode(value)
	if err != nil {
		return false, err
	}
	err = t.redis.Apply(func(conn redis.Conn) error {
		reply, err := conn.Do(AddToSortedSet, t.key, key, b)
		if err != nil {
			return err
		}
		length, err := redis.Int(reply, err)
		if err != nil {
			return err
		}
		if length == 0 {
			return fmt.Errorf("list at %s key should be non empty", t.key)
		}
		t.last = key
		return nil
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

func (t ts) Get(key int64, value interface{}) (bool, error) {
	err := t.redis.Apply(func(conn redis.Conn) error {
		reply, err := conn.Do(ReversedRangeByScore, t.key, key, key)
		if err != nil {
			return err
		}
		var tmp [][]byte
		tmp, err = redis.ByteSlices(reply, err)
		if err != nil {
			return err
		}
		if len(tmp) == 0 {
			return fmt.Errorf("record under %s key and %d score not found", t.key, key)
		}
		dec := gob.NewDecoder(bytes.NewBuffer(tmp[0]))
		err = dec.Decode(value)
		return err
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

func (t ts) Last(value interface{}) (int64, error) {
	var last int64 = 0
	err := t.redis.Apply(func(conn redis.Conn) error {
		reply, err := conn.Do(ReversedRange, t.key, 0, 0, "WITHSCORES")
		if err != nil {
			return err
		}
		var tmp [][]byte
		tmp, err = redis.ByteSlices(reply, err)
		if err != nil {
			return err
		}
		if len(tmp) > 0 {
			dec := gob.NewDecoder(bytes.NewBuffer(tmp[0]))
			if err = dec.Decode(value); err != nil {
				return err
			}
			last, err = strconv.ParseInt(string(tmp[1]), 10, 64)
		}
		return err
	})
	if err != nil {
		return 0, err
	}
	return last, nil
}

func (t ts) Delete(key int64) error {
	return t.redis.Apply(func(conn redis.Conn) error {
		_, err := conn.Do(RemoveRangeByScore, t.key, key, key)
		return err
	})
}

func (t ts) DeleteBefore(key int64) error {
	return t.redis.Apply(func(conn redis.Conn) error {
		bound := fmt.Sprintf("(%d", key)
		_, err := conn.Do(RemoveRangeByScore, t.key, "-INF", bound)
		return err
	})
}

func (t ts) Close() error {
	return nil
}

func (t ts) Drop() error {
	return t.redis.Apply(func(conn redis.Conn) error {
		_, err := conn.Do(Delete, t.key)
		return err
	})
}

func getLast(db dbRedis.Instance, key string) (error, int64) {
	var lastTs int64
	err := db.Apply(func(conn redis.Conn) error {
		reply, err := conn.Do(ReversedRange, key, 0, 0, "WITHSCORES")
		if err != nil {
			return err
		}
		var tmp [][]byte
		tmp, err = redis.ByteSlices(reply, err)
		if err != nil {
			return err
		}
		if len(tmp) == 0 {
			return nil
		}
		lastTs, err = strconv.ParseInt(string(tmp[1]), 10, 64)
		return err
	})
	if err != nil {
		return err, 0
	}
	return nil, lastTs
}
