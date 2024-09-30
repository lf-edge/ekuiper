// Copyright 2023-2024 EMQ Technologies Co., Ltd.
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
	"fmt"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/redis/go-redis/v9"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type redisSub struct {
	conf *redisSubConfig
	conn *redis.Client
}

type redisSubConfig struct {
	Address  string   `json:"address"`
	Db       int      `json:"db"`
	Username string   `json:"username"`
	Password string   `json:"password"`
	Channels []string `json:"channels"`
}

func (r *redisSub) Validate(props map[string]any) error {
	cfg := &redisSubConfig{}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if cfg.Db < 0 || cfg.Db > 15 {
		return fmt.Errorf("redisSub db should be in range 0-15")
	}
	r.conf = cfg
	return nil
}

func (r *redisSub) Ping(ctx api.StreamContext, props map[string]any) error {
	if err := r.Validate(props); err != nil {
		return err
	}
	r.conn = redis.NewClient(&redis.Options{
		Addr:     r.conf.Address,
		Username: r.conf.Username,
		Password: r.conf.Password,
		DB:       r.conf.Db,
	})
	if err := r.conn.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("Ping Redis failed with error: %v", err)
	}
	return nil
}

func (r *redisSub) Provision(ctx api.StreamContext, props map[string]any) error {
	return r.Validate(props)
}

func (r *redisSub) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	ctx.GetLogger().Infof("redisSub is opening")
	r.conn = redis.NewClient(&redis.Options{
		Addr:     r.conf.Address,
		Username: r.conf.Username,
		Password: r.conf.Password,
		DB:       r.conf.Db,
	})
	_, err := r.conn.Ping(ctx).Result()
	if err != nil {
		sch(api.ConnectionDisconnected, err.Error())
		return err
	}
	sch(api.ConnectionConnected, "")
	return nil
}

func (r *redisSub) Subscribe(ctx api.StreamContext, ingest api.BytesIngest, _ api.ErrorIngest) error {
	// Subscribe to Redis channels
	sub := r.conn.PSubscribe(ctx, r.conf.Channels...)
	channel := sub.Channel()
	defer sub.Close()
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-channel:
			rcvTime := timex.GetNow()
			ingest(ctx, []byte(msg.Payload), map[string]any{
				"channel": msg.Channel,
			}, rcvTime)
		}
	}
}

func (r *redisSub) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing redisSub source")
	if r.conn != nil {
		err := r.conn.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func RedisSub() api.Source {
	return &redisSub{}
}

var _ util.PingableConn = &redisSub{}
