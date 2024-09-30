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
	"context"
	"fmt"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/redis/go-redis/v9"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
)

type redisPub struct {
	conf *redisPubConfig
	conn *redis.Client
}

type redisPubConfig struct {
	Address  string `json:"address"`
	Db       int    `json:"db"`
	Username string `json:"username"`
	Password string `json:"password"`
	Channel  string `json:"channel"`
}

func (r *redisPub) Validate(props map[string]any) error {
	cfg := &redisPubConfig{}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if cfg.Db < 0 || cfg.Db > 15 {
		return fmt.Errorf("redisPub db should be in range 0-15")
	}
	if cfg.Channel == "" {
		return fmt.Errorf("redisPub sink is missing property channel")
	}
	r.conf = cfg
	return nil
}

func (r *redisPub) Ping(ctx api.StreamContext, props map[string]any) error {
	if err := r.Validate(props); err != nil {
		return err
	}
	r.conn = redis.NewClient(&redis.Options{
		Addr:     r.conf.Address,
		Username: r.conf.Username,
		Password: r.conf.Password,
		DB:       r.conf.Db,
	})
	if err := r.conn.Ping(context.Background()).Err(); err != nil {
		return fmt.Errorf("Ping Redis failed with error: %v", err)
	}
	return nil
}

func (r *redisPub) Provision(ctx api.StreamContext, props map[string]any) error {
	return r.Validate(props)
}

func (r *redisPub) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
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

func (r *redisPub) Collect(ctx api.StreamContext, item api.RawTuple) error {
	// Publish
	err := r.conn.Publish(ctx, r.conf.Channel, item.Raw()).Err()
	if err != nil {
		return errorx.NewIOErr(fmt.Sprintf(`Error occurred while publishing the Redis message to %s`, r.conf.Address))
	}
	return nil
}

func (r *redisPub) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing redisPub sink")
	if r.conn != nil {
		err := r.conn.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func RedisPub() api.Sink {
	return &redisPub{}
}

var (
	_ api.BytesCollector = &redisPub{}
	_ util.PingableConn  = &redisPub{}
)
