// Copyright 2023-2023 emy120115@gmail.com
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

package pubsub

import (
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/lf-edge/ekuiper/internal/compressor"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	"github.com/lf-edge/ekuiper/pkg/message"
)

type redisPub struct {
	conf       *redisPubConfig
	conn       *redis.Client
	compressor message.Compressor
}

type redisPubConfig struct {
	Address       string `json:"address"`
	Db            int    `json:"db"`
	Username      string `json:"username"`
	Password      string `json:"password"`
	Channel       string `json:"channel"`
	Compression   string `json:"compression"`
	ResendChannel string `json:"resendDestination"`
}

func (r *redisPub) Configure(props map[string]interface{}) error {
	cfg := &redisPubConfig{}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if cfg.Channel == "" {
		return fmt.Errorf("redisPub sink is missing property channel")
	}
	if cfg.Compression != "" {
		r.compressor, err = compressor.GetCompressor(cfg.Compression)
		if err != nil {
			return fmt.Errorf("invalid compression method %s", cfg.Compression)
		}
	}
	if cfg.ResendChannel == "" {
		cfg.ResendChannel = cfg.Channel
	}
	r.conf = cfg

	return nil
}

func (r *redisPub) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("redisPub sink opening")
	r.conn = redis.NewClient(&redis.Options{
		Addr:     r.conf.Address,
		Username: r.conf.Username,
		Password: r.conf.Password,
		DB:       r.conf.Db,
	})
	// Ping Redis to check if the connection is alive
	err := r.conn.Ping(ctx).Err()
	if err != nil {
		return fmt.Errorf("Ping Redis failed with error: %v", err)
	}
	return nil
}

func (r *redisPub) Collect(ctx api.StreamContext, item interface{}) error {
	return r.collectWithChannel(ctx, item, r.conf.Channel)
}

func (r *redisPub) CollectResend(ctx api.StreamContext, item interface{}) error {
	return r.collectWithChannel(ctx, item, r.conf.ResendChannel)
}

func (r *redisPub) collectWithChannel(ctx api.StreamContext, item interface{}, channel string) error {
	logger := ctx.GetLogger()
	logger.Debugf("receive %+v", item)
	// Transform
	jsonBytes, _, err := ctx.TransformOutput(item)
	if err != nil {
		return err
	}
	logger.Debugf("%s publish %s", ctx.GetOpId(), jsonBytes)
	// Compress
	if r.compressor != nil {
		jsonBytes, err = r.compressor.Compress(jsonBytes)
		if err != nil {
			return err
		}
	}
	// Publish
	err = r.conn.Publish(ctx, channel, jsonBytes).Err()
	if err != nil {
		return fmt.Errorf("%s: Error occurred while publishing the Redis message to %s", errorx.IOErr, r.conf.Address)
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
