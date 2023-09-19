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
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/lf-edge/ekuiper/internal/compressor"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/io"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/infra"
	"github.com/lf-edge/ekuiper/pkg/message"
)

type redisSub struct {
	conf         *redisSubConfig
	conn         *redis.Client
	decompressor message.Decompressor
}

type redisSubConfig struct {
	Address       string   `json:"address"`
	Db            int      `json:"db"`
	Username      string   `json:"username"`
	Password      string   `json:"password"`
	Channels      []string `json:"channels"`
	Decompression string   `json:"decompression"`
}

func (r *redisSub) Configure(_ string, props map[string]interface{}) error {
	cfg := &redisSubConfig{}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	r.conf = cfg
	r.conn = redis.NewClient(&redis.Options{
		Addr:     r.conf.Address,
		Username: r.conf.Username,
		Password: r.conf.Password,
		DB:       r.conf.Db,
	})

	if cfg.Decompression != "" {
		dc, err := compressor.GetDecompressor(cfg.Decompression)
		if err != nil {
			return fmt.Errorf("get decompressor %s fail with error: %v", cfg.Decompression, err)
		}
		r.decompressor = dc
	}

	// Ping Redis to check if the connection is alive
	err = r.conn.Ping(context.Background()).Err()
	if err != nil {
		return fmt.Errorf("Ping Redis failed with error: %v", err)
	}
	return nil
}

func (r *redisSub) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	logger := ctx.GetLogger()
	logger.Infof("redisSub sink Opening")
	err := subscribe(r, ctx, consumer)
	if err != nil {
		infra.DrainError(ctx, err, errCh)
	}
}

func subscribe(r *redisSub, ctx api.StreamContext, consumer chan<- api.SourceTuple) error {
	// Subscribe to Redis channels
	sub := r.conn.PSubscribe(ctx, r.conf.Channels...)
	channel := sub.Channel()
	defer sub.Close()
	var tuples []api.SourceTuple
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-channel:
			tuples = getTuples(ctx, r, msg)
		}
		io.ReceiveTuples(ctx, consumer, tuples)
	}
}

func getTuples(ctx api.StreamContext, r *redisSub, env interface{}) []api.SourceTuple {
	rcvTime := conf.GetNow()
	msg, ok := env.(*redis.Message)
	if !ok { // should never happen
		return []api.SourceTuple{
			&xsql.ErrorSourceTuple{
				Error: fmt.Errorf("can not convert interface data to redis message %v.", env),
			},
		}
	}
	payload := []byte(msg.Payload)
	var err error
	if r.decompressor != nil {
		payload, err = r.decompressor.Decompress(payload)
		if err != nil {
			return []api.SourceTuple{
				&xsql.ErrorSourceTuple{
					Error: fmt.Errorf("can not decompress redis message %v.", err),
				},
			}
		}
	}
	results, e := ctx.DecodeIntoList(payload)
	// The unmarshal type can only be bool, float64, string, []interface{}, map[string]interface{}, nil
	if e != nil {
		return []api.SourceTuple{
			&xsql.ErrorSourceTuple{
				Error: fmt.Errorf("Invalid data format, cannot decode %s with error %s", payload, e),
			},
		}
	}

	meta := make(map[string]interface{})
	meta["channel"] = msg.Channel

	tuples := make([]api.SourceTuple, 0, len(results))
	for _, result := range results {
		tuples = append(tuples, api.NewDefaultSourceTupleWithTime(result, meta, rcvTime))
	}
	return tuples
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
