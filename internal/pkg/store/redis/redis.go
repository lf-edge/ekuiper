// Copyright 2022-2022 EMQ Technologies Co., Ltd.
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
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/lf-edge/ekuiper/internal/pkg/store/definition"
)

func NewRedisFromConf(c definition.Config) *redis.Client {
	conf := c.Redis
	return redis.NewClient(&redis.Options{
		Addr:        fmt.Sprintf("%s:%d", conf.Host, conf.Port),
		Password:    conf.Password,
		DialTimeout: time.Duration(conf.Timeout) * time.Millisecond,
	})
}

func NewRedis(host string, port int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", host, port),
	})
}
