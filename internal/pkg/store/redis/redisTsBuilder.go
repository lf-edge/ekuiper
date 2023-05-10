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
	"github.com/redis/go-redis/v9"

	st "github.com/lf-edge/ekuiper/pkg/kv"
)

type TsBuilder struct {
	redis *redis.Client
}

func NewTsBuilder(d *redis.Client) TsBuilder {
	return TsBuilder{
		redis: d,
	}
}

func (b TsBuilder) CreateTs(table string) (st.Tskv, error) {
	return createRedisTs(b.redis, table)
}
