// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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

package io

import (
	"github.com/lf-edge/ekuiper/v2/internal/io/redis"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func init() {
	modules.RegisterLookupSource("redis", redis.GetLookupSource)
	modules.RegisterSink("redis", redis.GetSink)
	modules.RegisterSink("redisPub", redis.RedisPub)
	modules.RegisterSource("redisSub", redis.RedisSub)
}
