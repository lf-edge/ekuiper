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

package main

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

type RedisSink struct {
	// host:port address.
	addr     string
	username string
	// Optional password. Must match the password specified in the
	password string
	// Database to be selected after connecting to the server.
	db int

	// key of field
	field string

	// key define
	key string

	dataType string

	expiration time.Duration

	sendSingle bool

	cli *redis.Client
}

func (r *RedisSink) Configure(props map[string]interface{}) error {
	if i, ok := props["addr"]; ok {
		if i, ok := i.(string); ok {
			r.addr = i
		}
	} else {
		return errors.New("redis addr is null")
	}

	if i, ok := props["password"]; ok {
		if i, ok := i.(string); ok {
			r.password = i
		}
	}

	r.db = 0
	if i, ok := props["db"]; ok {
		if t, err := cast.ToInt(i, cast.STRICT); err == nil {
			r.db = t
		}
	}

	if i, ok := props["key"]; ok {
		if i, ok := i.(string); ok {
			r.key = i
		}
	} else {
		return errors.New("not config data key for redis")
	}

	if i, ok := props["field"]; ok {
		if i, ok := i.(string); ok {
			r.field = i
		}
	}

	r.sendSingle = true
	if i, ok := props["sendSingle"]; ok {
		if i, ok := i.(bool); ok {
			r.sendSingle = i
		}
	}

	r.dataType = "string"
	if i, ok := props["dataType"]; ok {
		if i, ok := i.(string); ok {
			r.dataType = i
		}
	}

	r.expiration = -1
	if i, ok := props["expiration"]; ok {
		if t, err := cast.ToInt(i, cast.STRICT); err == nil {
			r.expiration = time.Duration(t)
		}
	}

	return nil
}

func (r *RedisSink) Open(ctx api.StreamContext) (err error) {
	logger := ctx.GetLogger()
	logger.Debug("Opening redis sink")

	r.cli = redis.NewClient(&redis.Options{
		Addr:     r.addr,
		Username: r.username,
		Password: r.password,
		DB:       r.db, // use default DB
	})

	return nil
}

func (r *RedisSink) Collect(ctx api.StreamContext, data interface{}) error {
	logger := ctx.GetLogger()

	if v, ok := data.([]byte); ok {
		if r.field != "" {
			if !r.sendSingle {
				var out []map[string]interface{}
				if err := json.Unmarshal(v, &out); err != nil {
					logger.Debug("Failed to unmarshal data with error: ", err, " data:", string(v))
					return err
				}

				for _, m := range out {
					key := r.field
					field, ok := m[key].(string)
					if ok {
						key = field
					}

					if r.dataType == "list" {
						err := r.cli.LPush(ctx, key, v).Err()
						if err != nil {
							logger.Error(err)
							return err
						}
						logger.Debugf("send redis list success, key:%s data: %s", key, string(v))
					} else {
						err := r.cli.Set(ctx, key, v, r.expiration*time.Second).Err()
						if err != nil {
							logger.Error(err)
							return err
						}
						logger.Debugf("send redis string success, key:%s data: %s", key, string(v))
					}
				}
			} else {
				var out map[string]interface{}
				if err := json.Unmarshal(v, &out); err != nil {
					logger.Debug("Failed to unmarshal data with error: ", err, " data:", string(v))
					return err
				}
				key := r.field
				field, ok := out[key].(string)
				if ok {
					key = field
				}

				if r.dataType == "list" {
					err := r.cli.LPush(ctx, key, v).Err()
					if err != nil {
						logger.Error(err)
						return err
					}
					logger.Debugf("send redis list success, key:%s data: %s", key, string(v))
				} else {
					err := r.cli.Set(ctx, key, v, r.expiration*time.Second).Err()
					if err != nil {
						logger.Error(err)
						return err
					}
					logger.Debugf("send redis string success, key:%s data: %s", key, string(v))
				}
			}

		} else if r.key != "" {
			if r.dataType == "list" {
				err := r.cli.LPush(ctx, r.key, v).Err()
				if err != nil {
					logger.Error(err)
					return err
				}
				logger.Debugf("send redis list success, key:%s data: %s", r.key, string(v))
			} else {
				err := r.cli.Set(ctx, r.key, v, r.expiration*time.Second).Err()
				if err != nil {
					logger.Error(err)
					return err
				}
				logger.Debugf("send redis string success, key:%s data: %s", r.key, string(v))
			}
		}

		logger.Debug("insert success", string(v))
	} else {
		logger.Debug("insert failed data is not []byte data:", data)
	}
	return nil
}

func (r *RedisSink) Close(ctx api.StreamContext) error {
	err := r.cli.Close()
	return err
}

func Redis() api.Sink {
	return &RedisSink{}
}
