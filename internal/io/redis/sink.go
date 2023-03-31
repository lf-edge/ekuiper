// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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

package redis

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/redis/go-redis/v9"
	"time"
)

type config struct {
	// host:port address.
	Addr     string `json:"addr,omitempty"`
	Username string `json:"username,omitempty"`
	// Optional password. Must match the password specified in the
	Password string `json:"password,omitempty"`
	// Database to be selected after connecting to the server.
	Db int `json:"db,omitempty"`
	// key of field
	Field string `json:"field,omitempty"`
	// key define
	Key          string        `json:"key,omitempty"`
	DataType     string        `json:"dataType,omitempty"`
	Expiration   time.Duration `json:"expiration,omitempty"`
	RowkindField string        `json:"rowkindField"`
	DataTemplate string        `json:"dataTemplate"`
}

type RedisSink struct {
	c   *config
	cli *redis.Client
}

func (r *RedisSink) Configure(props map[string]interface{}) error {
	c := &config{DataType: "string", Expiration: -1}
	err := cast.MapToStruct(props, c)
	if err != nil {
		return err
	}
	if c.Key == "" && c.Field == "" {
		return errors.New("redis sink must have key or field")
	}
	if c.DataType != "string" && c.DataType != "list" {
		return errors.New("redis sink only support string or list data type")
	}
	r.c = c
	return nil
}

func (r *RedisSink) Open(ctx api.StreamContext) (err error) {
	logger := ctx.GetLogger()
	logger.Debug("Opening redis sink")

	r.cli = redis.NewClient(&redis.Options{
		Addr:     r.c.Addr,
		Username: r.c.Username,
		Password: r.c.Password,
		DB:       r.c.Db, // use default DB
	})
	_, err = r.cli.Ping(ctx).Result()
	return err
}

func (r *RedisSink) Collect(ctx api.StreamContext, data interface{}) error {
	logger := ctx.GetLogger()
	var val string
	if r.c.DataTemplate != "" { // The result is a string
		v, _, err := ctx.TransformOutput(data)
		if err != nil {
			logger.Error(err)
			return err
		}
		m := make(map[string]interface{})
		err = json.Unmarshal(v, &m)
		if err != nil {
			return fmt.Errorf("fail to decode data %s after applying dataTemplate for error %v", string(v), err)
		}
		data = m
		val = string(v)
	}
	switch d := data.(type) {
	case []map[string]interface{}:
		for _, el := range d {
			err := r.save(ctx, el, val)
			if err != nil {
				return err
			}
		}
	case map[string]interface{}:
		err := r.save(ctx, d, val)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unrecognized format of %s", data)
	}
	logger.Debug("insert success %v", data)
	return nil
}

func (r *RedisSink) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing redis sink")
	err := r.cli.Close()
	return err
}

func (r *RedisSink) save(ctx api.StreamContext, data map[string]interface{}, val string) error {
	logger := ctx.GetLogger()
	if val == "" {
		jsonBytes, err := json.Marshal(data)
		if err != nil {
			return err
		}
		val = string(jsonBytes)
	}
	key := r.c.Key
	var err error
	if r.c.Field != "" {
		keyval, ok := data[r.c.Field]
		if !ok {
			return fmt.Errorf("field %s does not exist in data %v", r.c.Field, data)
		}
		key, err = cast.ToString(keyval, cast.CONVERT_ALL)
		if err != nil {
			return fmt.Errorf("key must be string or convertible to string, but got %v", keyval)
		}
	}
	rowkind := ast.RowkindUpsert
	if r.c.RowkindField != "" {
		c, ok := data[r.c.RowkindField]
		if ok {
			rowkind, ok = c.(string)
			if !ok {
				return fmt.Errorf("rowkind field %s is not a string in data %v", r.c.RowkindField, data)
			}
			if rowkind != ast.RowkindInsert && rowkind != ast.RowkindUpdate && rowkind != ast.RowkindDelete && rowkind != ast.RowkindUpsert {
				return fmt.Errorf("invalid rowkind %s", rowkind)
			}
		}
	}
	switch rowkind {
	case ast.RowkindInsert, ast.RowkindUpdate, ast.RowkindUpsert:
		if r.c.DataType == "list" {
			err = r.cli.LPush(ctx, key, val).Err()
			if err != nil {
				return fmt.Errorf("lpush %s:%s error, %v", key, val, err)
			}
			logger.Debugf("push redis list success, key:%s data: %v", key, val)
		} else {
			err = r.cli.Set(ctx, key, val, r.c.Expiration*time.Second).Err()
			if err != nil {
				return fmt.Errorf("set %s:%s error, %v", key, val, err)
			}
			logger.Debugf("set redis string success, key:%s data: %s", key, val)
		}
	case ast.RowkindDelete:
		if r.c.DataType == "list" {
			err = r.cli.LPop(ctx, key).Err()
			if err != nil {
				return fmt.Errorf("lpop %s error, %v", key, err)
			}
			logger.Debugf("pop redis list success, key:%s data: %v", key, val)
		} else {
			err = r.cli.Del(ctx, key).Err()
			if err != nil {
				logger.Error(err)
				return err
			}
			logger.Debugf("delete redis string success, key:%s data: %s", key, val)
		}
	default:
		// never happen
		logger.Errorf("unexpected rowkind %s", rowkind)
	}
	return nil
}

func GetSink() api.Sink {
	return &RedisSink{}
}
