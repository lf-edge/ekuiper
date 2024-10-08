// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/redis/go-redis/v9"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

type conf struct {
	// host:port address.
	Addr     string `json:"addr,omitempty"`
	Username string `json:"username,omitempty"`
	// Optional password. Must match the password specified in the
	Password string `json:"password,omitempty"`
	DataType string `json:"dataType,omitempty"`
	DB       string `json:"datasource,omitempty"`
}

type lookupSource struct {
	c   *conf
	db  int
	cli *redis.Client
}

func (s *lookupSource) Ping(ctx api.StreamContext, props map[string]any) error {
	err := s.Validate(props)
	if err != nil {
		return err
	}
	s.cli = redis.NewClient(&redis.Options{
		Addr:     s.c.Addr,
		Username: s.c.Username,
		Password: s.c.Password,
		DB:       s.db, // use default DB
	})
	defer s.cli.Close()
	_, err = s.cli.Ping(ctx).Result()
	return err
}

func (s *lookupSource) Provision(ctx api.StreamContext, props map[string]any) error {
	return s.Validate(props)
}

func (s *lookupSource) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	logger := ctx.GetLogger()
	logger.Debug("Opening redis lookup source")

	s.cli = redis.NewClient(&redis.Options{
		Addr:     s.c.Addr,
		Username: s.c.Username,
		Password: s.c.Password,
		DB:       s.db, // use default DB
	})
	_, err := s.cli.Ping(ctx).Result()
	if err != nil {
		sch(api.ConnectionDisconnected, err.Error())
		return err
	}
	sch(api.ConnectionConnected, "")
	return nil
}

func (s *lookupSource) Lookup(ctx api.StreamContext, _ []string, keys []string, values []any) ([]map[string]any, error) {
	ctx.GetLogger().Debugf("Lookup redis %v", keys)
	if len(keys) != 1 {
		return nil, fmt.Errorf("redis lookup only support one key, but got %v", keys)
	}
	v := fmt.Sprintf("%v", values[0])
	if s.c.DataType == "string" {
		res, err := s.cli.Get(ctx, v).Result()
		if err != nil {
			if err == redis.Nil {
				return []map[string]any{}, nil
			}
			return nil, err
		}
		m := make(map[string]any)
		err = json.Unmarshal(cast.StringToBytes(res), &m)
		if err != nil {
			return nil, err
		}
		return []map[string]any{m}, nil
	} else {
		res, err := s.cli.LRange(ctx, v, 0, -1).Result()
		if err != nil {
			if err == redis.Nil {
				return []map[string]any{}, nil
			}
			return nil, err
		}
		ret := make([]map[string]any, 0, len(res))
		for _, r := range res {
			m := make(map[string]any)
			err = json.Unmarshal(cast.StringToBytes(r), &m)
			if err != nil {
				return nil, err
			}
			ret = append(ret, m)
		}
		return ret, nil
	}
}

func (s *lookupSource) Validate(props map[string]any) error {
	cfg := &conf{}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return err
	}
	if cfg.Addr == "" {
		return errors.New("redis addr is null")
	}
	if cfg.DataType != "string" && cfg.DataType != "list" {
		return errors.New("redis dataType must be string or list")
	}
	if cfg.DB == "/$$TEST_CONNECTION$$" {
		cfg.DB = "0"
	}
	s.db, err = strconv.Atoi(cfg.DB)
	if err != nil {
		return fmt.Errorf("datasource %s is invalid", cfg.DB)
	}
	if s.db < 0 || s.db > 15 {
		return fmt.Errorf("redis lookup source db should be in range 0-15")
	}
	s.c = cfg
	return nil
}

func (s *lookupSource) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Opening redis lookup source with conf %v", s.c)
	return nil
}

func (s *lookupSource) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing redis lookup source")
	return s.cli.Close()
}

func GetLookupSource() api.Source {
	return &lookupSource{}
}

var (
	_ api.LookupSource  = &lookupSource{}
	_ util.PingableConn = &lookupSource{}
)
