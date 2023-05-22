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

package store

import (
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/store/definition"
)

func SetupDefault() error {
	dir, err := conf.GetDataLoc()
	if err != nil {
		return err
	}

	c := definition.Config{
		Type:         "sqlite",
		ExtStateType: "sqlite",
		Redis:        definition.RedisConfig{},
		Sqlite: definition.SqliteConfig{
			Path: dir,
			Name: "",
		},
	}

	return Setup(c)
}

func SetupWithKuiperConfig(kconf *conf.KuiperConf) error {
	dir, err := conf.GetDataLoc()
	if err != nil {
		return err
	}
	c := definition.Config{
		Type:         kconf.Store.Type,
		ExtStateType: kconf.Store.ExtStateType,
		Redis: definition.RedisConfig{
			Host:     kconf.Store.Redis.Host,
			Port:     kconf.Store.Redis.Port,
			Password: kconf.Store.Redis.Password,
			Timeout:  kconf.Store.Redis.Timeout,
		},
		Sqlite: definition.SqliteConfig{
			Path: dir,
			Name: kconf.Store.Sqlite.Name,
		},
	}
	return Setup(c)
}

func Setup(config definition.Config) error {
	s, err := newStores(config, "sqliteKV.db")
	if err != nil {
		return err
	}
	globalStores = s
	s, err = newStores(config, "cache.db")
	if err != nil {
		return err
	}
	cacheStores = s
	s, err = newExtStateStores(config, "extState.db")
	if err != nil {
		return err
	}
	extStateStores = s
	return nil
}
