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
	"github.com/lf-edge/ekuiper/internal/pkg/store/definition"
)

type StoreConf struct {
	Type         string
	ExtStateType string
	RedisConfig  definition.RedisConfig
	SqliteConfig definition.SqliteConfig
}

func SetupDefault(dataDir string) error {
	c := definition.Config{
		Type:         "sqlite",
		ExtStateType: "sqlite",
		Redis:        definition.RedisConfig{},
		Sqlite: definition.SqliteConfig{
			Path: dataDir,
			Name: "",
		},
	}

	return Setup(c)
}

func SetupWithConfig(sc *StoreConf) error {
	c := definition.Config{
		Type:         sc.Type,
		ExtStateType: sc.ExtStateType,
		Redis:        sc.RedisConfig,
		Sqlite:       sc.SqliteConfig,
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
