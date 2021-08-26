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

package store

import (
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/db"
	"github.com/lf-edge/ekuiper/internal/pkg/db/redis"
	"github.com/lf-edge/ekuiper/internal/pkg/db/sql/sqlite"
)

func SetupDefault() error {
	dir, err := conf.GetDataLoc()
	if err != nil {
		return err
	}

	c := db.Config{
		Type:  "sqlite",
		Redis: redis.Config{},
		Sqlite: sqlite.Config{
			Path: dir,
			Name: "",
		},
	}

	return Setup(c)
}

func SetupWithKuiperConfig(conf *conf.KuiperConf) error {
	c := db.Config{
		Type: conf.Store.Type,
		Redis: redis.Config{
			Host:     conf.Store.Redis.Host,
			Port:     conf.Store.Redis.Port,
			Password: conf.Store.Redis.Password,
			Timeout:  conf.Store.Redis.Timeout,
		},
		Sqlite: sqlite.Config{
			Path: conf.Store.Sqlite.Path,
			Name: conf.Store.Sqlite.Name,
		},
	}
	return Setup(c)
}

func Setup(config db.Config) error {
	database, err := db.CreateDatabase(config)
	if err != nil {
		return err
	}

	err = InitGlobalStores(database)
	if err != nil {
		return err
	}

	return nil
}
