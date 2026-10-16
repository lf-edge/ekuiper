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
	"database/sql"

	"github.com/lf-edge/ekuiper/v2/internal/conf/logger"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store/definition"
	sqldb "github.com/lf-edge/ekuiper/v2/internal/pkg/store/sql"
)

type StoreConf struct {
	Type         string
	ExtStateType string
	RedisConfig  definition.RedisConfig
	SqliteConfig definition.SqliteConfig
	FdbConfig    definition.FdbConfig
	PebbleConfig definition.PebbleConfig
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
		Fdb: definition.FdbConfig{},
	}

	return Setup(c, false)
}

func SetupWithConfig(sc *StoreConf, setupCheckpointDB bool) error {
	c := definition.Config{
		Type:         sc.Type,
		ExtStateType: sc.ExtStateType,
		Redis:        sc.RedisConfig,
		Sqlite:       sc.SqliteConfig,
		Fdb:          sc.FdbConfig,
		Pebble:       sc.PebbleConfig,
	}
	return Setup(c, setupCheckpointDB)
}

type MainStoreError struct {
	Err error
}

func (e *MainStoreError) Error() string {
	return e.Err.Error()
}

func (e *MainStoreError) Unwrap() error {
	return e.Err
}

func Setup(config definition.Config, setupCheckpointDB bool) error {
	s, err := newStores(config, "sqliteKV.db")
	if err != nil {
		return &MainStoreError{Err: err}
	}
	globalStores = s

	isSqlite := config.Type == "sqlite"

	if setupCheckpointDB {
		s, err = newStores(config, "checkpoint.db")
		if err != nil {
			if !isSqlite {
				return err
			}
			checkpointStores = nil
			checkpointStoreErr = err
			logger.Log.Errorf("failed to init checkpoint store: %v", err)
		} else {
			checkpointStores = s
			db, err := checkpointStores.GetKV("init_checkpointdb")
			if err != nil {
				_ = s.Disconnect()
				checkpointStores = nil
				if !isSqlite {
					return err
				}
				checkpointStoreErr = err
				logger.Log.Errorf("failed to init checkpoint db kv: %v", err)
			} else {
				// write sth to ensure checkpoint.db created
				err = db.Set("init", "init")
				if err != nil {
					_ = s.Disconnect()
					checkpointStores = nil
					if !isSqlite {
						return err
					}
					checkpointStoreErr = err
					logger.Log.Errorf("failed to set checkpoint init marker: %v", err)
				}
			}
		}
	}
	s, err = newStores(config, "cache.db")
	if err != nil {
		if !isSqlite {
			return err
		}
		cacheStores = nil
		logger.Log.Errorf("failed to init cache store: %v", err)
	} else {
		cacheStores = s
	}
	s, err = newExtStateStores(config, "extState.db")
	if err != nil {
		if config.ExtStateType != "sqlite" {
			return err
		}
		extStateStores = nil
		logger.Log.Errorf("failed to init extState store: %v", err)
	} else {
		extStateStores = s
	}
	db, err := sqldb.BuildSqliteStore(config, "trace.db")
	if err != nil {
		TraceStores = nil
		logger.Log.Errorf("failed to init trace store: %v", err)
	} else {
		TraceStores = db
		err = TraceStores.Apply(func(db *sql.DB) error {
			_, err := db.Exec(`CREATE TABLE IF NOT EXISTS trace (traceID TEXT PRIMARY KEY, ruleID TEXT NOT NULL, value BLOB,createdtimestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP);`)
			return err
		})
		if err != nil {
			_ = TraceStores.Disconnect()
			TraceStores = nil
			logger.Log.Errorf("failed to apply trace schema: %v", err)
		}
	}
	return nil
}
