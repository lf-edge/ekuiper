// Copyright 2024 EMQ Technologies Co., Ltd.
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

package client

import (
	"database/sql"
	"fmt"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
	"github.com/lf-edge/ekuiper/v2/pkg/sqldatabase/driver"
)

type SQLConnection struct {
	url string
	db  *sql.DB
}

func (s *SQLConnection) GetDB() *sql.DB {
	return s.db
}

func (s *SQLConnection) Ping(ctx api.StreamContext) error {
	return s.db.Ping()
}

func (s *SQLConnection) DetachSub(ctx api.StreamContext, props map[string]any) {
	return
}

func (s *SQLConnection) Close(ctx api.StreamContext) error {
	conf.Log.Infof("close db with url:%v", s.url)
	s.db.Close()
	return nil
}

func CreateConnection(ctx api.StreamContext, props map[string]any) (modules.Connection, error) {
	return CreateClient(ctx, props)
}

func CreateClient(ctx api.StreamContext, props map[string]any) (*SQLConnection, error) {
	dbUrlRaw, ok := props["dburl"]
	if !ok {
		return nil, fmt.Errorf("dburl should be defined")
	}
	dburl, ok := dbUrlRaw.(string)
	if !ok || len(dburl) < 1 {
		return nil, fmt.Errorf("dburl should be defined as string")
	}
	conf.Log.Infof("create db with url:%v", dburl)
	db, err := openDB(dburl)
	if err != nil {
		return nil, fmt.Errorf("create connection err:%v, supported drivers:%v", err, driver.GetSupportedDrivers())
	}
	return &SQLConnection{
		url: dburl,
		db:  db,
	}, nil
}
