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
	"strings"

	"github.com/xo/dburl"

	_ "github.com/lf-edge/ekuiper/v2/extensions/impl/sql/sqldatabase/driver"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

func ParseDBUrl(urlstr string) (string, string, error) {
	u, err := dburl.Parse(urlstr)
	if err != nil {
		return "", "", err
	}
	// Open returns *sql.DB from urlstr
	// As we use modernc.org/sqlite with `sqlite` as driver name and dburl use `sqlite3` as driver name, we need to fix it before open sql.DB
	if strings.ToLower(u.Driver) == "sqlite3" {
		u.Driver = "sqlite"
	}
	return u.Driver, u.DSN, nil
}

func ParseDriver(url string) (string, error) {
	u, err := dburl.Parse(url)
	if err != nil {
		return "", fmt.Errorf("parse driver err:%v", err)
	}
	return u.Driver, nil
}

func openDB(url string) (*sql.DB, error) {
	driver, dsn, err := ParseDBUrl(url)
	if err != nil {
		return nil, err
	}
	// sql.Open won't check connection, we need ping it later
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	c := conf.Config
	if c != nil && c.Basic.SQLConf != nil && c.Basic.SQLConf.MaxConnections > 0 {
		db.SetMaxOpenConns(c.Basic.SQLConf.MaxConnections)
	}
	return db, nil
}
