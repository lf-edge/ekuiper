// Copyright 2023 EMQ Technologies Co., Ltd.
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

package util

import (
	"database/sql"
	"strings"

	"github.com/xo/dburl"
)

// Open returns *sql.DB from urlstr
// As we use modernc.org/sqlite with `sqlite` as driver name and dburl use `sqlite3` as driver name, we need to fix it before open sql.DB
func Open(urlstr string) (*sql.DB, error) {
	u, err := dburl.Parse(urlstr)
	if err != nil {
		return nil, err
	}
	if strings.ToLower(u.Driver) == "sqlite3" {
		u.Driver = "sqlite"
	}
	return sql.Open(u.Driver, u.DSN)
}
