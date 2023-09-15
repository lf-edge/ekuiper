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

package confStore

import (
	"database/sql"
	"encoding/json"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

type sqlKVStore struct {
	db *sql.DB
}

func NewSqliteKVStore(driver, path string) (*sqlKVStore, error) {
	s := &sqlKVStore{}
	db, err := sql.Open(driver, path)
	if err != nil {
		return nil, err
	}
	s.db = db
	_, err = s.db.Exec(`create table if not exists cfg (k TEXT,v TEXT)`)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *sqlKVStore) Set(k string, v map[string]interface{}) error {
	bs, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(fmt.Sprintf(`insert into cfg(k,v) values("%v","%v")`, k, string(bs)))
	return err
}

func (s *sqlKVStore) Delete(k string) error {
	_, err := s.db.Exec(fmt.Sprintf(`delete from cfg where k = "%v"`, k))
	return err
}

func (s *sqlKVStore) GetByPrefix(prefix string) (map[string]map[string]interface{}, error) {
	rows, err := s.db.Query(fmt.Sprintf(`select k,v from cfg where k like "%v%%"`, prefix))
	if err != nil {
		return nil, err
	}
	r := make(map[string]map[string]interface{})
	for rows.Next() {
		var k, v string
		err := rows.Scan(&k, &v)
		if err != nil {
			return nil, err
		}
		d := map[string]interface{}{}
		err = json.Unmarshal([]byte(v), &d)
		if err != nil {
			return nil, err
		}
		r[k] = d
	}
	return r, nil
}
