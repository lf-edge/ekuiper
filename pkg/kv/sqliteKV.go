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

package kv

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type SqliteKVStore struct {
	db    *sql.DB
	table string
	path  string
}

func init() {
	gob.Register(make(map[string]interface{}))
}

func GetSqliteKVStore(fpath string) (ret *SqliteKVStore) {
	dir, file := filepath.Split(fpath)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, os.ModePerm)
	}
	ret = new(SqliteKVStore)
	ret.path = path.Join(dir, "sqliteKV.db")
	ret.table = file
	return ret
}

func (m *SqliteKVStore) Open() error {
	db, err := sql.Open("sqlite3", m.path)
	if nil != err {
		return err
	}
	m.db = db
	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS '%s'('key' VARCHAR(255) PRIMARY KEY, 'val' BLOB);", m.table)
	_, err = m.db.Exec(sql)
	return err
}

func (m *SqliteKVStore) Close() error {
	if nil != m.db {
		return m.db.Close()
	}
	return nil
}

func (m *SqliteKVStore) encode(value interface{}) ([]byte, error) {
	var buf bytes.Buffer
	gob.Register(value)
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(value); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (m *SqliteKVStore) Setnx(key string, value interface{}) error {
	b, err := m.encode(value)
	if nil != err {
		return err
	}
	sql := fmt.Sprintf("INSERT INTO %s(key,val) values(?,?);", m.table)
	stmt, err := m.db.Prepare(sql)
	_, err = stmt.Exec(key, b)
	stmt.Close()
	if nil != err && strings.Contains(err.Error(), "UNIQUE constraint failed") {
		return fmt.Errorf(`Item %s already exists`, key)
	}
	return err
}

func (m *SqliteKVStore) Set(key string, value interface{}) error {
	b, err := m.encode(value)
	if nil != err {
		return err
	}
	sql := fmt.Sprintf("REPLACE INTO %s(key,val) values(?,?);", m.table)
	stmt, err := m.db.Prepare(sql)
	_, err = stmt.Exec(key, b)
	stmt.Close()
	return err
}

func (m *SqliteKVStore) Get(key string, value interface{}) (bool, error) {
	sql := fmt.Sprintf("SELECT val FROM %s WHERE key='%s';", m.table, key)
	row := m.db.QueryRow(sql)
	var tmp []byte
	err := row.Scan(&tmp)
	if nil != err {
		return false, nil
	}

	dec := gob.NewDecoder(bytes.NewBuffer(tmp))
	if err := dec.Decode(value); err != nil {
		return false, err
	}
	return true, nil
}

func (m *SqliteKVStore) Delete(key string) error {
	sql := fmt.Sprintf("SELECT key FROM %s WHERE key='%s';", m.table, key)
	row := m.db.QueryRow(sql)
	var tmp []byte
	err := row.Scan(&tmp)
	if nil != err || 0 == len(tmp) {
		return errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("%s is not found", key))
	}
	sql = fmt.Sprintf("DELETE FROM %s WHERE key='%s';", m.table, key)
	_, err = m.db.Exec(sql)
	return err
}

func (m *SqliteKVStore) Keys() ([]string, error) {
	keys := make([]string, 0)
	sql := fmt.Sprintf("SELECT key FROM %s", m.table)
	row, err := m.db.Query(sql)
	if nil != err {
		return nil, err
	}
	defer row.Close()
	for row.Next() {
		var val string
		err = row.Scan(&val)
		if nil != err {
			return nil, err
		} else {
			keys = append(keys, val)
		}
	}
	return keys, nil
}

func (m *SqliteKVStore) Clean() error {
	sql := fmt.Sprintf("DELETE FROM %s", m.table)
	_, err := m.db.Exec(sql)
	return err
}
