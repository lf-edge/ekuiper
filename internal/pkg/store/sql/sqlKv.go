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

package sql

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	dbSql "github.com/lf-edge/ekuiper/internal/pkg/db/sql"
	kvEncoding "github.com/lf-edge/ekuiper/internal/pkg/store/encoding"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	"strings"
)

type sqlKvStore struct {
	database dbSql.Database
	table    string
}

func createSqlKvStore(database dbSql.Database, table string) (*sqlKvStore, error) {
	store := &sqlKvStore{
		database: database,
		table:    table,
	}
	err := store.database.Apply(func(db *sql.DB) error {
		query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS '%s'('key' VARCHAR(255) PRIMARY KEY, 'val' BLOB);", table)
		_, err := db.Exec(query)
		return err
	})
	if err != nil {
		return nil, err
	}
	return store, nil
}

func (kv *sqlKvStore) Setnx(key string, value interface{}) error {
	return kv.database.Apply(func(db *sql.DB) error {
		err, b := kvEncoding.Encode(value)
		if nil != err {
			return err
		}
		query := fmt.Sprintf("INSERT INTO '%s'(key,val) values(?,?);", kv.table)
		stmt, err := db.Prepare(query)
		_, err = stmt.Exec(key, b)
		stmt.Close()
		if err != nil {
			if strings.Contains(err.Error(), "UNIQUE constraint failed") {
				return fmt.Errorf(`Item %s already exists`, key)
			}
		}
		return err
	})
}

func (kv *sqlKvStore) Set(key string, value interface{}) error {
	err, b := kvEncoding.Encode(value)
	if nil != err {
		return err
	}
	err = kv.database.Apply(func(db *sql.DB) error {
		query := fmt.Sprintf("REPLACE INTO '%s'(key,val) values(?,?);", kv.table)
		stmt, err := db.Prepare(query)
		if err != nil {
			return err
		}
		_, err = stmt.Exec(key, b)
		stmt.Close()
		return err
	})
	return err
}

func (kv *sqlKvStore) Get(key string, value interface{}) (bool, error) {
	result := false
	err := kv.database.Apply(func(db *sql.DB) error {
		query := fmt.Sprintf("SELECT val FROM '%s' WHERE key='%s';", kv.table, key)
		row := db.QueryRow(query)
		var tmp []byte
		err := row.Scan(&tmp)
		if err != nil {
			result = false
			return nil
		}
		dec := gob.NewDecoder(bytes.NewBuffer(tmp))
		if err := dec.Decode(value); err != nil {
			return err
		}
		result = true
		return nil
	})
	return result, err
}

func (kv *sqlKvStore) Delete(key string) error {
	return kv.database.Apply(func(db *sql.DB) error {
		query := fmt.Sprintf("SELECT key FROM '%s' WHERE key='%s';", kv.table, key)
		row := db.QueryRow(query)
		var tmp []byte
		err := row.Scan(&tmp)
		if nil != err || 0 == len(tmp) {
			return errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("%s is not found", key))
		}
		query = fmt.Sprintf("DELETE FROM '%s' WHERE key='%s';", kv.table, key)
		_, err = db.Exec(query)
		return err
	})
}

func (kv *sqlKvStore) Keys() ([]string, error) {
	keys := make([]string, 0)
	err := kv.database.Apply(func(db *sql.DB) error {
		query := fmt.Sprintf("SELECT key FROM '%s'", kv.table)
		row, err := db.Query(query)
		if nil != err {
			return err
		}
		defer row.Close()
		for row.Next() {
			var val string
			err = row.Scan(&val)
			if nil != err {
				return err
			} else {
				keys = append(keys, val)
			}
		}
		return nil
	})
	return keys, err
}

func (kv *sqlKvStore) Clean() error {
	return kv.database.Apply(func(db *sql.DB) error {
		query := fmt.Sprintf("DELETE FROM '%s'", kv.table)
		_, err := db.Exec(query)
		return err
	})
}
