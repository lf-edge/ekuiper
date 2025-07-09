// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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

	kvEncoding "github.com/lf-edge/ekuiper/v2/internal/pkg/store/encoding"
)

type ts struct {
	database Database
	table    string
	last     int64
}

func createSqlTs(database Database, table string) (*ts, error) {
	if !isValidTableName(table) {
		return nil, fmt.Errorf("invalid table name: %s", table)
	}
	store := &ts{
		database: database,
		table:    table,
		last:     getLast(database, table),
	}
	err := store.database.Apply(func(db *sql.DB) error {
		query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ('key' INTEGER PRIMARY KEY, 'val' BLOB);", table)
		stmt, err := db.Prepare(query)
		if err != nil {
			return err
		}
		_, err = stmt.Exec()
		return err
	})
	if err != nil {
		return nil, err
	}
	return store, nil
}

func (t *ts) Set(key int64, value interface{}) (bool, error) {
	if key <= t.last {
		return false, nil
	}
	b, err := kvEncoding.Encode(value)
	if err != nil {
		return false, err
	}
	err = t.database.Apply(func(db *sql.DB) error {
		query := fmt.Sprintf("INSERT INTO %s(key,val) values(?,?);", t.table)
		stmt, err := db.Prepare(query)
		if err != nil {
			return err
		}
		defer stmt.Close()
		_, err = stmt.Exec(key, b)
		if err != nil {
			return err
		}
		t.last = key
		return nil
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

func (t ts) Get(key int64, value interface{}) (bool, error) {
	result := false
	err := t.database.Apply(func(db *sql.DB) error {
		query := fmt.Sprintf("SELECT val FROM %s WHERE key=?;", t.table)
		stmt, err := db.Prepare(query)
		if err != nil {
			return err
		}
		row := stmt.QueryRow(key)
		var tmp []byte
		switch err := row.Scan(&tmp); err {
		case sql.ErrNoRows:
			return nil
		case nil:
		default:
			return err
		}

		dec := gob.NewDecoder(bytes.NewBuffer(tmp))
		if err := dec.Decode(value); err != nil {
			return err
		}
		result = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return result, nil
}

func (t ts) Last(value interface{}) (int64, error) {
	_, err := t.Get(t.last, value)
	if err != nil {
		return 0, err
	}
	return t.last, nil
}

func (t ts) Delete(key int64) error {
	return t.database.Apply(func(db *sql.DB) error {
		query := fmt.Sprintf("DELETE FROM %s WHERE key=?;", t.table)
		stmt, err := db.Prepare(query)
		if err != nil {
			return err
		}
		_, err = stmt.Exec(key)
		return err
	})
}

func (t ts) DeleteBefore(key int64) error {
	return t.database.Apply(func(db *sql.DB) error {
		query := fmt.Sprintf("DELETE FROM %s WHERE key<?;", t.table)
		stmt, err := db.Prepare(query)
		if err != nil {
			return err
		}
		_, err = stmt.Exec(key)
		return err
	})
}

func (t ts) Close() error {
	return nil
}

func (t ts) Drop() error {
	return t.database.Apply(func(db *sql.DB) error {
		query := fmt.Sprintf("Drop table %s;", t.table)
		_, err := db.Exec(query)
		return err
	})
}

func getLast(d Database, table string) int64 {
	var last int64 = 0
	if !isValidTableName(table) {
		return 0 // or handle the error appropriately
	}
	_ = d.Apply(func(db *sql.DB) error {
		query := fmt.Sprintf("SELECT key FROM %s ORDER BY key DESC LIMIT 1;", table)
		stmt, err := db.Prepare(query)
		if err != nil {
			return err
		}
		row := stmt.QueryRow()
		return row.Scan(&last)
	})
	return last
}
