// Copyright 2022-2022 EMQ Technologies Co., Ltd.
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

package sqlite

import (
	"database/sql"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/pkg/store/definition"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"path"
	"sync"
)

type Database struct {
	db   *sql.DB
	Path string
	mu   sync.Mutex
}

func NewSqliteDatabase(c definition.Config) (definition.Database, error) {
	conf := c.Sqlite
	dir := conf.Path
	name := "sqliteKV.db"
	if conf.Name != "" {
		name = conf.Name
	}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, os.ModePerm)
	}
	dbPath := path.Join(dir, name)
	return &Database{
		db:   nil,
		Path: dbPath,
		mu:   sync.Mutex{},
	}, nil
}

func (d *Database) Connect() error {
	db, err := sql.Open("sqlite3", connectionString(d.Path))
	if err != nil {
		return err
	}
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(-1)
	d.db = db
	return nil
}

func connectionString(dpath string) string {
	return fmt.Sprintf("file:%s?cache=shared", dpath)
}

func (d *Database) Disconnect() error {
	err := d.db.Close()
	return err
}

func (d *Database) Apply(f func(db *sql.DB) error) error {
	d.mu.Lock()
	err := f(d.db)
	d.mu.Unlock()
	return err
}
