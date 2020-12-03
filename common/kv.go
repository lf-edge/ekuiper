package common

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type KeyValue interface {
	Open() error
	Close() error
	// Set key to hold string value if key does not exist otherwise return an error
	Setnx(key string, value interface{}) error
	// Set key to hold the string value. If key already holds a value, it is overwritten
	Set(key string, value interface{}) error
	Get(key string, val interface{}) bool
	//Must return *common.Error with NOT_FOUND error
	Delete(key string) error
	Keys() (keys []string, err error)
	Clean() error
}

type SimpleKVStore struct {
	db    *sql.DB
	table string
	path  string
}

func GetSimpleKVStore(fpath string) (ret *SimpleKVStore) {
	if _, err := os.Stat(fpath); os.IsNotExist(err) {
		os.MkdirAll(fpath, os.ModePerm)
	}
	dir, file := filepath.Split(fpath)
	ret = new(SimpleKVStore)
	ret.path = path.Join(dir, "sqliteKV.db")
	ret.table = file
	return ret
}

func (m *SimpleKVStore) Open() error {
	db, err := sql.Open("sqlite3", m.path)
	if nil != err {
		return err
	}
	m.db = db
	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS '%s'('key' VARCHAR(255) PRIMARY KEY, 'val' BLOB);", m.table)
	_, err = m.db.Exec(sql)
	return err
}

func (m *SimpleKVStore) Close() error {
	if nil != m.db {
		return m.db.Close()
	}
	return nil
}

func (m *SimpleKVStore) encode(value interface{}) ([]byte, error) {
	var buf bytes.Buffer
	gob.Register(value)
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(value); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (m *SimpleKVStore) Setnx(key string, value interface{}) error {
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

func (m *SimpleKVStore) Set(key string, value interface{}) error {
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

func (m *SimpleKVStore) Get(key string, value interface{}) bool {
	sql := fmt.Sprintf("SELECT val FROM %s WHERE key='%s';", m.table, key)
	row := m.db.QueryRow(sql)
	var tmp []byte
	err := row.Scan(&tmp)
	if nil != err {
		return false
	}

	dec := gob.NewDecoder(bytes.NewBuffer(tmp))
	if err := dec.Decode(value); err != nil {
		return false
	}
	return true
}

func (m *SimpleKVStore) Delete(key string) error {
	sql := fmt.Sprintf("SELECT key FROM %s WHERE key='%s';", m.table, key)
	row := m.db.QueryRow(sql)
	var tmp []byte
	err := row.Scan(&tmp)
	if nil != err || 0 == len(tmp) {
		return NewErrorWithCode(NOT_FOUND, fmt.Sprintf("%s is not found", key))
	}
	sql = fmt.Sprintf("DELETE FROM %s WHERE key='%s';", m.table, key)
	_, err = m.db.Exec(sql)
	return err
}

func (m *SimpleKVStore) Keys() (keys []string, err error) {
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

func (m *SimpleKVStore) Clean() error {
	sql := fmt.Sprintf("DELETE FROM %s", m.table)
	_, err := m.db.Exec(sql)
	return err
}
