package common

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"sync"
)

type KeyValue interface {
	Open() error
	Close() error
	Set(key, val string) error
	Replace(key, val string) error
	Get(key string) (string, bool)
	//Must return *common.Error with NOT_FOUND error
	Delete(key string) error
	Keys() (keys []string, err error)
	Clean() error
}

var g_sqliteDb *sql.DB

func OpenSqlite(fpath string) error {
	db, err := sql.Open("sqlite3", fpath)
	if nil == err {
		g_sqliteDb = db
	}
	return err
}

func CloseSqlite() {
	if nil != g_sqliteDb {
		g_sqliteDb.Close()
	}
}

type SyncKVMap struct {
	sync.RWMutex
	internal map[string]*SqliteKV
}

func (sm *SyncKVMap) Load(table string) (result *SqliteKV) {
	sm.Lock()
	defer sm.Unlock()
	if s, ok := sm.internal[table]; ok {
		result = s
	} else {
		result = new(SqliteKV)
		result.table = table
		sm.internal[table] = result
	}

	return
}

var stores = &SyncKVMap{
	internal: make(map[string]*SqliteKV),
}

func GetSqliteKV(table string) *SqliteKV {
	return stores.Load(table)
}

type SqliteKV struct {
	table string
}

func (m *SqliteKV) Open() error {
	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS '%s'('key' VARCHAR(255) PRIMARY KEY NOT NULL, 'val' TEXT NOT NULL);", m.table)
	_, err := g_sqliteDb.Exec(sql)
	fmt.Println("myd:", sql, err)
	return err
}

func (m *SqliteKV) Close() error {
	return nil
}

func (m *SqliteKV) Set(key, value string) error {
	sql := fmt.Sprintf(`INSERT INTO %s(key,val)VALUES('%s','%s');`, m.table, key, value)
	_, err := g_sqliteDb.Exec(sql)
	fmt.Println("myd:", sql, err)
	return err
}

func (m *SqliteKV) Replace(key, value string) error {
	sql := fmt.Sprintf("UPDATE %s SET val='%s' where key='%s';", m.table, value, key)
	_, err := g_sqliteDb.Exec(sql)
	fmt.Println("myd:", sql, err)
	return err
}

func (m *SqliteKV) Get(key string) (string, bool) {
	sql := fmt.Sprintf("SELECT val FROM %s WHERE key='%s';", m.table, key)
	row, err := g_sqliteDb.Query(sql)
	fmt.Println("myd:", sql, err)
	if nil != err {
		return "", false
	}
	defer row.Close()
	var val string
	for row.Next() {
		err = row.Scan(&val)
		if nil != err {
			return "", false
		} else {
			return val, true
		}
	}
	return val, true
}

func (m *SqliteKV) Delete(key string) error {
	sql := fmt.Sprintf("DELETE FROM %s WHERE key='%s';", m.table, key)
	_, err := g_sqliteDb.Exec(sql)
	fmt.Println("myd:", sql, err)
	return err
}

func (m *SqliteKV) Keys() (keys []string, err error) {
	sql := fmt.Sprintf("SELECT key FROM %s", m.table)
	row, err := g_sqliteDb.Query(sql)
	fmt.Println("myd:", sql, err)
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

func (m *SqliteKV) Clean() error {
	sql := fmt.Sprintf("DROP TABLE %s", m.table)
	_, err := g_sqliteDb.Exec(sql)
	fmt.Println("myd:", sql, err)
	return err
}
