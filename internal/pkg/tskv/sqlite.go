package tskv

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	_ "github.com/mattn/go-sqlite3"
	"path"
	"sync"
)

// All TSKV instances share ONE database with different tables
var (
	db   *sql.DB
	once sync.Once
)

// SqliteTskv All TSKV instances share the same database but with different tables
// Each table must have ONLY ONE instance
type SqliteTskv struct {
	table string
	// only append key bigger than the latest key inside; ONLY check in the instance itself
	last int64
}

func init() {
	gob.Register(make(map[string]interface{}))
}

func NewSqlite(table string) (*SqliteTskv, error) {
	var outerError error
	once.Do(func() {
		d, err := conf.GetDataLoc()
		if err != nil {
			outerError = err
			return
		}
		db, outerError = sql.Open("sqlite3", path.Join(d, "tskv.db"))
	})
	if outerError != nil {
		return nil, outerError
	}
	if db == nil {
		return nil, fmt.Errorf("cannot initiate sqlite db, please restart")
	}
	sqlStr := fmt.Sprintf("CREATE TABLE IF NOT EXISTS '%s'('key' INTEGER PRIMARY KEY, 'val' BLOB);", table)
	_, outerError = db.Exec(sqlStr)
	if outerError != nil {
		return nil, fmt.Errorf("cannot create table: %v", outerError)
	}
	return &SqliteTskv{
		table: table,
		last:  last(table),
	}, nil
}

func (m *SqliteTskv) Set(key int64, value interface{}) (bool, error) {
	if key > m.last {
		b, err := m.encode(value)
		if err != nil {
			return false, err
		}
		sqlStr := fmt.Sprintf("INSERT INTO %s(key,val) values(?,?);", m.table)
		stmt, err := db.Prepare(sqlStr)
		if err != nil {
			return false, err
		}
		defer stmt.Close()
		_, err = stmt.Exec(key, b)
		if err != nil {
			return false, err
		} else {
			m.last = key
			return true, nil
		}
	} else {
		return false, nil
	}
}

func (m *SqliteTskv) Get(key int64, value interface{}) (bool, error) {
	sqlStr := fmt.Sprintf("SELECT val FROM %s WHERE key=%d;", m.table, key)
	row := db.QueryRow(sqlStr)
	var tmp []byte
	switch err := row.Scan(&tmp); err {
	case sql.ErrNoRows:
		return false, nil
	case nil:
		// do nothing, continue processing
	default:
		return false, err
	}

	dec := gob.NewDecoder(bytes.NewBuffer(tmp))
	if err := dec.Decode(value); err != nil {
		return false, err
	}
	return true, nil
}

func (m *SqliteTskv) Last(value interface{}) (int64, error) {
	_, err := m.Get(m.last, value)
	if err != nil {
		return 0, err
	}
	return m.last, nil
}

func (m *SqliteTskv) Delete(k int64) error {
	sqlStr := fmt.Sprintf("DELETE FROM %s WHERE key=%d;", m.table, k)
	_, err := db.Exec(sqlStr)
	return err
}

func (m *SqliteTskv) DeleteBefore(k int64) error {
	sqlStr := fmt.Sprintf("DELETE FROM %s WHERE key<%d;", m.table, k)
	_, err := db.Exec(sqlStr)
	return err
}

func (m *SqliteTskv) Close() error {
	return nil
}

func (m *SqliteTskv) Drop() error {
	sqlStr := fmt.Sprintf("Drop table %s;", m.table)
	_, err := db.Exec(sqlStr)
	return err
}

func (m *SqliteTskv) encode(value interface{}) ([]byte, error) {
	var buf bytes.Buffer
	gob.Register(value)
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(value); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func last(table string) int64 {
	sqlStr := fmt.Sprintf("SELECT key FROM %s Order by key DESC Limit 1;", table)
	row := db.QueryRow(sqlStr)
	var tmp int64
	switch err := row.Scan(&tmp); err {
	case sql.ErrNoRows:
		return 0
	case nil:
		return tmp
	default:
		return 0
	}
}
