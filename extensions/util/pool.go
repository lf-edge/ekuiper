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
	"sync"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/xo/dburl"
)

var GlobalPool *dbPool

func init() {
	// GlobalPool maintained the *sql.DB group by the driver and DSN.
	// Multiple sql sources/sinks can directly fetch the `*sql.DB` from the GlobalPool and return it back when they don't need it anymore.
	// As multiple sql sources/sinks share the same `*sql.DB`, we can directly control the total count of connections by using `SetMaxOpenConns`
	GlobalPool = newDBPool()
}

type dbPool struct {
	isTesting bool

	sync.RWMutex
	// url -> *sql.DB
	pool map[string]*sql.DB
	// url -> connection count
	connections map[string]int
}

func newDBPool() *dbPool {
	return &dbPool{
		pool:        map[string]*sql.DB{},
		connections: map[string]int{},
	}
}

func (dp *dbPool) getDBConnCount(url string) int {
	dp.RLock()
	defer dp.RUnlock()
	count, ok := dp.connections[url]
	if ok {
		return count
	}
	return 0
}

func (dp *dbPool) getOrCreate(url string) (*sql.DB, error) {
	dp.Lock()
	defer dp.Unlock()
	db, ok := dp.pool[url]
	if ok {
		dp.connections[url] = dp.connections[url] + 1
		return db, nil
	}
	newDb, err := openDB(url, dp.isTesting)
	if err != nil {
		return nil, err
	}
	conf.Log.Debugf("create new database instance: %v", url)
	dp.pool[url] = newDb
	dp.connections[url] = 1
	return newDb, nil
}

func openDB(url string, isTesting bool) (*sql.DB, error) {
	if isTesting {
		return nil, nil
	}
	if strings.HasPrefix(strings.ToLower(url), "dm://") {
		return openDMDB(url)
	}
	driver, dsn, err := ParseDBUrl(url)
	if err != nil {
		return nil, err
	}
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

func openDMDB(url string) (*sql.DB, error) {
	db, err := sql.Open("dm", url)
	if err != nil {
		return nil, err
	}
	c := conf.Config
	if c != nil && c.Basic.SQLConf != nil && c.Basic.SQLConf.MaxConnections > 0 {
		db.SetMaxOpenConns(c.Basic.SQLConf.MaxConnections)
	}
	return db, nil
}

func (dp *dbPool) closeOneConn(url string) error {
	dp.Lock()
	defer dp.Unlock()
	connCount, ok := dp.connections[url]
	if !ok {
		return nil
	}
	connCount--
	if connCount > 0 {
		dp.connections[url] = connCount
		return nil
	}
	conf.Log.Debugf("drop database instance: %v", url)
	db := dp.pool[url]
	// remove db instance from map in order to avoid memory leak
	delete(dp.pool, url)
	delete(dp.connections, url)
	if dp.isTesting {
		return nil
	}
	return db.Close()
}

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

func FetchDBToOneNode(pool *dbPool, url string) (*sql.DB, error) {
	return pool.getOrCreate(url)
}

func ReturnDBFromOneNode(pool *dbPool, url string) error {
	return pool.closeOneConn(url)
}

func getDBConnCount(pool *dbPool, url string) int {
	return pool.getDBConnCount(url)
}

func ParseDriver(url string) (string, error) {
	if strings.HasPrefix(strings.ToLower(url), "dm://") {
		return "dm", nil
	}
	u, err := dburl.Parse(url)
	if err != nil {
		return "", err
	}
	return u.Driver, nil
}
