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

package dbutil

import (
	"database/sql"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/xo/dburl"
	"strings"
	"sync"
)

type dbPool struct {
	driver string

	sync.RWMutex
	pool        map[string]*sql.DB
	connections map[string]int
}

func (dp *dbPool) getDBConnCount(dsn string) int {
	dp.RLock()
	defer dp.RUnlock()
	count, ok := dp.connections[dsn]
	if ok {
		return count
	}
	return 0
}

func (dp *dbPool) getOrCreate(dsn string) (*sql.DB, error) {
	dp.Lock()
	defer dp.Unlock()
	db, ok := dp.pool[dsn]
	if ok {
		dp.connections[dsn] = dp.connections[dsn] + 1
		return db, nil
	}
	newDb, err := openDB(dp.driver, dsn)
	if err != nil {
		return nil, err
	}
	conf.Log.Debugf("create new database instance: %v", dsn)
	dp.pool[dsn] = newDb
	dp.connections[dsn] = 1
	return newDb, nil
}

func openDB(driver, dsn string) (*sql.DB, error) {
	return sql.Open(driver, dsn)
}

func (dp *dbPool) closeOneConn(dsn string) error {
	dp.Lock()
	defer dp.Unlock()
	connCount, ok := dp.connections[dsn]
	if !ok {
		return nil
	}
	connCount--
	if connCount > 0 {
		dp.connections[dsn] = connCount
		return nil
	}
	conf.Log.Debugf("drop database instance: %v", dsn)
	db := dp.pool[dsn]
	// remove db instance from map in order to avoid memory leak
	delete(dp.pool, dsn)
	delete(dp.connections, dsn)
	return db.Close()
}

type driverPool struct {
	sync.RWMutex
	pool map[string]*dbPool
}

func (dp *driverPool) getOrCreate(driver string) *dbPool {
	dp.Lock()
	defer dp.Unlock()
	db, ok := dp.pool[driver]
	if ok {
		return db
	}
	newDB := &dbPool{
		driver:      driver,
		pool:        map[string]*sql.DB{},
		connections: map[string]int{},
	}
	dp.pool[driver] = newDB
	return newDB
}

func (dp *driverPool) get(driver string) (*dbPool, bool) {
	dp.RLock()
	defer dp.RUnlock()
	dbPool, ok := dp.pool[driver]
	return dbPool, ok
}

func newDriverPool() *driverPool {
	return &driverPool{
		pool: map[string]*dbPool{},
	}
}

var GlobalPool *driverPool

func init() {
	GlobalPool = newDriverPool()
}

func ParseDBUrl(urlstr string) (string, string, error) {
	u, err := dburl.Parse(urlstr)
	if err != nil {
		return "", "", err
	}
	if strings.ToLower(u.Driver) == "sqlite3" {
		u.Driver = "sqlite"
	}
	return u.Driver, u.DSN, nil
}

func FetchDBToOneNode(driverPool *driverPool, driver, dsn string) (*sql.DB, error) {
	dbPool := driverPool.getOrCreate(driver)
	return dbPool.getOrCreate(dsn)
}

func ReturnDBFromOneNode(driverPool *driverPool, driver, dsn string) error {
	dbPool, ok := driverPool.get(driver)
	if !ok {
		return nil
	}
	return dbPool.closeOneConn(dsn)
}

func getDBConnCount(driverPool *driverPool, driver, dsn string) int {
	dbPool := driverPool.getOrCreate(driver)
	return dbPool.getDBConnCount(dsn)
}
