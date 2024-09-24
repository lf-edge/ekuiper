// Copyright 2024 EMQ Technologies Co., Ltd.
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

package testx

import (
	"context"
	"fmt"
	"time"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	_ "github.com/go-sql-driver/mysql"
)

func SetupEmbeddedMysqlServer(address string, port int) (*server.Server, error) {
	pro := createTestDatabase()
	engine := sqle.NewDefault(pro)
	config := server.Config{
		Protocol: "tcp",
		Address:  fmt.Sprintf("%s:%d", address, port),
	}
	s, err := server.NewServer(config, engine, memory.NewSessionBuilder(pro), nil)
	if err != nil {
		return nil, err
	}
	go func() {
		s.Start()
	}()
	// wait server Start
	time.Sleep(500 * time.Millisecond)
	return s, nil
}

func createTestDatabase() *memory.DbProvider {
	tableName := "t"
	db := memory.NewDatabase("test")
	db.BaseDatabase.EnablePrimaryKeyIndexes()

	pro := memory.NewDBProvider(db)
	session := memory.NewSession(sql.NewBaseSession(), pro)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))

	table := memory.NewTable(db, tableName, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Type: types.Int64, Nullable: false, Source: tableName},
		{Name: "b", Type: types.Int64, Nullable: false, Source: tableName},
	}), db.GetForeignKeyCollection())
	db.AddTable(tableName, table)
	err := table.Insert(ctx, sql.NewRow(int64(1), int64(1)))
	if err != nil {
		panic(err)
	}
	return pro
}
