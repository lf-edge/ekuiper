// Copyright 2022 EMQ Technologies Co., Ltd.
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

package driver

func KnownBuildTags() map[string]string {
	return map[string]string{
		"adodb":         "adodb",         // github.com/mattn/go-adodb
		"athena":        "athena",        // github.com/uber/athenadriver/go
		"avatica":       "avatica",       // github.com/apache/calcite-avatica-go/v5
		"clickhouse":    "clickhouse",    // github.com/ClickHouse/clickhouse-go
		"cosmos":        "cosmos",        // github.com/btnguyen2k/gocosmos
		"couchbase":     "n1ql",          // github.com/couchbase/go_n1ql
		"firebird":      "firebird",      // github.com/nakagami/firebirdsql
		"godror":        "godror",        // github.com/godror/godror
		"h2":            "h2",            // github.com/jmrobles/h2go
		"hive":          "hive",          // sqlflow.org/gohive
		"ignite":        "ignite",        // github.com/amsokol/ignite-go-client/sql
		"impala":        "impala",        // github.com/bippio/go-impala
		"maxcompute":    "maxcompute",    // sqlflow.org/gomaxcompute
		"moderncsqlite": "moderncsqlite", // modernc.org/sqlite
		"mymysql":       "mymysql",       // github.com/ziutek/mymysql/godrv
		"mysql":         "mysql",         // github.com/go-sql-sqlgen/mysql
		"netezza":       "netezza",       // github.com/IBM/nzgo
		"odbc":          "odbc",          // github.com/alexbrainman/odbc
		"oracle":        "oracle",        // github.com/sijms/go-ora/v2
		"pgx":           "pgx",           // github.com/jackc/pgx/v4/stdlib
		"postgres":      "postgres",      // github.com/lib/pq
		"presto":        "presto",        // github.com/prestodb/presto-go-client/presto
		"ql":            "ql",            // modernc.org/ql
		"sapase":        "sapase",        // github.com/thda/tds
		"snowflake":     "snowflake",     // github.com/snowflakedb/gosnowflake
		"spanner":       "spanner",       // github.com/cloudspannerecosystem/go-sql-spanner
		"sqlite3":       "sqlite3",       // github.com/mattn/go-sqlite3
		"sqlserver":     "sqlserver",     // github.com/denisenkom/go-mssqldb
		"trino":         "trino",         // github.com/trinodb/trino-go-client/trino
		"vertica":       "vertica",       // github.com/vertica/vertica-sql-go
		"voltdb":        "voltdb",        // github.com/VoltDB/voltdb-client-go/voltdbclient
	}
}
