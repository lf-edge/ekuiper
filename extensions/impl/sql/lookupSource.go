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

package sql

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/pingcap/failpoint"

	client2 "github.com/lf-edge/ekuiper/v2/extensions/impl/sql/client"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
)

// lookupConnectTimeout bounds how long lookup table creation waits for the
// pooled connection to become ready. The pooled connection keeps retrying in
// the background, so a timeout here only fails fast and releases the lookup
// lock instead of hanging table creation forever.
const lookupConnectTimeout = 10 * time.Second

type SqlLookupSource struct {
	conf          *SQLConf
	conn          *client2.SQLConnection
	props         map[string]any
	driver        string
	table         string
	needReconnect bool
	gen           sqlQueryGen
	conId         string
	refId         string
}

func (s *SqlLookupSource) Ping(ctx api.StreamContext, m map[string]any) error {
	cli := &client2.SQLConnection{}
	err := cli.Provision(ctx, "test", m)
	if err != nil {
		return err
	}
	defer cli.Close(ctx)
	return cli.Ping(ctx)
}

func (s *SqlLookupSource) Provision(ctx api.StreamContext, configs map[string]any) error {
	cfg := &SQLConf{}
	err := cast.MapToStruct(configs, cfg)
	failpoint.Inject("MapToStructErr", func() {
		err = errors.New("MapToStruct")
	})
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", configs, err)
	}
	props, err := cfg.resolveDBURL(configs)
	if err != nil {
		return err
	}
	s.conf = cfg
	s.driver, err = client2.ParseDriver(s.conf.DBUrl)
	if err != nil {
		return err
	}
	s.table = cfg.Datasource
	s.props = props
	s.gen = s.buildGen()
	return nil
}

func (s *SqlLookupSource) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing sql source connector url:%v", s.conf.DBUrl)
	if s.conn != nil {
		s.conn.DetachSub(ctx, s.props)
	}
	return connection.DetachConnectionByRef(ctx, s.conId, s.refId)
}

func (s *SqlLookupSource) Connect(ctx api.StreamContext, sc api.StatusChangeHandler) error {
	ctx.GetLogger().Infof("Connecting to sql server")
	var cli *client2.SQLConnection
	var err error
	id := s.conf.DBUrl
	cw, err := connection.FetchConnection(ctx, id, "sql", s.props, sc)
	if err != nil {
		return err
	}
	s.conId = cw.ID
	s.refId = id
	waitCtx, cancel := ctx.WithCancel()
	timer := time.AfterFunc(lookupConnectTimeout, cancel)
	defer func() {
		timer.Stop()
		cancel()
	}()
	conn, err := cw.Wait(waitCtx)
	if err != nil || conn == nil {
		_ = connection.DetachConnectionByRef(ctx, cw.ID, id)
		return fmt.Errorf("sql client not ready: %v", err)
	}
	cli = conn.(*client2.SQLConnection)
	s.conn = cli
	return nil
}

func (s *SqlLookupSource) Lookup(ctx api.StreamContext, fields []string, keys []string, values []any) ([]map[string]any, error) {
	if s.needReconnect {
		err := s.conn.Reconnect()
		if err != nil {
			conf.Log.Errorf("reconnect db error %v", err)
			return nil, err
		}
	}
	var query string
	var args []any
	if s.conf.TemplateSqlQueryCfg == nil {
		var err error
		query, args, err = s.gen.buildQuery(fields, keys, values)
		if err != nil {
			return nil, err
		}
	} else {
		mapValue := make(map[string]any)
		for index, key := range keys {
			mapValue[key] = values[index]
		}
		sqlQuery, err := ctx.ParseTemplate(s.conf.TemplateSqlQueryCfg.TemplateSql, mapValue)
		if err != nil {
			return nil, err
		}
		query = sqlQuery
	}
	ctx.GetLogger().Debugf("Query is %s with args %v", query, args)
	rows, err := s.conn.GetDB().Query(query, args...)
	failpoint.Inject("dbErr", func() {
		err = errors.New("dbErr")
	})
	if err != nil {
		s.needReconnect = true
		ctx.GetLogger().Errorf("sql look table failed, err:%v, query: %v, args: %v", err, query, args)
		return nil, err
	} else {
		s.needReconnect = false
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	dataList := make([]map[string]any, 0)
	for rows.Next() {
		data := make(map[string]any)
		columns := make([]interface{}, len(cols))
		prepareValues(ctx, columns, types, cols)

		err := rows.Scan(columns...)
		if err != nil {
			return nil, err
		}
		scanIntoMap(data, columns, cols, nil)
		dataList = append(dataList, data)
	}
	return dataList, nil
}

type sqlQueryGen interface {
	buildQuery(fields []string, keys []string, values []interface{}) (string, []any, error)
}

// paramSQLGen builds parameterized lookup queries. Values are passed as
// query arguments so the database driver formats time.Time, strings, etc.
// correctly instead of string-concatenating them into the SQL text.
// placeholder renders the bind variable for the i-th (1-based) argument,
// e.g. "?" for mysql/sqlite, "$1" for postgres, "@p1" for sqlserver.
// quoteID quotes WHERE-clause identifiers; it preserves the historical
// quoting style per dialect to avoid breaking existing rules. strictKeys
// enables allowlist validation of keys for dialects without a safe
// identifier-quoting mechanism (see bareQuoteID).
type paramSQLGen struct {
	table       string
	quoteID     func(string) string
	placeholder func(i int) string
	strictKeys  bool
}

func (g paramSQLGen) buildQuery(fields []string, keys []string, values []interface{}) (string, []any, error) {
	// The SELECT list was always interpolated raw, so exotic field names
	// never worked; validate them for a clear error instead of a DB syntax
	// error. Table is operator-configured and trusted.
	for _, f := range fields {
		if !isSafeDynamicFieldName(f) {
			return "", nil, fmt.Errorf("invalid lookup field name %q: expected [A-Za-z_][A-Za-z0-9_]*", f)
		}
	}
	if len(keys) == 0 {
		return "", nil, fmt.Errorf("lookup keys must not be empty")
	}
	for _, k := range keys {
		if k == "" {
			return "", nil, fmt.Errorf("lookup key must not be empty")
		}
	}
	if g.strictKeys {
		for _, k := range keys {
			if !isSafeDynamicFieldName(k) {
				return "", nil, fmt.Errorf("invalid lookup key name %q: expected [A-Za-z_][A-Za-z0-9_]*", k)
			}
		}
	}
	query := "SELECT "
	if len(fields) == 0 {
		query += "*"
	} else {
		for i, f := range fields {
			if i > 0 {
				query += ","
			}
			query += f
		}
	}
	query += fmt.Sprintf(" FROM %s WHERE ", g.table)
	args := make([]any, 0, len(keys))
	for i, k := range keys {
		if i > 0 {
			query += " AND "
		}
		if values[i] == nil {
			query += fmt.Sprintf("%s IS NULL", g.quoteID(k))
			continue
		}
		query += fmt.Sprintf("%s = %s", g.quoteID(k), g.placeholder(len(args)+1))
		args = append(args, values[i])
	}
	return query, args, nil
}

// backtickQuoteID quotes for mysql/sqlite-style dialects. An embedded
// backtick is escaped by doubling, so any parser-derived name (including
// eKuiper backtick-quoted identifiers like `device-id`) is safe to inline.
func backtickQuoteID(k string) string {
	return "`" + strings.ReplaceAll(k, "`", "``") + "`"
}

// bareQuoteID emits the identifier unquoted, preserving the historical style
// for postgres/sqlserver/oracle-style dialects. There is no escaping
// mechanism for a bare identifier, so keys using this quoter must pass the
// allowlist check (strictKeys) instead.
func bareQuoteID(k string) string { return k }

func questionPlaceholder(_ int) string { return "?" }
func dollarPlaceholder(i int) string   { return fmt.Sprintf("$%d", i) }
func atPPlaceholder(i int) string      { return fmt.Sprintf("@p%d", i) }

// colonPlaceholder emits :1/:2 positional binds for go-ora/godror. Verified
// against driver docs only; confirm with a real Oracle instance if possible,
// as these drivers also accept named binds and behavior may vary by version.
func colonPlaceholder(i int) string { return fmt.Sprintf(":%d", i) }

func (s *SqlLookupSource) buildGen() sqlQueryGen {
	switch strings.ToLower(s.driver) {
	case "postgres", "postgresql", "pgx":
		return paramSQLGen{table: s.table, quoteID: bareQuoteID, placeholder: dollarPlaceholder, strictKeys: true}
	case "sqlserver", "mssql":
		return paramSQLGen{table: s.table, quoteID: bareQuoteID, placeholder: atPPlaceholder, strictKeys: true}
	case "oracle", "godror", "ora", "go-ora":
		return paramSQLGen{table: s.table, quoteID: bareQuoteID, placeholder: colonPlaceholder, strictKeys: true}
	case "mysql", "mymysql", "sqlite", "sqlite3":
		return paramSQLGen{table: s.table, quoteID: backtickQuoteID, placeholder: questionPlaceholder}
	default:
		// Unknown drivers fall back to bare identifiers with "?" placeholders:
		// backticks are rejected by most dialects, while bare identifiers and
		// "?" are accepted by the majority (clickhouse, snowflake, presto, ...).
		conf.Log.Warnf("unknown sql driver %q for lookup source, falling back to \"?\" placeholders", s.driver)
		return paramSQLGen{table: s.table, quoteID: bareQuoteID, placeholder: questionPlaceholder, strictKeys: true}
	}
}

func GetLookupSource() api.Source {
	return &SqlLookupSource{}
}

var (
	_ api.LookupSource  = &SqlLookupSource{}
	_ util.PingableConn = &SqlLookupSource{}
)
