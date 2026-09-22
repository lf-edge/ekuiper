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

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/pingcap/failpoint"

	client2 "github.com/lf-edge/ekuiper/v2/extensions/impl/sql/client"
	sqldriver "github.com/lf-edge/ekuiper/v2/extensions/impl/sql/sqldatabase/driver"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
)

type SqlLookupSource struct {
	conf   *SQLConf
	props  map[string]any
	driver string
	table  string
	gen    sqlQueryGen
	conId  string
	refId  string

	// cw is the pooled connection this lookup attached to. It is set once
	// by Connect and never mutated afterwards.
	cw *connection.ConnWrapper

	// mu guards the lazy connection state below. It is never held while
	// waiting on cw.Wait or retryReconnect so a slow recovery cannot block
	// Close or concurrent lookups on the state itself.
	mu            syncx.Mutex
	conn          *client2.SQLConnection
	needReconnect bool
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
	if conn, _ := s.getState(); conn != nil {
		conn.DetachSub(ctx, s.props)
	}
	// Always detach with the refId saved by Connect, so a lookup whose
	// pooled connection never became ready still releases its reference.
	return connection.DetachConnectionByRef(ctx, s.conId, s.refId)
}

// Connect only attaches to the pooled connection and returns immediately:
// table creation must not depend on whether the database is currently
// reachable. The pool keeps its initial retry in the background and the
// first Lookup reports (and then recovers from) an unavailable database.
func (s *SqlLookupSource) Connect(ctx api.StreamContext, sc api.StatusChangeHandler) error {
	ctx.GetLogger().Infof("Connecting to sql server")
	// The consumer identity is framework-owned (DESIGN §5.3): it must be
	// injected by lookup.CreateInstance via connection.WithLookupRefID.
	// Never derive it from DBUrl/ctx here.
	refID, ok := connection.LookupRefID(ctx)
	if !ok {
		return fmt.Errorf("lookup ref id missing: Connect must be called with a lookup framework context")
	}
	key, requireExisting, err := sqlConnectionKey(s.props, s.conf.DBUrl)
	if err != nil {
		return err
	}
	cw, err := connection.FetchConnectionWithOptions(ctx, connection.FetchOptions{
		ConnectionKey:   key,
		RefID:           refID,
		RequireExisting: requireExisting,
		Type:            "sql",
		Props:           s.props,
		StatusHandler:   sc,
	})
	if err != nil {
		return err
	}
	s.cw = cw
	s.conId = cw.ID
	s.refId = refID
	return nil
}

// ensureConnection returns a usable SQLConnection for the current request.
//
// Semantics (shared by first-unreachable and runtime disconnect):
//   - conn != nil && !needReconnect: use it directly.
//   - conn == nil and the pooled connection is not ready yet: the first
//     request reports a not-ready error once; needReconnect is set so
//     subsequent requests wait for the pool's initial retry instead.
//   - conn == nil && needReconnect: wait for the pool's initial connection.
//   - conn != nil && needReconnect: retry reconnecting until the database
//     is back or the rule context is canceled.
//
// Waiting is always bound to ctx: a rule stop cancels it and unblocks the
// current Lookup.
func (s *SqlLookupSource) ensureConnection(
	ctx api.StreamContext,
) (*client2.SQLConnection, error) {
	conn, needReconnect := s.getState()

	if conn == nil {
		if !needReconnect && !s.cw.IsInitialized() {
			s.setNeedReconnect(true)
			return nil, errorx.NewIOErr("sql client not ready")
		}

		c, err := s.cw.Wait(ctx)

		// Do not depend on Wait returning ctx.Err() itself.
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil {
			return nil, err
		}
		if c == nil {
			return nil, errorx.NewIOErr("sql client not ready")
		}

		sqlConn := c.(*client2.SQLConnection)
		s.setConnection(sqlConn)

		return sqlConn, nil
	}

	if needReconnect {
		if err := retryReconnect(ctx, conn); err != nil {
			return nil, err
		}
	}

	return conn, nil
}

func (s *SqlLookupSource) getState() (conn *client2.SQLConnection, needReconnect bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn, s.needReconnect
}

func (s *SqlLookupSource) setConnection(conn *client2.SQLConnection) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conn = conn
	s.needReconnect = false
}

func (s *SqlLookupSource) setNeedReconnect(needReconnect bool) {
	s.mu.Lock()
	s.needReconnect = needReconnect
	s.mu.Unlock()
}

func (s *SqlLookupSource) Lookup(ctx api.StreamContext, fields []string, keys []string, values []any) ([]map[string]any, error) {
	conn, err := s.ensureConnection(ctx)
	if err != nil {
		conf.Log.Errorf("sql lookup connect err %v", err)
		return nil, err
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
	rows, err := conn.GetDB().QueryContext(ctx, query, args...)
	failpoint.Inject("dbErr", func() {
		err = errors.New("dbErr")
	})
	if err != nil {
		s.setNeedReconnect(true)
		ctx.GetLogger().Errorf("sql look table failed, err:%v, query: %v, args: %v", err, query, args)
		return nil, err
	} else {
		s.setNeedReconnect(false)
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
	// transformArg converts values before they are appended as arguments,
	// resolved from the driver layer in buildGen. Nil means passthrough.
	transformArg func(any) any
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
			// A nil lookup key used to produce broken SQL; it must fail loud
			// instead of silently matching rows via IS NULL on a possibly
			// non-unique key. (The lookup node already skips nil keys before
			// calling, so this guards direct API use.)
			return "", nil, fmt.Errorf("lookup key %q must not be nil", k)
		}
		query += fmt.Sprintf("%s = %s", g.quoteID(k), g.placeholder(len(args)+1))
		v := values[i]
		if g.transformArg != nil {
			v = g.transformArg(v)
		}
		args = append(args, v)
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
	g := paramSQLGen{
		table:        s.table,
		transformArg: sqldriver.TransformerFor(s.driver),
	}
	switch strings.ToLower(s.driver) {
	case "postgres", "postgresql", "pgx":
		g.quoteID, g.placeholder, g.strictKeys = bareQuoteID, dollarPlaceholder, true
	case "sqlserver", "mssql":
		g.quoteID, g.placeholder, g.strictKeys = bareQuoteID, atPPlaceholder, true
	case "oracle", "godror", "ora", "go-ora":
		g.quoteID, g.placeholder, g.strictKeys = bareQuoteID, colonPlaceholder, true
	case "mysql", "mymysql", "sqlite", "sqlite3":
		g.quoteID, g.placeholder = backtickQuoteID, questionPlaceholder
	default:
		// Unknown drivers fall back to bare identifiers with "?" placeholders:
		// backticks are rejected by most dialects, while bare identifiers and
		// "?" are accepted by the majority (clickhouse, snowflake, presto, ...).
		conf.Log.Warnf("unknown sql driver %q for lookup source, falling back to \"?\" placeholders", s.driver)
		g.quoteID, g.placeholder, g.strictKeys = bareQuoteID, questionPlaceholder, true
	}
	return g
}

func GetLookupSource() api.Source {
	return &SqlLookupSource{}
}

var (
	_ api.LookupSource  = &SqlLookupSource{}
	_ util.PingableConn = &SqlLookupSource{}
)
