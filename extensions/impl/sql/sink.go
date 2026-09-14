// Copyright 2024-2026 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/v2/extensions/impl/sql/client"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/metrics"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

const (
	LblInsert = "insert"
	LblUpdate = "update"
	LblDel    = "del"
)

type SQLSinkConnector struct {
	config        *sqlSinkConfig
	cw            *connection.ConnWrapper
	conn          *client.SQLConnection
	props         map[string]any
	needReconnect bool
	// bindNext renders the bind variable for the i-th (1-based) argument of
	// the current statement, resolved from the driver in Provision.
	// bindInline selects legacy inline literals for drivers that cannot
	// bind (see sqlSinkBinder).
	bindNext   func(i int) string
	bindInline bool
}

type sqlSinkConfig struct {
	*SQLConf
	Table        string   `json:"table"`
	Fields       []string `json:"fields"`
	RowKindField string   `json:"rowKindField"`
	KeyField     string   `json:"keyField"`
}

func (c *sqlSinkConfig) buildInsertRow(ctx api.StreamContext, b *sqlSinkBinder, mapData map[string]interface{}, keys []string) (string, error) {
	if len(mapData) == 0 {
		return "", fmt.Errorf("data is empty")
	}
	parts := make([]string, 0, len(keys))
	logger := ctx.GetLogger()
	for _, k := range keys {
		v, ok := mapData[k]
		if ok && v != nil {
			parts = append(parts, b.bind(v))
		} else {
			logger.Warn("not found field:", k)
			parts = append(parts, `NULL`)
		}
	}
	return "(" + strings.Join(parts, ",") + ")", nil
}

// sqlSinkBinder numbers bind variables within a single statement and
// collects the matching arguments. Values stay Go values; the driver formats
// time.Time, strings, etc. instead of string-concatenating them into SQL.
//
// Some drivers cannot bind at all. The pinned MaxCompute driver mangles any
// supplied args (its Exec path runs fmt.Sprintf over the query, and Prepare
// panics), so for those dialects the binder works in inline mode: bind
// renders the historical literal and appends nothing, yielding a complete
// SQL string executed with zero args exactly like before.
type sqlSinkBinder struct {
	next   func(i int) string
	inline bool
	n      int
	args   []any
}

func (b *sqlSinkBinder) bind(v any) string {
	if b.inline {
		if s, ok := v.(string); ok {
			return "'" + strings.ReplaceAll(s, "'", "''") + "'"
		}
		return fmt.Sprintf(`%v`, v)
	}
	b.n++
	b.args = append(b.args, v)
	return b.next(b.n)
}

// TODO: converge with the lookup parameterized-query helpers; the
// placeholder styles are intentionally identical.
func qmarkBind(_ int) string  { return "?" }
func dollarBind(i int) string { return fmt.Sprintf("$%d", i) }
func atPBind(i int) string    { return fmt.Sprintf("@p%d", i) }
func colonBind(i int) string  { return fmt.Sprintf(":%d", i) }

// sinkDialect resolves the placeholder style and binding capability for a
// driver name as reported by dburl. Drivers fall back to "?" binding; only
// drivers proven to mangle bound args (MaxCompute) use inline literals.
func sinkDialect(ctx api.StreamContext, driver string) (next func(i int) string, inline bool) {
	switch strings.ToLower(driver) {
	case "postgres", "postgresql", "pgx":
		return dollarBind, false
	case "sqlserver", "mssql":
		return atPBind, false
	case "oracle", "godror", "ora", "go-ora":
		return colonBind, false
	case "ql", "cznic", "cznicql":
		// QL parameters require a numeric suffix ($1 or ?1); bare ? is invalid.
		return dollarBind, false
	case "maxcompute", "mc":
		return qmarkBind, true
	case "mysql", "mymysql", "sqlite", "sqlite3":
		return qmarkBind, false
	default:
		ctx.GetLogger().Warnf("unknown sql driver %q for sink, falling back to \"?\" placeholders", driver)
		return qmarkBind, false
	}
}

func sinkBinderForDriver(ctx api.StreamContext, driver string) func(i int) string {
	next, _ := sinkDialect(ctx, driver)
	return next
}

// isSafeDynamicFieldName recognizes dynamic message keys that cannot alter SQL syntax.
// Explicitly configured identifiers are trusted SQL configuration and retain their
// existing database-specific syntax.
func isSafeDynamicFieldName(identifier string) bool {
	if len(identifier) == 0 || !isIdentifierStart(identifier[0]) {
		return false
	}
	for i := 1; i < len(identifier); i++ {
		if !isIdentifierPart(identifier[i]) {
			return false
		}
	}
	return true
}

func isIdentifierStart(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func isIdentifierPart(c byte) bool {
	return isIdentifierStart(c) || c >= '0' && c <= '9'
}

func (s *SQLSinkConnector) Ping(ctx api.StreamContext, props map[string]any) error {
	cli := &client.SQLConnection{}
	err := cli.Provision(ctx, "test", props)
	if err != nil {
		return err
	}
	defer cli.Close(ctx)
	return cli.Ping(ctx)
}

func (s *SQLSinkConnector) Provision(ctx api.StreamContext, configs map[string]any) error {
	sc := &SQLConf{}
	err := cast.MapToStruct(configs, sc)
	if err != nil {
		return err
	}
	c := &sqlSinkConfig{SQLConf: sc}
	err = cast.MapToStruct(configs, c)
	if err != nil {
		return err
	}
	configs, err = sc.resolveDBURL(configs)
	if err != nil {
		return err
	}
	if c.Table == "" {
		return fmt.Errorf("property table is required")
	}
	if c.RowKindField != "" && c.KeyField == "" {
		return fmt.Errorf("keyField is required when rowKindField is set")
	}
	driver, err := client.ParseDriver(sc.DBUrl)
	if err != nil {
		return err
	}
	s.config = c
	s.props = configs
	s.bindNext, s.bindInline = sinkDialect(ctx, driver)
	return nil
}

// Consume This is run after provision. Discard common confs that will only be handled in sink itself
func (s *SQLSinkConnector) Consume(props map[string]any) {
	delete(props, "fields")
}

func (s *SQLSinkConnector) Connect(ctx api.StreamContext, sc api.StatusChangeHandler) error {
	ctx.GetLogger().Infof("Connecting to sql server")
	var err error
	id := s.config.DBUrl
	cw, err := connection.FetchConnection(ctx, id, "sql", s.props, sc)
	if err != nil {
		return err
	}
	s.cw = cw
	conn, err := s.cw.Wait(ctx)
	if conn == nil {
		return fmt.Errorf("sql client not ready: %v", err)
	}
	s.conn = conn.(*client.SQLConnection)
	return err
}

func (s *SQLSinkConnector) Close(ctx api.StreamContext) error {
	if s.config != nil {
		ctx.GetLogger().Infof("Closing sql sink connector url:%v", s.config.DBUrl)
	}
	if s.cw != nil {
		return connection.DetachConnection(ctx, s.cw.ID)
	}
	return nil
}

func (s *SQLSinkConnector) Collect(ctx api.StreamContext, item api.MessageTuple) (err error) {
	defer func() {
		if err != nil {
			metrics.IOCounter.WithLabelValues(LblSql, metrics.LblSinkIO, LblException, ctx.GetRuleId(), ctx.GetOpId()).Inc()
		}
	}()
	metrics.IOCounter.WithLabelValues(LblSql, metrics.LblSinkIO, LblReq, ctx.GetRuleId(), ctx.GetOpId()).Inc()
	return s.collect(ctx, item.ToMap())
}

func (s *SQLSinkConnector) collect(ctx api.StreamContext, item map[string]any) error {
	if len(s.config.RowKindField) < 1 {
		keys, err := s.extractKeys(item)
		if err != nil {
			return err
		}
		b := &sqlSinkBinder{next: s.bindNext, inline: s.bindInline}
		row, err := s.config.buildInsertRow(ctx, b, item, keys)
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			sqlStr := buildInsertSQL(s.config.Table, keys, []string{row})
			return s.writeToDB(ctx, sqlStr, b.args...)
		}
		return nil
	}
	return s.save(ctx, s.config.Table, item)
}

func (s *SQLSinkConnector) CollectList(ctx api.StreamContext, items api.MessageTupleList) (err error) {
	defer func() {
		if err != nil {
			metrics.IOCounter.WithLabelValues(LblSql, metrics.LblSinkIO, LblReq, ctx.GetRuleId(), ctx.GetOpId()).Inc()
		}
	}()
	metrics.IOCounter.WithLabelValues(LblSql, metrics.LblSinkIO, LblReq, ctx.GetRuleId(), ctx.GetOpId()).Inc()
	return s.collectList(ctx, items.ToMaps())
}

func (s *SQLSinkConnector) collectList(ctx api.StreamContext, items []map[string]any) error {
	if len(items) < 1 {
		return nil
	}
	keys, err := s.extractKeys(items[0])
	if err != nil {
		return err
	}
	b := &sqlSinkBinder{next: s.bindNext, inline: s.bindInline}
	var values []string
	if len(s.config.RowKindField) < 1 {
		for _, mapData := range items {
			row, err := s.config.buildInsertRow(ctx, b, mapData, keys)
			if err != nil {
				return err
			}
			values = append(values, row)
		}
		if len(keys) > 0 {
			sqlStr := buildInsertSQL(s.config.Table, keys, values)
			return s.writeToDB(ctx, sqlStr, b.args...)
		}
		return nil
	}
	for _, el := range items {
		err := s.save(ctx, s.config.Table, el)
		if err != nil {
			ctx.GetLogger().Error(err)
		}
	}
	return nil
}

// save save updatable data only to db
func (s *SQLSinkConnector) save(ctx api.StreamContext, table string, data map[string]interface{}) error {
	rowkind := ast.RowkindInsert
	c, ok := data[s.config.RowKindField]
	if ok {
		rowkind, ok = c.(string)
		if !ok {
			return fmt.Errorf("rowkind field %s is not a string in data %v", s.config.RowKindField, data)
		}
		if rowkind != ast.RowkindInsert && rowkind != ast.RowkindUpdate && rowkind != ast.RowkindDelete {
			return fmt.Errorf("invalid rowkind %s", rowkind)
		}
	}
	keys, err := s.extractKeys(data)
	if err != nil {
		return err
	}
	var sqlStr string
	var args []any
	b := &sqlSinkBinder{next: s.bindNext, inline: s.bindInline}
	switch rowkind {
	case ast.RowkindInsert:
		row, err := s.config.buildInsertRow(ctx, b, data, keys)
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			sqlStr = buildInsertSQL(table, keys, []string{row})
			args = b.args
		}
	case ast.RowkindUpdate:
		keyval, ok := data[s.config.KeyField]
		if !ok {
			return fmt.Errorf("field %s does not exist in data %v", s.config.KeyField, data)
		}
		sqlStr = buildUpdateSQL(table, keys, b, data, s.config.KeyField, keyval)
		args = b.args
	case ast.RowkindDelete:
		keyval, ok := data[s.config.KeyField]
		if !ok {
			return fmt.Errorf("field %s does not exist in data %v", s.config.KeyField, data)
		}
		sqlStr = buildDeleteSQL(table, s.config.KeyField, keyval, b)
		args = b.args
	default:
		return fmt.Errorf("invalid rowkind %s", rowkind)
	}
	return s.writeToDB(ctx, sqlStr, args...)
}

func (s *SQLSinkConnector) writeToDB(ctx api.StreamContext, sqlStr string, args ...any) error {
	ctx.GetLogger().Debugf("%s with args %v", sqlStr, args)
	if s.needReconnect {
		metrics.IOCounter.WithLabelValues(LblSql, metrics.LblSinkIO, LblReconn, ctx.GetRuleId(), ctx.GetOpId()).Inc()
		err := s.conn.Reconnect()
		if err != nil {
			return errorx.NewIOErr(err.Error())
		}
	}
	start := time.Now()
	r, err := s.conn.GetDB().Exec(sqlStr, args...)
	failpoint.Inject("dbErr", func() {
		err = errors.New("dbErr")
	})
	if err != nil {
		s.needReconnect = true
		return errorx.NewIOErr(err.Error())
	}
	metrics.IODurationHist.WithLabelValues(LblSql, metrics.LblSinkIO, ctx.GetRuleId(), ctx.GetOpId()).Observe(float64(time.Since(start).Microseconds()))
	s.needReconnect = false
	d, err := r.RowsAffected()
	if err != nil {
		ctx.GetLogger().Errorf("get rows affected error: %s", err.Error())
	}
	ctx.GetLogger().Debugf("Rows affected: %d", d)
	return nil
}

func (s *SQLSinkConnector) extractKeys(item map[string]any) ([]string, error) {
	if len(s.config.Fields) > 0 {
		return s.config.Fields, nil
	}
	keys := make([]string, 0, len(item))
	for k := range item {
		if !isSafeDynamicFieldName(k) {
			return nil, fmt.Errorf("invalid dynamic field name %q: expected [A-Za-z_][A-Za-z0-9_]*", k)
		}
		keys = append(keys, k)
	}
	return keys, nil
}

func buildInsertSQL(table string, keys []string, values []string) string {
	sql := fmt.Sprintf("INSERT INTO %s (%s) values ", table, strings.Join(keys, ",")) + strings.Join(values, ",") + ";"
	return sql
}

// buildUpdateSQL renders SET pairs with bind variables and appends the key
// argument to b. Missing/nil values keep the historical NULL literal in SET;
// a nil key renders IS NULL in WHERE instead of binding NULL (which matches
// nothing with =).
func buildUpdateSQL(table string, keys []string, b *sqlSinkBinder, data map[string]any, keyField string, keyval any) string {
	sqlStr := fmt.Sprintf("UPDATE %s SET ", table)
	for i, key := range keys {
		if i != 0 {
			sqlStr += ","
		}
		if v, ok := data[key]; ok && v != nil {
			sqlStr += fmt.Sprintf("%s=%s", key, b.bind(v))
		} else {
			sqlStr += fmt.Sprintf("%s=NULL", key)
		}
	}
	if keyval == nil {
		sqlStr += fmt.Sprintf(" WHERE %s IS NULL;", keyField)
	} else {
		sqlStr += fmt.Sprintf(" WHERE %s = %s;", keyField, b.bind(keyval))
	}
	return sqlStr
}

func buildDeleteSQL(table string, keyField string, keyval any, b *sqlSinkBinder) string {
	if keyval == nil {
		return fmt.Sprintf("DELETE FROM %s WHERE %s IS NULL;", table, keyField)
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s = %s;", table, keyField, b.bind(keyval))
}

func GetSink() api.Sink {
	return &SQLSinkConnector{}
}

var (
	_ api.TupleCollector  = &SQLSinkConnector{}
	_ util.PingableConn   = &SQLSinkConnector{}
	_ model.PropsConsumer = &SQLSinkConnector{}
)
