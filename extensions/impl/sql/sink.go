// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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
	"reflect"
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
}

type sqlSinkConfig struct {
	*SQLConf
	Table        string   `json:"table"`
	Fields       []string `json:"fields"`
	RowKindField string   `json:"rowKindField"`
	KeyField     string   `json:"keyField"`
}

func (c *sqlSinkConfig) buildInsertSql(ctx api.StreamContext, mapData map[string]interface{}, keys []string) (string, error) {
	vals, err := c.getValuesByKeys(ctx, mapData, keys)
	if err != nil {
		return "", err
	}
	sqlStr := "(" + strings.Join(vals, ",") + ")"
	return sqlStr, nil
}

func (c *sqlSinkConfig) getValuesByKeys(ctx api.StreamContext, mapData map[string]interface{}, keys []string) ([]string, error) {
	if len(mapData) == 0 {
		return nil, fmt.Errorf("data is empty")
	}
	var vals []string
	logger := ctx.GetLogger()
	for _, k := range keys {
		v, ok := mapData[k]
		if ok && v != nil {
			if reflect.String == reflect.TypeOf(v).Kind() {
				// Escape single quotes by doubling them (SQL standard) to avoid breaking the literal.
				vals = append(vals, quoteSQLString(fmt.Sprint(v)))
			} else {
				vals = append(vals, fmt.Sprintf(`%v`, v))
			}
		} else {
			logger.Warn("not found field:", k)
			vals = append(vals, `NULL`)
		}
	}
	return vals, nil
}

func quoteSQLString(s string) string {
	// SQL string literal escaping: ' -> ''.
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
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
	s.config = c
	s.props = configs
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
	ctx.GetLogger().Infof("Closing sql sink connector url:%v", s.config.DBUrl)
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

func (s *SQLSinkConnector) collect(ctx api.StreamContext, item map[string]any) (err error) {
	if len(s.config.RowKindField) < 1 {
		keys := s.extractKeys(item)
		var values []string = nil
		var vars string
		vars, err = s.config.buildInsertSql(ctx, item, keys)
		if err != nil {
			return err
		}
		values = append(values, vars)
		if keys != nil {
			sqlStr := buildInsertSQL(s.config.Table, keys, values)
			return s.writeToDB(ctx, sqlStr)
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

func (s *SQLSinkConnector) collectList(ctx api.StreamContext, items []map[string]any) (err error) {
	if len(items) < 1 {
		return nil
	}
	keys := s.extractKeys(items[0])
	var values []string = nil
	var vars string
	if len(s.config.RowKindField) < 1 {
		for _, mapData := range items {
			vars, err = s.config.buildInsertSql(ctx, mapData, keys)
			if err != nil {
				return err
			}
			values = append(values, vars)
		}
		if keys != nil {
			sqlStr := buildInsertSQL(s.config.Table, keys, values)
			return s.writeToDB(ctx, sqlStr)
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
	keys := s.extractKeys(data)
	var sqlStr string
	switch rowkind {
	case ast.RowkindInsert:
		vars, err := s.config.buildInsertSql(ctx, data, keys)
		if err != nil {
			return err
		}
		values := []string{vars}
		if keys != nil {
			sqlStr = buildInsertSQL(table, keys, values)
		}
	case ast.RowkindUpdate:
		keyval, ok := data[s.config.KeyField]
		if !ok {
			return fmt.Errorf("field %s does not exist in data %v", s.config.KeyField, data)
		}
		vals, err := s.config.getValuesByKeys(ctx, data, keys)
		if err != nil {
			return err
		}
		sqlStr = fmt.Sprintf("UPDATE %s SET ", table)
		for i, key := range keys {
			if i != 0 {
				sqlStr += ","
			}
			sqlStr += fmt.Sprintf("%s=%s", key, vals[i])
		}
		if ksv, ok := keyval.(string); ok {
			sqlStr += fmt.Sprintf(" WHERE %s = %s;", s.config.KeyField, quoteSQLString(ksv))
		} else {
			sqlStr += fmt.Sprintf(" WHERE %s = %v;", s.config.KeyField, keyval)
		}
	case ast.RowkindDelete:
		keyval, ok := data[s.config.KeyField]
		if !ok {
			return fmt.Errorf("field %s does not exist in data %v", s.config.KeyField, data)
		}
		if ksv, ok := keyval.(string); ok {
			sqlStr = fmt.Sprintf("DELETE FROM %s WHERE %s = %s;", table, s.config.KeyField, quoteSQLString(ksv))
		} else {
			sqlStr = fmt.Sprintf("DELETE FROM %s WHERE %s = %v;", table, s.config.KeyField, keyval)
		}
	default:
		return fmt.Errorf("invalid rowkind %s", rowkind)
	}
	return s.writeToDB(ctx, sqlStr)
}

func (s *SQLSinkConnector) writeToDB(ctx api.StreamContext, sqlStr string) error {
	ctx.GetLogger().Debugf(sqlStr)
	if s.needReconnect {
		metrics.IOCounter.WithLabelValues(LblSql, metrics.LblSinkIO, LblReconn, ctx.GetRuleId(), ctx.GetOpId()).Inc()
		err := s.conn.Reconnect()
		if err != nil {
			return errorx.NewIOErr(err.Error())
		}
	}
	start := time.Now()
	r, err := s.conn.GetDB().Exec(sqlStr)
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

func (s *SQLSinkConnector) extractKeys(item map[string]any) []string {
	if len(s.config.Fields) > 0 {
		return s.config.Fields
	}
	keys := make([]string, 0, len(item))
	for k := range item {
		keys = append(keys, k)
	}
	return keys
}

func buildInsertSQL(table string, keys []string, values []string) string {
	sql := fmt.Sprintf("INSERT INTO %s (%s) values ", table, strings.Join(keys, ",")) + strings.Join(values, ",") + ";"
	return sql
}

func GetSink() api.Sink {
	return &SQLSinkConnector{}
}

var (
	_ api.TupleCollector  = &SQLSinkConnector{}
	_ util.PingableConn   = &SQLSinkConnector{}
	_ model.PropsConsumer = &SQLSinkConnector{}
)
