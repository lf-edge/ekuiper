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
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/pingcap/failpoint"

	client2 "github.com/lf-edge/ekuiper/v2/extensions/impl/sql/client"
	"github.com/lf-edge/ekuiper/v2/extensions/impl/sql/sqldatabase/sqlgen"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

type SQLSourceConnector struct {
	id    string
	conf  *SQLConf
	Query sqlgen.SqlQueryGenerator
	conn  *client2.SQLConnection
	props map[string]any
	conId string
	// cw is the pooled connection attached at Connect time. The
	// pooled object identity is stable across recovery, so conn is
	// resolved once while cw serves WaitReady parks and suspect
	// reports for every poll.
	cw *connection.ConnWrapper
	// refID is the consumer identity attached at Connect time. It is
	// passed back verbatim at Close; never re-derived from ctx.
	refID   string
	columns []interface{}
	stats   *sqlSourceStats
	ruleID  string
	opID    string
}

type sqlSourceStats struct {
	totalScanIntoMapDuration time.Duration
	totalWaitDuration        time.Duration
}

func (s *SQLSourceConnector) Ping(ctx api.StreamContext, m map[string]any) error {
	// Validation probe: connect for real on a throwaway candidate.
	// Dial is explicit here — SQLConnection.Ping itself is a pure
	// health check and never dials.
	cli := &client2.SQLConnection{}
	err := cli.Provision(ctx, "test", m)
	if err != nil {
		return err
	}
	defer cli.Close(ctx)
	if err := cli.Dial(ctx); err != nil {
		return err
	}
	return cli.Ping(ctx)
}

func (s *SQLSourceConnector) resetStats() {
	s.stats = &sqlSourceStats{}
}

func (s *SQLSourceConnector) updateMetrics() {
	if s.stats != nil {
		SqlSourceQueryDurationHist.WithLabelValues(LblWait, s.ruleID, s.opID).Observe(float64(s.stats.totalWaitDuration.Microseconds()))
		SqlSourceQueryDurationHist.WithLabelValues(LblScanInto, s.ruleID, s.opID).Observe(float64(s.stats.totalScanIntoMapDuration.Microseconds()))
		s.resetStats()
	}
}

type SQLConf struct {
	Interval            cast.DurationConf           `json:"interval"`
	DBUrl               string                      `json:"dburl"`
	URL                 string                      `json:"url,omitempty"`
	Datasource          string                      `json:"datasource"`
	TemplateSqlQueryCfg *sqlgen.TemplateSqlQueryCfg `json:"templateSqlQueryCfg"`
}

func init() {
	modules.RegisterConnection("sql", client2.CreateConnection)
}

func (s *SQLSourceConnector) Provision(ctx api.StreamContext, props map[string]any) error {
	cfg := &SQLConf{}
	err := cast.MapToStruct(props, cfg)
	failpoint.Inject("MapToStructErr", func() {
		err = errors.New("MapToStruct")
	})
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if time.Duration(cfg.Interval) < 1 {
		return fmt.Errorf("interval should be defined")
	}
	props, err = cfg.resolveDBURL(props)
	if err != nil {
		return err
	}
	s.conf = cfg
	s.props = props
	sqlDriver, err := client2.ParseDriver(cfg.DBUrl)
	if err != nil {
		return fmt.Errorf("dburl.Parse %s fail with error: %v", cfg.DBUrl, err)
	}
	generator, err := sqlgen.GetQueryGenerator(sqlDriver, props)
	failpoint.Inject("GetQueryGeneratorErr", func() {
		err = errors.New("GetQueryGeneratorErr")
	})
	if err != nil {
		return fmt.Errorf("GetQueryGenerator %s fail with error: %v", cfg.DBUrl, err)
	}
	s.Query = generator
	return nil
}

func (s *SQLSourceConnector) Connect(ctx api.StreamContext, sc api.StatusChangeHandler) error {
	ctx.GetLogger().Infof("Connecting to sql server")
	var cli *client2.SQLConnection
	var err error
	s.id = s.conf.DBUrl
	key, requireExisting, err := sqlConnectionKey(s.props, s.conf.DBUrl)
	if err != nil {
		return err
	}
	refID := connection.ConsumerRefID(ctx)
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
	s.conId = cw.ID
	s.refID = refID
	s.cw = cw
	conn, err := cw.Wait(ctx)
	if conn == nil {
		return fmt.Errorf("sql client not ready: %v", err)
	}
	cli = conn.(*client2.SQLConnection)
	s.conn = cli
	s.ruleID = ctx.GetRuleId()
	s.opID = ctx.GetOpId()
	return err
}

func (s *SQLSourceConnector) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing sql source connector url:%v", s.conf.DBUrl)
	if s.conn != nil {
		s.conn.DetachSub(ctx, s.props)
	}
	return connection.DetachConnectionByRef(ctx, s.conId, s.refID)
}

func (s *SQLSourceConnector) Pull(ctx api.StreamContext, recvTime time.Time, ingest api.TupleIngest, ingestError api.ErrorIngest) {
	SqlSourceCounter.WithLabelValues(LblPull, s.ruleID, s.opID).Inc()
	s.queryData(ctx, recvTime, ingest, ingestError)
}

func (s *SQLSourceConnector) queryData(ctx api.StreamContext, rcvTime time.Time, ingest api.TupleIngest, ingestError api.ErrorIngest) {
	s.resetStats()
	logger := ctx.GetLogger()
	// Park on the Pool gate first: connected is a fast state read,
	// a closed gate waits for the recovery worker. The first failed
	// poll surfaces below and reports a suspect; later polls park
	// here instead of retrying locally.
	if err := s.cw.WaitReady(ctx); err != nil {
		logger.Errorf("wait sql connection ready error %v", err)
		ingestError(ctx, err)
		return
	}
	query, err := s.Query.SqlQueryStatement()
	failpoint.Inject("StatementErr", func() {
		err = errors.New("StatementErr")
	})
	if err != nil {
		logger.Errorf("Get sql query error %v", err)
		ingestError(ctx, err)
		return
	}
	logger.Debugf("Query the database with %s", query)

	queryStart := time.Now()
	rows, err := s.conn.QueryContext(ctx, query)
	failpoint.Inject("QueryErr", func() {
		err = errors.New("QueryErr")
	})
	if err != nil {
		logger.Errorf("query sql error %v", err)
		// First failure surfaces and reports a suspect, unless the
		// caller itself is already gone (see reportTransportFailure);
		// the Pool verifies and recovers while later polls park
		// above. Only a failed QueryContext reports — statement,
		// column-type and scan errors below are data errors, not
		// transport.
		reportTransportFailure(ctx, s.cw)
		ingestError(ctx, err)
		return
	}
	cols, _ := rows.Columns()
	types, err := rows.ColumnTypes()
	failpoint.Inject("ColumnTypesErr", func() {
		err = errors.New("ColumnTypesErr")
	})
	if err != nil {
		logger.Errorf("query %v row ColumnTypes error %v", query, err)
		ingestError(ctx, err)
		return
	}
	if s.columns == nil {
		columns := make([]interface{}, len(cols))
		prepareValues(ctx, columns, types, cols)
		s.columns = columns
	}
	rowCount := 0
	for rows.Next() {
		data := make(map[string]interface{})
		err := rows.Scan(s.columns...)
		failpoint.Inject("ScanErr", func() {
			err = errors.New("ScanErr")
		})
		if err != nil {
			logger.Errorf("Run sql scan(%s) error %v", query, err)
			ingestError(ctx, err)
			return
		}
		scanIntoMap(data, s.columns, cols, s.stats)
		s.Query.UpdateMaxIndexValue(data)
		watiStart := time.Now()
		ingest(ctx, data, nil, rcvTime)
		s.stats.totalWaitDuration += time.Since(watiStart)
		rowCount++
	}
	SqlSourceGauge.WithLabelValues(LblQuery, s.ruleID, s.opID).Set(float64(rowCount))
	SqlSourceQueryDurationHist.WithLabelValues(LblQuery, s.ruleID, s.opID).Observe(float64(time.Since(queryStart).Microseconds()))
}

func (s *SQLSourceConnector) GetOffset() (interface{}, error) {
	return s.Query.GetIndexValue(), nil
}

func (s *SQLSourceConnector) Rewind(offset interface{}) error {
	s.Query.SetIndexValue(offset)
	return nil
}

func (s *SQLSourceConnector) ResetOffset(input map[string]interface{}) error {
	wrap := s.Query.GetIndexValueWrap()
	if wrap != nil {
		wrap.UpdateByInput(input)
	}
	return nil
}

func scanIntoMap(mapValue map[string]interface{}, values []interface{}, columns []string, stats *sqlSourceStats) {
	start := time.Now()
	defer func() {
		if stats != nil {
			stats.totalScanIntoMapDuration += time.Since(start)
		}
	}()
	for idx, column := range columns {
		if reflectValue := reflect.Indirect(reflect.Indirect(reflect.ValueOf(values[idx]))); reflectValue.IsValid() {
			mapValue[column] = reflectValue.Interface()
			if valuer, ok := mapValue[column].(driver.Valuer); ok {
				mapValue[column], _ = valuer.Value()
			} else if b, ok := mapValue[column].(sql.RawBytes); ok {
				mapValue[column] = string(b)
			}
		} else {
			mapValue[column] = nil
		}
	}
}

func prepareValues(ctx api.StreamContext, values []interface{}, columnTypes []*sql.ColumnType, columns []string) {
	if len(columnTypes) > 0 {
		for idx, columnType := range columnTypes {
			nullable, ok := columnType.Nullable()
			if got := buildScanValueByColumnType(ctx, columnType.Name(), columnType.DatabaseTypeName(), nullable && ok); got != nil {
				values[idx] = got
				continue
			}
			if columnType.ScanType() != nil {
				values[idx] = reflect.New(reflect.PointerTo(columnType.ScanType())).Interface()
			} else {
				values[idx] = new(interface{})
			}
		}
	} else {
		for idx := range columns {
			values[idx] = new(interface{})
		}
	}
}

func GetSource() api.Source {
	return &SQLSourceConnector{}
}

var (
	_ api.PullTupleSource = &SQLSourceConnector{}
	_ util.PingableConn   = &SQLSourceConnector{}
)

func (sc *SQLConf) resolveDBURL(props map[string]any) (map[string]any, error) {
	if len(sc.DBUrl) < 1 && len(sc.URL) < 1 {
		return props, fmt.Errorf("dburl should be defined")
	}
	if len(sc.DBUrl) < 1 {
		props["dburl"] = props["url"]
		sc.DBUrl = sc.URL
	}
	sc.URL = ""
	return props, nil
}

func buildScanValueByColumnType(ctx api.StreamContext, colName, colType string, nullable bool) interface{} {
	switch strings.ToUpper(colType) {
	case "CHAR", "VARCHAR", "NCHAR", "NVARCHAR", "TEXT", "NTEXT":
		if nullable {
			return &sql.NullString{}
		}
		return new(string)
	case "DECIMAL", "NUMERIC", "FLOAT", "REAL":
		if nullable {
			return &sql.NullFloat64{}
		}
		return new(float64)
	case "BOOL":
		if nullable {
			return &sql.NullBool{}
		}
		return new(bool)
	case "INT", "BIGINT", "SMALLINT", "TINYINT", "INT4":
		if nullable {
			return &sql.NullInt64{}
		}
		return new(int64)
	case "TIMESTAMP":
		if nullable {
			return &sql.NullTime{}
		}
		return new(time.Time)
	default:
		ctx.GetLogger().Debugf("sql source meet column %v unknown columnType:%v", colName, colType)
		return nil
	}
}
