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
	"time"

	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	client2 "github.com/lf-edge/ekuiper/v2/extensions/impl/sql/client"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/sqldatabase/sqlgen"
)

type SQLSourceConnector struct {
	id            string
	conf          *SQLConf
	Query         sqlgen.SqlQueryGenerator
	conn          *client2.SQLConnection
	props         map[string]any
	needReconnect bool
}

type SQLConf struct {
	Interval   cast.DurationConf `json:"interval"`
	DBUrl      string            `json:"dburl"`
	URL        string            `json:"url,omitempty"`
	Datasource string            `json:"datasource"`
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
	if err := cfg.resolveDBURL(); err != nil {
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

func (s *SQLSourceConnector) Connect(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Connecting to sql server")
	var cli *client2.SQLConnection
	var err error
	s.id = s.conf.DBUrl
	cw, err := connection.FetchConnection(ctx, s.id, "sql", s.props)
	if err != nil {
		return err
	}
	conn, err := cw.Wait()
	if err != nil {
		return err
	}
	cli = conn.(*client2.SQLConnection)
	s.conn = cli
	return err
}

func (s *SQLSourceConnector) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing sql source connector url:%v", s.conf.DBUrl)
	if s.conn != nil {
		s.conn.DetachSub(ctx, s.props)
	}
	connection.DetachConnection(ctx, s.id, s.props)
	return nil
}

func (s *SQLSourceConnector) Pull(ctx api.StreamContext, recvTime time.Time, ingest api.TupleIngest, ingestError api.ErrorIngest) {
	s.queryData(ctx, recvTime, ingest, ingestError)
}

func (s *SQLSourceConnector) queryData(ctx api.StreamContext, rcvTime time.Time, ingest api.TupleIngest, ingestError api.ErrorIngest) {
	logger := ctx.GetLogger()
	if s.needReconnect {
		err := s.conn.Reconnect()
		if err != nil {
			logger.Errorf("reconnect db error %v", err)
			ingestError(ctx, err)
			return
		}
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
	rows, err := s.conn.GetDB().Query(query)
	failpoint.Inject("QueryErr", func() {
		err = errors.New("QueryErr")
	})
	if err != nil {
		logger.Errorf("query sql error %v", err)
		s.needReconnect = true
		ingestError(ctx, err)
		return
	} else if s.needReconnect {
		logger.Infof("reconnect sql success")
		s.needReconnect = false
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
	for rows.Next() {
		data := make(map[string]interface{})
		columns := make([]interface{}, len(cols))
		prepareValues(columns, types, cols)
		err := rows.Scan(columns...)
		failpoint.Inject("ScanErr", func() {
			err = errors.New("ScanErr")
		})
		if err != nil {
			logger.Errorf("Run sql scan(%s) error %v", query, err)
			ingestError(ctx, err)
			return
		}
		scanIntoMap(data, columns, cols)
		s.Query.UpdateMaxIndexValue(data)
		ingest(ctx, data, nil, rcvTime)
	}
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

func scanIntoMap(mapValue map[string]interface{}, values []interface{}, columns []string) {
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

func prepareValues(values []interface{}, columnTypes []*sql.ColumnType, columns []string) {
	if len(columnTypes) > 0 {
		for idx, columnType := range columnTypes {
			if columnType.ScanType() != nil {
				values[idx] = reflect.New(reflect.PtrTo(columnType.ScanType())).Interface()
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

var _ api.PullTupleSource = &SQLSourceConnector{}

func (sc *SQLConf) resolveDBURL() error {
	if len(sc.DBUrl) < 1 && len(sc.URL) < 1 {
		return fmt.Errorf("dburl should be defined")
	}
	if len(sc.DBUrl) < 1 {
		sc.DBUrl = sc.URL
	}
	sc.URL = ""
	return nil
}
