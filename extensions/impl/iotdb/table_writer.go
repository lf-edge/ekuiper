// Copyright 2026 Timecho Limited
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

package iotdb

import (
	"fmt"

	"github.com/apache/iotdb-client-go/v2/client"
	"github.com/lf-edge/ekuiper/contract/v2/api"
)

// tableWriter writes data using the IoTDB table model via TableSessionPool.
type tableWriter struct {
	pool *client.TableSessionPool
	conf *iotdbConfig
}

func (w *tableWriter) connect(ctx api.StreamContext, conf *iotdbConfig) error {
	w.conf = conf

	host, port, err := splitAddr(conf.Addr)
	if err != nil {
		return err
	}

	poolConfig := &client.PoolConfig{
		Host:     host,
		Port:     port,
		UserName: conf.Username,
		Password: conf.Password,
		Database: conf.Database,
	}
	if len(conf.NodeUrls) > 0 {
		poolConfig.NodeUrls = conf.NodeUrls
	}

	pool := client.NewTableSessionPool(poolConfig, conf.PoolSize, int(conf.Timeout), 60000, false)
	w.pool = &pool

	// 自动创建数据库（table dialect 下的 CREATE DATABASE）
	autoSession, err := w.pool.GetSession()
	if err != nil {
		return fmt.Errorf("failed to get iotdb table session for database creation: %w", err)
	}
	createDBSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", conf.Database)
	if err := autoSession.ExecuteNonQueryStatement(createDBSQL); err != nil {
		autoSession.Close()
		// 数据库可能已存在，记录 warning 但不阻断连接
		ctx.GetLogger().Warnf("iotdb table writer: auto-create database %q failed (may already exist): %v", conf.Database, err)
	} else {
		ctx.GetLogger().Infof("iotdb table writer: database %q ensured", conf.Database)
	}
	autoSession.Close()

	// test connection by acquiring and closing a session
	session, err := w.pool.GetSession()
	if err != nil {
		return fmt.Errorf("failed to get iotdb table session: %w", err)
	}
	session.Close()

	ctx.GetLogger().Infof("iotdb table writer connected to %s (database=%s)", conf.Addr, conf.Database)
	return nil
}

func (w *tableWriter) write(ctx api.StreamContext, data []map[string]any) error {
	if len(data) == 0 {
		return nil
	}
	logger := ctx.GetLogger()

	schemas, err := buildMeasurementSchemas(w.conf.Measurements, w.conf.DataTypes)
	if err != nil {
		return err
	}
	categories := make([]client.ColumnCategory, 0, len(w.conf.ColumnCategories))
	for _, c := range w.conf.ColumnCategories {
		cc, err := toColumnCategory(c)
		if err != nil {
			return err
		}
		categories = append(categories, cc)
	}

	batchSize := w.conf.BatchSize
	if batchSize <= 0 {
		batchSize = len(data)
	}

	for start := 0; start < len(data); start += batchSize {
		end := start + batchSize
		if end > len(data) {
			end = len(data)
		}
		chunk := data[start:end]

		tablet, err := client.NewRelationalTablet(w.conf.Table, schemas, categories, len(chunk))
		if err != nil {
			return fmt.Errorf("failed to create relational tablet: %w", err)
		}

		for rowIdx, row := range chunk {
			ts, err := extractTimestamp(row, w.conf.TsFieldName)
			if err != nil {
				return err
			}
			tablet.SetTimestamp(ts, rowIdx)
			for colIdx, m := range w.conf.Measurements {
				raw, ok := row[m]
				if !ok {
					raw = nil
				}
				val, err := convertValue(raw, w.conf.DataTypes[colIdx])
				if err != nil {
					return fmt.Errorf("convert column %q row %d: %w", m, rowIdx, err)
				}
				if err := tablet.SetValueAt(val, colIdx, rowIdx); err != nil {
					return fmt.Errorf("set value at (%d, %d): %w", colIdx, rowIdx, err)
				}
			}
			tablet.RowSize++
		}

		session, err := w.pool.GetSession()
		if err != nil {
			return fmt.Errorf("failed to get iotdb table session: %w", err)
		}
		if err := session.Insert(tablet); err != nil {
			session.Close()
			return fmt.Errorf("insert relational tablet: %w", err)
		}
		session.Close()
		logger.Debugf("iotdb table writer inserted %d rows", tablet.RowSize)
	}
	return nil
}

func (w *tableWriter) close() error {
	if w.pool != nil {
		w.pool.Close()
	}
	return nil
}
