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

	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
)

type tableSessionProvider interface {
	GetSession() (client.ITableSession, error)
}

type tableSessionPool interface {
	tableSessionProvider
	Close()
}

// tableWriter writes data using the IoTDB table model via TableSessionPool.
type tableWriter struct {
	pool tableSessionPool
	conf *iotdbConfig
	// lastAuto is the monotonic cursor for auto-generated timestamps.
	lastAuto int64
}

func (w *tableWriter) connect(ctx api.StreamContext, conf *iotdbConfig) error {
	w.conf = conf

	// Bootstrap without a database because the client selects PoolConfig.Database
	// during OpenSession, before CREATE DATABASE can run.
	bootstrapConfig, err := conf.newPoolConfig("")
	if err != nil {
		return err
	}
	bootstrapPool := client.NewTableSessionPool(bootstrapConfig, conf.PoolSize, int(conf.Timeout), 60000, false)
	if err := ensureDatabase(&bootstrapPool, conf.Database); err != nil {
		bootstrapPool.Close()
		return err
	}
	bootstrapPool.Close()
	ctx.GetLogger().Infof("iotdb table writer: database %q ensured", conf.Database)

	poolConfig, err := conf.newPoolConfig(conf.Database)
	if err != nil {
		return err
	}
	pool := client.NewTableSessionPool(poolConfig, conf.PoolSize, int(conf.Timeout), 60000, false)
	session, err := pool.GetSession()
	if err != nil {
		pool.Close()
		return fmt.Errorf("failed to get iotdb table session: %w", err)
	}
	// PooledTableSession.Close returns the wrapper's session to the table pool.
	_ = session.Close()
	w.pool = &pool

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
		if err := fillTablet(tablet, chunk, w.conf, &w.lastAuto); err != nil {
			return err
		}
		if err := tablet.Sort(); err != nil {
			return fmt.Errorf("sort relational tablet: %w", err)
		}
		if err := insertRelationalTablet(w.pool, tablet); err != nil {
			return err
		}
		logger.Debugf("iotdb table writer inserted %d rows", tablet.RowSize)
	}
	return nil
}

func ensureDatabase(pool tableSessionProvider, database string) error {
	session, err := pool.GetSession()
	if err != nil {
		return fmt.Errorf("failed to get iotdb table session for database creation: %w", err)
	}
	defer session.Close()

	if err := session.ExecuteNonQueryStatement(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database)); err != nil {
		return fmt.Errorf("create iotdb database %q: %w", database, err)
	}
	return nil
}

func insertRelationalTablet(pool tableSessionProvider, tablet *client.Tablet) error {
	session, err := pool.GetSession()
	if err != nil {
		return errorx.NewIOErr(fmt.Sprintf("failed to get iotdb table session: %v", err))
	}
	defer session.Close()

	if err := session.Insert(tablet); err != nil {
		return errorx.NewIOErr(fmt.Sprintf("insert relational tablet: %v", err))
	}
	return nil
}

func (w *tableWriter) close() error {
	if w.pool != nil {
		w.pool.Close()
	}
	return nil
}
