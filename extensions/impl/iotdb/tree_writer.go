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
	"time"

	"github.com/apache/iotdb-client-go/v2/client"
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

// treeWriter writes data using the IoTDB tree model via SessionPool.
type treeWriter struct {
	pool *client.SessionPool
	conf *iotdbConfig
	// lastAuto is the monotonic cursor for auto-generated timestamps.
	lastAuto int64
}

func (w *treeWriter) connect(ctx api.StreamContext, conf *iotdbConfig) error {
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
	}
	if len(conf.NodeUrls) > 0 {
		poolConfig.NodeUrls = conf.NodeUrls
	}

	pool := client.NewSessionPool(poolConfig, conf.PoolSize, int(conf.Timeout), 60000, false)
	w.pool = &pool

	// test connection by acquiring and releasing a session
	session, err := w.pool.GetSession()
	if err != nil {
		return fmt.Errorf("failed to get iotdb session: %w", err)
	}
	w.pool.PutBack(session)

	ctx.GetLogger().Infof("iotdb tree writer connected to %s", conf.Addr)
	return nil
}

func (w *treeWriter) write(ctx api.StreamContext, data []map[string]any) error {
	if len(data) == 0 {
		return nil
	}
	logger := ctx.GetLogger()

	session, err := w.pool.GetSession()
	if err != nil {
		return fmt.Errorf("failed to get iotdb session: %w", err)
	}
	defer w.pool.PutBack(session)

	schemas, err := buildMeasurementSchemas(w.conf.Measurements, w.conf.DataTypes)
	if err != nil {
		return err
	}

	// write in batches according to the configured BatchSize
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

		tablet, err := client.NewTablet(w.conf.Device, schemas, len(chunk))
		if err != nil {
			return fmt.Errorf("failed to create tablet: %w", err)
		}
		if err := fillTablet(tablet, chunk, w.conf, &w.lastAuto); err != nil {
			return err
		}

		if w.conf.IsAligned {
			if err := session.InsertAlignedTablet(tablet, false); err != nil {
				return fmt.Errorf("insert aligned tablet: %w", err)
			}
		} else {
			if err := session.InsertTablet(tablet, false); err != nil {
				return fmt.Errorf("insert tablet: %w", err)
			}
		}

		logger.Debugf("iotdb tree writer inserted %d rows", tablet.RowSize)
	}
	return nil
}

func (w *treeWriter) close() error {
	if w.pool != nil {
		w.pool.Close()
	}
	return nil
}

// extractTimestamp returns the timestamp for the given row. If tsFieldName is
// empty or missing, the current time in milliseconds is returned.
func extractTimestamp(row map[string]any, tsFieldName string) (int64, error) {
	if tsFieldName == "" {
		return time.Now().UnixMilli(), nil
	}
	v, ok := row[tsFieldName]
	if !ok {
		return time.Now().UnixMilli(), nil
	}
	ts, err := cast.ToInt64(v, cast.CONVERT_ALL)
	if err != nil {
		return 0, fmt.Errorf("invalid timestamp field %q: %v", tsFieldName, err)
	}
	return ts, nil
}
