// Copyright 2026 EMQ Technologies Co., Ltd.
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

//go:build parquet || full

package file

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/parquet-go/parquet-go"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/filex"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func init() {
	modules.RegisterSink("parquet", GetParquetSink)
}

type parquetSink struct {
	conf         *sinkConf
	schema       *parquet.Schema
	w            *parquet.GenericWriter[map[string]any]
	file         *os.File
	mu           syncx.Mutex
	dataWrote    int
	creationTime time.Time
}

func (p *parquetSink) Provision(ctx api.StreamContext, props map[string]any) error {
	c := &sinkConf{
		RollingCount:  1000000,
		Path:          "cache",
		FileType:      PARQUET_TYPE,
		CheckInterval: cast.DurationConf(5 * time.Minute),
	}

	if err := cast.MapToStruct(props, c); err != nil {
		return err
	}

	if c.RollingInterval < 0 {
		return fmt.Errorf("rollingInterval must be positive")
	}

	if c.RollingCount < 0 {
		return fmt.Errorf("rollingCount must be positive")
	}

	if c.CheckInterval < 0 {
		return fmt.Errorf("checkInterval must be positive")
	}

	if c.RollingSize > 0 {
		return fmt.Errorf("rollingSize cannot be used with parquet")
	}

	if c.Compression != "" {
		return fmt.Errorf("compression cannot be used with parquet")
	}

	if c.Encryption != "" {
		return fmt.Errorf("encryption cannot be used with parquet")
	}

	if c.RollingHook != "" {
		return fmt.Errorf("rollingHook cannot be used with parquet")
	}

	if c.RollingInterval == 0 && c.RollingCount == 0 {
		return fmt.Errorf("one of rollingInterval, rollingCount must be set")
	}

	if c.RollingInterval > 0 && c.RollingInterval < c.CheckInterval {
		c.CheckInterval = c.RollingInterval
		ctx.GetLogger().Infof("set checkInterval to %v", c.CheckInterval)
	}

	if c.RollingNamePattern != "" && c.RollingNamePattern != "prefix" && c.RollingNamePattern != "suffix" && c.RollingNamePattern != "none" {
		return fmt.Errorf("rollingNamePattern must be one of prefix, suffix or none")
	}

	if c.Path == "" {
		return fmt.Errorf("path must be set")
	}

	if strings.Contains(c.Path, "{{") {
		return fmt.Errorf("path templates cannot be used with parquet")
	}

	validPath, err := filex.ValidateFilePath(c.Path)
	if err != nil {
		return err
	}
	c.Path = validPath

	if c.FileType != PARQUET_TYPE {
		return fmt.Errorf("fileType must be parquet not %s", c.FileType)
	}

	p.conf = c
	schema, err := p.buildSchema()
	if err != nil {
		return err
	}

	p.schema = schema
	return nil
}

func (p *parquetSink) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	if p.conf.CheckInterval > 0 && p.conf.RollingInterval > 0 {
		t := timex.GetTicker(time.Duration(p.conf.CheckInterval))
		go func() {
			defer t.Stop()
			for {
				select {
				case now := <-t.C:
					e := infra.SafeRun(func() error {
						p.mu.Lock()
						defer p.mu.Unlock()

						if now.Sub(p.creationTime) >= time.Duration(p.conf.RollingInterval) {
							err := p.closeFileAndWriter()
							if err != nil {
								return fmt.Errorf("parquet sink fails to close file %s: %w", p.conf.Path, err)
							}
						}
						return nil
					})
					if e != nil {
						ctx.GetLogger().Error(e)
					}
				case <-ctx.Done():
					ctx.GetLogger().Info("parquet sink done")
					return
				}
			}
		}()
	}
	sch(api.ConnectionConnected, "")
	return nil
}

func (p *parquetSink) Collect(ctx api.StreamContext, tuple api.MessageTuple) error {
	row := tuple.ToMap()
	if err := p.checkTypeSafety(row); err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	err := p.tryRoll()
	if err != nil {
		return err
	}

	n, err := p.w.Write([]map[string]any{row})
	p.dataWrote += n
	return err
}

func (p *parquetSink) CollectList(ctx api.StreamContext, tuples api.MessageTupleList) error {
	rows := tuples.ToMaps()
	for _, row := range rows {
		if err := p.checkTypeSafety(row); err != nil {
			return err
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	err := p.tryRoll()
	if err != nil {
		return err
	}

	n, err := p.w.Write(rows)
	p.dataWrote += n
	return err
}

func (p *parquetSink) Close(ctx api.StreamContext) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.closeFileAndWriter()
}

func (p *parquetSink) checkTypeSafety(tuple map[string]any) error {
	for colName, colValue := range tuple {
		leaf, ok := p.schema.Lookup(colName)
		if !ok {
			return fmt.Errorf("column %q: not declared in columns", colName)
		}

		n := leaf.Node
		if colValue == nil {
			if !n.Optional() {
				return fmt.Errorf("column %q: required value is missing", colName)
			}
			continue
		}

		switch n.Type().Kind() {
		case parquet.ByteArray:
			if _, ok := colValue.(string); !ok {
				return fmt.Errorf("column %q: expected a string value, got %T", colName, colValue)
			}
		case parquet.Int64:
			if _, ok := colValue.(int64); !ok {
				return fmt.Errorf("column %q: expected an int64 value, got %T", colName, colValue)
			}
		case parquet.Double:
			if _, ok := colValue.(float64); !ok {
				return fmt.Errorf("column %q: expected a double value, got %T", colName, colValue)
			}
		case parquet.Boolean:
			if _, ok := colValue.(bool); !ok {
				return fmt.Errorf("column %q: expected a boolean value, got %T", colName, colValue)
			}
		}
	}
	return nil
}

func (p *parquetSink) tryRoll() error {
	if p.isTimeToRoll() {
		if err := p.closeFileAndWriter(); err != nil {
			return err
		}
	}
	if p.file != nil {
		return nil
	}

	fname := filepath.Base(p.conf.Path)
	switch p.conf.RollingNamePattern {
	case "prefix":
		fname = fmt.Sprintf("%d-%s", timex.GetNowInMilli(), fname)
	case "suffix":
		ext := filepath.Ext(fname)
		fname = fmt.Sprintf("%s-%d%s", strings.TrimSuffix(fname, ext), timex.GetNowInMilli(), ext)
	}
	path := filepath.Join(filepath.Dir(p.conf.Path), fname)

	if err := p.createParquetFile(path); err != nil {
		return err
	}
	if p.w == nil {
		p.w = parquet.NewGenericWriter[map[string]any](p.file, p.schema)
	} else {
		p.w.Reset(p.file)
	}
	return nil
}

const optionalType = "?"

func (p *parquetSink) buildSchema() (*parquet.Schema, error) {
	if len(p.conf.Columns) == 0 {
		return nil, fmt.Errorf("columns must be set")
	}

	group := make(parquet.Group, len(p.conf.Columns))
	for colName, colType := range p.conf.Columns {
		if colName == "" {
			return nil, fmt.Errorf("column name must not be empty")
		}
		// check if the type is defined as nullable: int64?
		trimmedType, nullable := strings.CutSuffix(colType, optionalType)
		node, err := p.getParquetType(trimmedType, nullable)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", colName, err)
		}

		group[colName] = node
	}

	return parquet.NewSchema("sink", group), nil
}

const (
	parquetBoolean = "BOOLEAN"
	parquetInt64   = "INT64"
	parquetDouble  = "DOUBLE"
	parquetString  = "STRING"
)

func (p *parquetSink) getParquetType(t string, null bool) (parquet.Node, error) {
	var n parquet.Node
	switch strings.ToUpper(t) {
	case parquetBoolean:
		n = parquet.Leaf(parquet.BooleanType)
	case parquetInt64:
		n = parquet.Int(64)
	case parquetDouble:
		n = parquet.Leaf(parquet.DoubleType)
	case parquetString:
		n = parquet.String()
	default:
		return nil, fmt.Errorf("unsupported type %q, must be one of boolean, int64, double, string", t)
	}

	if null {
		return parquet.Optional(n), nil
	}

	return n, nil
}

func (p *parquetSink) createParquetFile(path string) error {
	var (
		err error
		dir = filepath.Dir(path)
	)
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		if err = os.MkdirAll(dir, 0o750); err != nil {
			return fmt.Errorf("fail to create file %s: %w", path, err)
		}
	}

	p.file, err = os.OpenFile(path, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0o640)
	if err != nil {
		return fmt.Errorf("fail to open parquet file %s: %w", path, err)
	}

	p.creationTime = timex.GetNow()
	return nil
}

func (p *parquetSink) isTimeToRoll() bool {
	return p.conf.RollingCount > 0 && p.dataWrote >= p.conf.RollingCount
}

func (p *parquetSink) closeFileAndWriter() error {
	if p.file == nil || p.w == nil {
		return nil
	}

	err := errors.Join(
		p.w.Close(),
		p.file.Sync(),
		p.file.Close(),
	)
	p.file = nil
	p.dataWrote = 0
	return err
}

func GetParquetSink() api.Sink {
	return &parquetSink{}
}
