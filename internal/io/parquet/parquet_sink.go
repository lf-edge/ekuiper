// Copyright 2025 EMQ Technologies Co., Ltd.
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

package parquet

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

type ParquetSinkConf struct {
	Path       string `json:"path"`
	JsonSchema string `json:"jsonSchema"`

	RollingInterval time.Duration `json:"rollingInterval"`
	RollingCount    int64         `json:"rollingCount"`
	RollingSize     int64         `json:"rollingSize"`
}

type parquetSink struct {
	conf *ParquetSinkConf

	fw source.ParquetFile
	pw *writer.JSONWriter

	currentCount int64
	currentSize  int64
	initFileTs   time.Time
}

func (p *parquetSink) Provision(ctx api.StreamContext, props map[string]any) error {
	c := &ParquetSinkConf{}
	if err := cast.MapToStruct(props, c); err != nil {
		return err
	}
	if c.Path == "" {
		return errors.New("path cannot be empty")
	}
	if c.JsonSchema == "" {
		return errors.New("jsonSchema cannot be empty")
	}
	p.conf = c
	return p.initParquetWriter()
}

func (p *parquetSink) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	return nil
}

func (p *parquetSink) Collect(ctx api.StreamContext, item api.MessageTuple) error {
	return p.writeMsg(item.ToMap())
}

func (p *parquetSink) CollectList(ctx api.StreamContext, items api.MessageTupleList) error {
	for _, m := range items.ToMaps() {
		if err := p.writeMsg(m); err != nil {
			return err
		}
	}
	return nil
}

func (p *parquetSink) Close(ctx api.StreamContext) error {
	p.finishCurrentWriter()
	return nil
}

func (p *parquetSink) writeMsg(m map[string]any) error {
	s, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if err := p.tryRolling(len(s)); err != nil {
		return err
	}
	if err := p.pw.Write(string(s)); err != nil {
		return err
	}
	p.updateStat(len(s))
	return nil
}

func (p *parquetSink) finishCurrentWriter() {
	p.pw.WriteStop()
	p.fw.Close()
}

func (p *parquetSink) initParquetWriter() error {
	var err error
	filename := fmt.Sprintf("%v.%v", p.conf.Path, time.Now().Format("20060102150405"))
	p.fw, err = local.NewLocalFileWriter(fmt.Sprintf("%v.%v", p.conf.Path, filename))
	if err != nil {
		return fmt.Errorf("can't create file %v", err)
	}
	p.pw, err = writer.NewJSONWriter(p.conf.JsonSchema, p.fw, 4)
	if err != nil {
		return fmt.Errorf("can't create json writer %v", err)
	}
	p.currentSize = 0
	p.currentSize = 0
	p.initFileTs = time.Now()
	return nil
}

func (p *parquetSink) tryRolling(msgSize int) error {
	if p.conf.RollingCount > 0 && p.currentCount+1 >= p.conf.RollingCount {
		return p.rolling()
	}
	if p.conf.RollingSize > 0 && p.currentSize+int64(msgSize) >= p.conf.RollingSize {
		return p.rolling()
	}
	if p.conf.RollingInterval > 0 && time.Now().Sub(p.initFileTs) > p.conf.RollingInterval {
		return p.rolling()
	}
	return nil
}

func (p *parquetSink) rolling() error {
	p.finishCurrentWriter()
	return p.initParquetWriter()
}

func (p *parquetSink) updateStat(msgSize int) {
	p.currentSize += int64(msgSize)
	p.currentCount += 1
}

func GetSink() api.Sink {
	return &parquetSink{}
}
