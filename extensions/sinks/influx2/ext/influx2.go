// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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

package influx2

import (
	"context"
	"fmt"
	"time"

	client "github.com/influxdata/influxdb-client-go/v2"

	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/errorx"
)

// c is the configuration for influx2 sink
type c struct {
	// connection
	Addr         string        `json:"addr"`
	Token        string        `json:"token"`
	Org          string        `json:"org"`
	Bucket       string        `json:"bucket"`
	PrecisionStr string        `json:"precision"`
	Precision    time.Duration `json:"-"`
	// http connection
	// tls
	// write options
	UseLineProtocol bool              `json:"useLineProtocol"` // 0: json, 1: line protocol
	Measurement     string            `json:"measurement"`
	Tags            map[string]string `json:"tags"`
	TsFieldName     string            `json:"tsFieldName"`
	// common options
	DataField  string   `json:"dataField"`
	Fields     []string `json:"fields"`
	BatchSize  int      `json:"batchSize"`
	SendSingle bool     `json:"sendSingle"`
}

// influxSink2 is the sink for influx2.
// To ensure exact order, it uses blocking write api to write data to influxdb2.
type influxSink2 struct {
	conf c
	// save the token privately
	token string
	// tagKey    string
	// tagValue  string
	// already have
	// dataField string
	// fields    []string

	hasTransform bool
	cli          client.Client
}

func (m *influxSink2) Configure(props map[string]any) error {
	m.conf = c{
		PrecisionStr: "ms",
	}
	err := cast.MapToStruct(props, &m.conf)
	if err != nil {
		return fmt.Errorf("error configuring influx2 sink: %s", err)
	}
	if len(m.conf.Addr) == 0 {
		return fmt.Errorf("addr is required")
	}
	if len(m.conf.Org) == 0 {
		return fmt.Errorf("org is required")
	}
	if len(m.conf.Bucket) == 0 {
		return fmt.Errorf("bucket is required")
	}
	switch m.conf.PrecisionStr {
	case "ms":
		m.conf.Precision = time.Millisecond
	case "s":
		m.conf.Precision = time.Second
	case "us":
		m.conf.Precision = time.Microsecond
	case "ns":
		m.conf.Precision = time.Nanosecond
	default:
		return fmt.Errorf("precision %s is not supported", m.conf.PrecisionStr)
	}
	if len(m.conf.Measurement) == 0 {
		return fmt.Errorf("measurement is required")
	}
	m.token = m.conf.Token
	m.conf.Token = "******"
	return err
}

func (m *influxSink2) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("influx2 sink open with properties %+v", m.conf)
	if m.conf.BatchSize <= 0 {
		m.conf.BatchSize = 1
	}
	options := client.DefaultOptions().SetPrecision(m.conf.Precision).SetBatchSize(uint(m.conf.BatchSize))
	m.cli = client.NewClientWithOptions(m.conf.Addr, m.token, options)
	_, err := m.cli.Ping(context.Background())
	return err
}

func (m *influxSink2) Collect(ctx api.StreamContext, data any) error {
	logger := ctx.GetLogger()
	// Write out with blocking API to keep order. Batch is done by sink node side
	writeAPI := m.cli.WriteAPIBlocking(m.conf.Org, m.conf.Bucket)
	if !m.conf.UseLineProtocol {
		pts, err := m.transformPoints(ctx, data)
		if err != nil {
			return err
		}
		err = writeAPI.WritePoint(ctx, pts...)
		if err != nil {
			logger.Errorf("influx2 sink error: %v", err)
			return fmt.Errorf(`%s: influx2 sink fails to send out the data . %v`, errorx.IOErr, err)
		}
	} else {
		lines, err := m.transformLines(ctx, data)
		if err != nil {
			return err
		}
		err = writeAPI.WriteRecord(ctx, lines...)
		if err != nil {
			logger.Errorf("influx2 sink error: %v", err)
			return fmt.Errorf(`%s: influx2 sink fails to send out the data . %v`, errorx.IOErr, err)
		}
	}
	logger.Debug("insert data into influxdb2 success")
	return nil
}

func (m *influxSink2) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("influx2 sink close")
	m.cli.Close()
	return nil
}

// Internal method to get timestamp from data
func (m *influxSink2) getTS(data map[string]any) (int64, error) {
	v, ok := data[m.conf.TsFieldName]
	if !ok {
		return 0, fmt.Errorf("time field %s not found", m.conf.TsFieldName)
	}
	v64, err := cast.ToInt64(v, cast.CONVERT_SAMEKIND)
	if err != nil {
		return 0, fmt.Errorf("time field %s can not convert to timestamp(int64) : %v", m.conf.TsFieldName, v)
	}
	return v64, nil
}

func GetSink() api.Sink {
	return &influxSink2{}
}
