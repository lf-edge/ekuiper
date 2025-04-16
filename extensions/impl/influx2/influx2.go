// Copyright 2021-2025 EMQ Technologies Co., Ltd.
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

package influx2

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	client "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/extensions/impl/tspoint"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/cert"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
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
	// tls conf in cert.go
	// write options
	UseLineProtocol bool   `json:"useLineProtocol"` // 0: json, 1: line protocol
	Measurement     string `json:"measurement"`
	tspoint.WriteOptions
	BatchSize int `json:"batchSize"`
}

// influxSink2 is the sink for influx2.
// To ensure exact order, it uses blocking write api to write data to influxdb2.
type influxSink2 struct {
	conf    c
	tlsconf *tls.Config
	// save the token privately
	cli client.Client
}

func (m *influxSink2) Ping(ctx api.StreamContext, props map[string]any) error {
	if err := m.Provision(ctx, props); err != nil {
		return err
	}
	options := client.DefaultOptions().SetPrecision(m.conf.Precision).SetBatchSize(uint(m.conf.BatchSize))
	if m.tlsconf != nil {
		options = options.SetTLSConfig(m.tlsconf)
	}
	m.cli = client.NewClientWithOptions(m.conf.Addr, m.conf.Token, options)
	defer func() {
		if m.cli != nil {
			m.cli.Close()
		}
	}()
	pingable, err := m.cli.Ping(context.Background())
	if err != nil || !pingable {
		return fmt.Errorf("error connecting to influxdb2: %v", err)
	}
	return nil
}

func (m *influxSink2) Provision(ctx api.StreamContext, props map[string]any) error {
	m.conf = c{
		PrecisionStr: "ms",
		WriteOptions: tspoint.WriteOptions{
			PrecisionStr: "ms",
		},
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
	if len(m.conf.Measurement) == 0 && !m.conf.UseLineProtocol {
		return fmt.Errorf("measurement is required")
	}
	err = cast.MapToStruct(props, &m.conf.WriteOptions)
	if err != nil {
		return fmt.Errorf("error configuring influx2 sink: %s", err)
	}
	err = m.conf.WriteOptions.Validate()
	if err != nil {
		return err
	}
	tlsConf, err := cert.GenTLSConfig(ctx, props)
	if err != nil {
		return fmt.Errorf("error configuring tls: %s", err)
	}
	m.tlsconf = tlsConf
	if m.conf.BatchSize <= 0 {
		m.conf.BatchSize = 1
	}
	return nil
}

func (m *influxSink2) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) (err error) {
	options := client.DefaultOptions().SetPrecision(m.conf.Precision).SetBatchSize(uint(m.conf.BatchSize))
	if m.tlsconf != nil {
		options = options.SetTLSConfig(m.tlsconf)
	}
	defer func() {
		if err != nil {
			sch(api.ConnectionDisconnected, err.Error())
		} else {
			sch(api.ConnectionConnected, "")
		}
	}()
	m.cli = client.NewClientWithOptions(m.conf.Addr, m.conf.Token, options)
	// Test connection
	_, err = m.cli.Ping(ctx)
	return err
}

func (m *influxSink2) Collect(ctx api.StreamContext, item api.MessageTuple) error {
	return m.collect(ctx, item.ToMap())
}

func (m *influxSink2) CollectList(ctx api.StreamContext, items api.MessageTupleList) error {
	return m.collect(ctx, items.ToMaps())
}

func (m *influxSink2) collect(ctx api.StreamContext, data any) error {
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
			return errorx.NewIOErr(fmt.Sprintf(`influx2 sink fails to send out the data . %v`, err))
		}
	} else {
		lines, err := m.transformLines(ctx, data)
		if err != nil {
			return err
		}
		err = writeAPI.WriteRecord(ctx, lines...)
		if err != nil {
			logger.Errorf("influx2 sink error: %v", err)
			return errorx.NewIOErr(fmt.Sprintf(`influx2 sink fails to send out the data . %v`, err.Error()))
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

func (m *influxSink2) transformPoints(ctx api.StreamContext, data any) ([]*write.Point, error) {
	rawPts, err := tspoint.SinkTransform(ctx, data, &m.conf.WriteOptions)
	if err != nil {
		ctx.GetLogger().Error(err)
		return nil, err
	}
	pts := make([]*write.Point, 0, len(rawPts))
	for _, rawPt := range rawPts {
		pts = append(pts, client.NewPoint(m.conf.Measurement, rawPt.Tags, rawPt.Fields, rawPt.Tt))
	}
	return pts, nil
}

func (m *influxSink2) transformLines(ctx api.StreamContext, data any) ([]string, error) {
	rawPts, err := tspoint.SinkTransform(ctx, data, &m.conf.WriteOptions)
	if err != nil {
		ctx.GetLogger().Error(err)
		return nil, err
	}
	lines := make([]string, 0, len(rawPts))
	for _, rawPt := range rawPts {
		lines = append(lines, m.rawPtToLine(rawPt))
	}
	return lines, nil
}

func (m *influxSink2) rawPtToLine(rawPt *tspoint.RawPoint) string {
	var builder strings.Builder
	builder.WriteString(m.conf.Measurement)

	for k, v := range rawPt.Tags {
		builder.WriteString(",")
		builder.WriteString(k)
		builder.WriteString("=")
		builder.WriteString(v)
	}
	builder.WriteString(" ")
	c := 0

	for k, v := range rawPt.Fields {
		c = writeLine(c, &builder, k, v)
	}

	builder.WriteString(" ")
	builder.WriteString(fmt.Sprintf("%d", rawPt.Ts))
	return builder.String()
}

func writeLine(c int, builder *strings.Builder, k string, v any) int {
	if c > 0 {
		builder.WriteString(",")
	}
	c++
	builder.WriteString(k)
	builder.WriteString("=")
	switch value := v.(type) {
	case string:
		builder.WriteString(fmt.Sprintf("\"%s\"", value))
	default:
		builder.WriteString(fmt.Sprintf("%v", value))
	}
	return c
}

func GetSink() api.Sink {
	return &influxSink2{}
}

var (
	_ api.TupleCollector = &influxSink2{}
	_ util.PingableConn  = &influxSink2{}
)
