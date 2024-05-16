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

package influx

import (
	"fmt"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"

	"github.com/lf-edge/ekuiper/extensions/sinks/tspoint"
	"github.com/lf-edge/ekuiper/internal/pkg/cert"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

// c is the configuration for influx2 sink
type c struct {
	// connection
	Addr     string `json:"addr"`
	Username string `json:"username"`
	Password string `json:"password"`
	// http connection
	// tls conf in cert.go
	// write options
	Database    string `json:"database"`
	Measurement string `json:"measurement"`
	tspoint.WriteOptions
}

type influxSink struct {
	conf c
	// internal conf value
	password string
	// temp variables
	bp  client.BatchPoints
	cli client.Client
}

func (m *influxSink) Configure(props map[string]interface{}) error {
	m.conf = c{
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
	if len(m.conf.Database) == 0 {
		return fmt.Errorf("database is required")
	}
	if len(m.conf.Measurement) == 0 {
		return fmt.Errorf("measurement is required")
	}
	err = cast.MapToStruct(props, &m.conf.WriteOptions)
	if err != nil {
		return fmt.Errorf("error configuring influx sink: %s", err)
	}
	err = m.conf.WriteOptions.Validate()
	if err != nil {
		return err
	}
	tlsConf, err := cert.GenTLSConfig(props, "influx-sink")
	if err != nil {
		return fmt.Errorf("error configuring tls: %s", err)
	}
	m.password = m.conf.Password
	m.conf.Password = "******"
	var insecureSkip bool
	if tlsConf != nil {
		insecureSkip = tlsConf.InsecureSkipVerify
	}

	m.cli, err = client.NewHTTPClient(client.HTTPConfig{
		Addr:               m.conf.Addr,
		Username:           m.conf.Username,
		Password:           m.password,
		InsecureSkipVerify: insecureSkip,
		TLSConfig:          tlsConf,
	})
	if err != nil {
		return fmt.Errorf("error creating influx client: %s", err)
	}
	return err
}

func (m *influxSink) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("influx sink open with properties %+v", m.conf)
	err := m.conf.WriteOptions.ValidateTagTemplates(ctx)
	if err != nil {
		return err
	}
	// Test connection. Put it here to avoid server connection when running test in Configure
	_, _, err = m.cli.Ping(time.Second * 10)
	if err != nil {
		return fmt.Errorf("error pinging influx server: %s", err)
	}
	m.bp, err = client.NewBatchPoints(client.BatchPointsConfig{
		Database:  m.conf.Database,
		Precision: m.conf.PrecisionStr,
	})
	return err
}

func (m *influxSink) Ping(_ string, props map[string]interface{}) error {
	if err := m.Configure(props); err != nil {
		return err
	}
	defer func() {
		if m.cli != nil {
			m.cli.Close()
		}
	}()
	// Test connection. Put it here to avoid server connection when running test in Configure
	_, _, err := m.cli.Ping(time.Second * 10)
	return err
}

func (m *influxSink) Collect(ctx api.StreamContext, data any) error {
	logger := ctx.GetLogger()
	err := m.transformPoints(ctx, data)
	if err != nil {
		logger.Error(err)
		return err
	}
	// Write the batch
	err = m.cli.Write(m.bp)
	if err != nil {
		logger.Error(err)
		return err
	}
	logger.Debug("influx insert success")
	return nil
}

func (m *influxSink) transformPoints(ctx api.StreamContext, data any) error {
	var err error
	m.bp, err = client.NewBatchPoints(client.BatchPointsConfig{
		Database:  m.conf.Database,
		Precision: m.conf.PrecisionStr,
	})
	if err != nil {
		return err
	}

	rawPts, err := tspoint.SinkTransform(ctx, data, &m.conf.WriteOptions)
	if err != nil {
		ctx.GetLogger().Error(err)
		return err
	}
	for _, rawPt := range rawPts {
		pt, err := client.NewPoint(m.conf.Measurement, rawPt.Tags, rawPt.Fields, rawPt.Tt)
		if err != nil {
			return err
		}
		m.bp.AddPoint(pt)
	}
	return nil
}

func (m *influxSink) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("influx sink close")
	return m.cli.Close()
}

func GetSink() api.Sink {
	return &influxSink{}
}
